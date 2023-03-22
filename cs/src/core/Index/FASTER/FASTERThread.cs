// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal (string, CommitPoint) InternalContinue<Input, Output, Context>(int sessionID, out FasterExecutionContext<Input, Output, Context> ctx)
        {
            ctx = null;

            if (_recoveredSessions != null)
            {
                if (_recoveredSessions.TryGetValue(sessionID, out _))
                {
                    // We have recovered the corresponding session. 
                    // Now obtain the session by first locking the rest phase
                    var currentState = SystemState.Copy(ref systemState);
                    if (currentState.Phase == Phase.REST)
                    {
                        var intermediateState = SystemState.MakeIntermediate(currentState);
                        if (MakeTransition(currentState, intermediateState))
                        {
                            // No one can change from REST phase
                            if (_recoveredSessions.TryRemove(sessionID, out var cp))
                            {
                                // We have atomically removed session details. 
                                // No one else can continue this session
                                ctx = new FasterExecutionContext<Input, Output, Context>();
                                InitContext(ctx, sessionID, cp.Item1);
                                ctx.prevCtx = new FasterExecutionContext<Input, Output, Context>();
                                InitContext(ctx.prevCtx, sessionID, cp.Item1);
                                ctx.prevCtx.version--;
                                ctx.serialNum = cp.Item2.UntilSerialNo;
                            }
                            else
                            {
                                // Someone else continued this session
                                cp = ((string)null, new CommitPoint { UntilSerialNo = -1 });
                                Debug.WriteLine("Session already continued by another thread!");
                            }

                            MakeTransition(intermediateState, currentState);
                            return cp;
                        }
                    }

                    // Need to try again when in REST
                    Debug.WriteLine("Can continue only in REST phase");
                    return (null, new CommitPoint { UntilSerialNo = -1 });
                }
            }

            Debug.WriteLine("No recovered sessions!");
            return (null, new CommitPoint { UntilSerialNo = -1 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh<Input, Output, Context, FasterSession>(FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref systemState);
            if (fasterSession.Ctx.phase == Phase.REST && newPhaseInfo.Phase == Phase.REST && fasterSession.Ctx.version == newPhaseInfo.Version)
                return;

            ThreadStateMachineStep(fasterSession.Ctx, fasterSession, default);
        }

        internal static void InitContext<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> ctx, int sessionID, string sessionName, long lsn = -1)
        {
            ctx.phase = Phase.REST;
            // The system version starts at 1. Because we do not know what the current state machine state is,
            // we need to play it safe and initialize context behind the system state. Otherwise the session may
            // never "catch up" with the rest of the system when stepping through the state machine as it is ahead.
            ctx.version = 1;
            ctx.markers = new bool[8];
            ctx.serialNum = lsn;
            ctx.sessionID = sessionID;
            ctx.sessionName = sessionName;

            if (ctx.readyResponses is null)
            {
                ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                ctx.ioPendingRequests = new Dictionary<long, PendingContext<Input, Output, Context>>();
                ctx.pendingReads = new AsyncCountDown();
            }
        }

        internal static void CopyContext<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> src, FasterExecutionContext<Input, Output, Context> dst)
        {
            dst.phase = src.phase;
            dst.version = src.version;
            dst.threadStateMachine = src.threadStateMachine;
            dst.markers = src.markers;
            dst.serialNum = src.serialNum;
            dst.sessionName = src.sessionName;
            dst.excludedSerialNos = new List<long>();

            foreach (var v in src.ioPendingRequests.Values)
            {
                dst.excludedSerialNos.Add(v.serialNum);
            }
        }

        internal bool InternalCompletePending<Input, Output, Context, FasterSession>(FasterSession fasterSession, bool wait = false, 
                                                                                     CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs = null)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            while (true)
            {
                InternalCompletePendingRequests(fasterSession, completedOutputs);
                if (wait) fasterSession.Ctx.WaitPending(epoch);

                if (fasterSession.Ctx.HasNoPendingRequests) return true;

                InternalRefresh<Input, Output, Context, FasterSession>(fasterSession);

                if (!wait) return false;
                Thread.Yield();
            }
        }

        internal bool InRestPhase() => systemState.Phase == Phase.REST;

        #region Complete Pending Requests
        internal void InternalCompletePendingRequests<Input, Output, Context, FasterSession>(FasterSession fasterSession, 
                                                                                             CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            hlog.TryComplete();

            if (fasterSession.Ctx.readyResponses.Count == 0) return;

            while (fasterSession.Ctx.readyResponses.TryDequeue(out AsyncIOContext<Key, Value> request))
            {
                InternalCompletePendingRequest(fasterSession, request, completedOutputs);
            }
        }

        internal void InternalCompletePendingRequest<Input, Output, Context, FasterSession>(FasterSession fasterSession, AsyncIOContext<Key, Value> request,
                                                                                            CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (fasterSession.Ctx.ioPendingRequests.TryGetValue(request.id, out var pendingContext))
            {
                // Remove from pending dictionary
                fasterSession.Ctx.ioPendingRequests.Remove(request.id);
                var status = InternalCompletePendingRequestFromContext(fasterSession, request, ref pendingContext, out _);
                if (completedOutputs is not null && status.IsCompletedSuccessfully)
                {
                    // Transfer things to outputs from pendingContext before we dispose it.
                    completedOutputs.TransferTo(ref pendingContext, status);
                }
                if (!status.IsPending)
                    pendingContext.Dispose();
            }
        }

        /// <summary>
        /// Caller is expected to dispose pendingContext after this method completes
        /// </summary>
        internal Status InternalCompletePendingRequestFromContext<Input, Output, Context, FasterSession>(FasterSession fasterSession, AsyncIOContext<Key, Value> request,
                                                                    ref PendingContext<Input, Output, Context> pendingContext, out AsyncIOContext<Key, Value> newRequest)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalCompletePendingRequestFromContext requires epoch acquision");
            newRequest = default;

            // If NoKey, we do not have the key in the initial call and must use the key from the satisfied request.
            // With the new overload of CompletePending that returns CompletedOutputs, pendingContext must have the key.
            if (pendingContext.NoKey && pendingContext.key == default)
                pendingContext.key = hlog.GetKeyContainer(ref hlog.GetContextRecordKey(ref request));
            ref Key key = ref pendingContext.key.Get();

            OperationStatus internalStatus = pendingContext.type switch
            {
                OperationType.READ => InternalContinuePendingRead(request, ref pendingContext, fasterSession),
                OperationType.RMW => InternalContinuePendingRMW(request, ref pendingContext, fasterSession),
                OperationType.CONDITIONAL_INSERT => InternalContinuePendingConditionalInsert(request, ref pendingContext, fasterSession),
                _ => throw new FasterException("Unexpected OperationType")
            };

            var status = HandleOperationStatus(fasterSession.Ctx, ref pendingContext, internalStatus, out newRequest);

            // If done, callback user code
            if (status.IsCompletedSuccessfully)
            {
                if (pendingContext.type == OperationType.READ)
                {
                    fasterSession.ReadCompletionCallback(ref key,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.recordInfo, pendingContext.logicalAddress));
                }
                else
                {
                    fasterSession.RMWCompletionCallback(ref key,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status,
                                                     new RecordMetadata(pendingContext.recordInfo, pendingContext.logicalAddress));
                }
            }

            unsafe
            {
                ref RecordInfo recordInfo = ref hlog.GetInfoFromBytePointer(request.record.GetValidPointer());
                fasterSession.DisposeDeserializedFromDisk(ref hlog.GetContextRecordKey(ref request), ref hlog.GetContextRecordValue(ref request), ref recordInfo);
            }
            request.Dispose();
            return status;
        }
        #endregion
    }
}

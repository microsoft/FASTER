// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal CommitPoint InternalContinue<Input, Output, Context>(string guid, out FasterExecutionContext<Input, Output, Context> ctx)
        {
            ctx = null;

            if (_recoveredSessions != null)
            {
                if (_recoveredSessions.TryGetValue(guid, out _))
                {
                    // We have recovered the corresponding session. 
                    // Now obtain the session by first locking the rest phase
                    var currentState = SystemState.Copy(ref systemState);
                    if (currentState.phase == Phase.REST)
                    {
                        var intermediateState = SystemState.MakeIntermediate(currentState);
                        if (MakeTransition(currentState, intermediateState))
                        {
                            // No one can change from REST phase
                            if (_recoveredSessions.TryRemove(guid, out CommitPoint cp))
                            {
                                // We have atomically removed session details. 
                                // No one else can continue this session
                                ctx = new FasterExecutionContext<Input, Output, Context>();
                                InitContext(ctx, guid);
                                ctx.prevCtx = new FasterExecutionContext<Input, Output, Context>();
                                InitContext(ctx.prevCtx, guid);
                                ctx.prevCtx.version--;
                                ctx.serialNum = cp.UntilSerialNo;
                            }
                            else
                            {
                                // Someone else continued this session
                                cp = new CommitPoint { UntilSerialNo = -1 };
                                Debug.WriteLine("Session already continued by another thread!");
                            }

                            MakeTransition(intermediateState, currentState);
                            return cp;
                        }
                    }

                    // Need to try again when in REST
                    Debug.WriteLine("Can continue only in REST phase");
                    return new CommitPoint { UntilSerialNo = -1 };
                }
            }

            Debug.WriteLine("No recovered sessions!");
            return new CommitPoint { UntilSerialNo = -1 };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh<Input, Output, Context, FasterSession>(FasterExecutionContext<Input, Output, Context> ctx, FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref systemState);
            if (ctx.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && ctx.version == newPhaseInfo.version)
            {
                return;
            }

            ThreadStateMachineStep(ctx, fasterSession, default);
        }

        internal void InitContext<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> ctx, string token, long lsn = -1)
        {
            ctx.phase = Phase.REST;
            // The system version starts at 1. Because we do not know what the current state machine state is,
            // we need to play it safe and initialize context behind the system state. Otherwise the session may
            // never "catch up" with the rest of the system when stepping through the state machine as it is ahead.
            ctx.version = 1;
            ctx.markers = new bool[8];
            ctx.serialNum = lsn;
            ctx.guid = token;

            if (RelaxedCPR)
            {
                if (ctx.retryRequests == null)
                {
                    ctx.retryRequests = new Queue<PendingContext<Input, Output, Context>>();
                    ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                    ctx.ioPendingRequests = new Dictionary<long, PendingContext<Input, Output, Context>>();
                    ctx.pendingReads = new AsyncCountDown();
                }
            }
            else
            {
                ctx.totalPending = 0;
                ctx.retryRequests = new Queue<PendingContext<Input, Output, Context>>();
                ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                ctx.ioPendingRequests = new Dictionary<long, PendingContext<Input, Output, Context>>();
                ctx.pendingReads = new AsyncCountDown();
            }
        }

        internal void CopyContext<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> src, FasterExecutionContext<Input, Output, Context> dst)
        {
            dst.phase = src.phase;
            dst.version = src.version;
            dst.markers = src.markers;
            dst.serialNum = src.serialNum;
            dst.guid = src.guid;
            dst.excludedSerialNos = new List<long>();

            if (!RelaxedCPR)
            {
                dst.totalPending = src.totalPending;
                dst.retryRequests = src.retryRequests;
                dst.readyResponses = src.readyResponses;
                dst.ioPendingRequests = src.ioPendingRequests;
                dst.pendingReads = src.pendingReads;
            }
            else
            {
                foreach (var v in src.ioPendingRequests.Values)
                {
                    dst.excludedSerialNos.Add(v.serialNum);
                }
                foreach (var v in src.retryRequests)
                {
                    dst.excludedSerialNos.Add(v.serialNum);
                }
            }
        }

        internal bool InternalCompletePending<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> ctx, 
            FasterSession fasterSession, 
            bool wait = false)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            do
            {
                bool done = true;

                #region Previous pending requests
                if (!RelaxedCPR)
                {
                    if (ctx.phase == Phase.IN_PROGRESS || ctx.phase == Phase.WAIT_PENDING)
                    {
                        InternalCompletePendingRequests(ctx.prevCtx, ctx, fasterSession);
                        InternalCompleteRetryRequests(ctx.prevCtx, ctx, fasterSession);
                        InternalRefresh(ctx, fasterSession);

                        done &= (ctx.prevCtx.HasNoPendingRequests);
                    }
                }
                #endregion

                InternalCompletePendingRequests(ctx, ctx, fasterSession);
                InternalCompleteRetryRequests(ctx, ctx, fasterSession);

                done &= (ctx.HasNoPendingRequests);

                if (done)
                {
                    return true;
                }

                InternalRefresh(ctx, fasterSession);

                if (wait)
                {
                    // Yield before checking again
                    Thread.Yield();
                }
            } while (wait);

            return false;
        }

        internal bool InRestPhase() => systemState.phase == Phase.REST;

        #region Complete Retry Requests
        internal void InternalCompleteRetryRequests<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            int count = opCtx.retryRequests.Count;

            if (count == 0) return;

            for (int i = 0; i < count; i++)
            {
                var pendingContext = opCtx.retryRequests.Dequeue();
                InternalCompleteRetryRequest(opCtx, currentCtx, pendingContext, fasterSession);
            }
        }

        internal void InternalCompleteRetryRequest<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            PendingContext<Input, Output, Context> pendingContext, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var internalStatus = default(OperationStatus);
            ref Key key = ref pendingContext.key.Get();

            do
            {
                // Issue retry command
                switch (pendingContext.type)
                {
                    case OperationType.RMW:
                        internalStatus = InternalRMW(ref key, ref pendingContext.input.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                        break;
                    case OperationType.UPSERT:
                        internalStatus = InternalUpsert(ref key, ref pendingContext.value.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                        break;
                    case OperationType.DELETE:
                        internalStatus = InternalDelete(ref key, ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                        break;
                    case OperationType.READ:
                        throw new FasterException("Cannot happen!");
                }
            } while (internalStatus == OperationStatus.RETRY_NOW);

            Status status;
            // Handle operation status
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(opCtx, currentCtx, ref pendingContext, fasterSession, internalStatus, false, out _);
            }

            // If done, callback user code.
            if (status == Status.OK || status == Status.NOTFOUND)
            {
                if (pendingContext.heldLatch == LatchOperation.Shared)
                    ReleaseSharedLatch(key);

                switch (pendingContext.type)
                {
                    case OperationType.RMW:
                        fasterSession.RMWCompletionCallback(ref key,
                                                ref pendingContext.input.Get(),
                                                pendingContext.userContext, status);
                        break;
                    case OperationType.UPSERT:
                        fasterSession.UpsertCompletionCallback(ref key,
                                                 ref pendingContext.value.Get(),
                                                 pendingContext.userContext);
                        break;
                    case OperationType.DELETE:
                        fasterSession.DeleteCompletionCallback(ref key,
                                                 pendingContext.userContext);
                        break;
                    default:
                        throw new FasterException("Operation type not allowed for retry");
                }

            }
        }
        #endregion

        #region Complete Pending Requests
        internal void InternalCompletePendingRequests<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (opCtx.readyResponses.Count == 0) return;

            while (opCtx.readyResponses.TryDequeue(out AsyncIOContext<Key, Value> request))
            {
                InternalCompletePendingRequest(opCtx, currentCtx, fasterSession, request);
            }
        }

        internal async ValueTask InternalCompletePendingRequestsAsync<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            FasterSession fasterSession, 
            CancellationToken token = default)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            while (opCtx.SyncIoPendingCount > 0)
            {
                AsyncIOContext<Key, Value> request;

                if (opCtx.readyResponses.Count > 0)
                {
                    fasterSession.UnsafeResumeThread();
                    while (opCtx.readyResponses.Count > 0)
                    {
                        opCtx.readyResponses.TryDequeue(out request);
                        InternalCompletePendingRequest(opCtx, currentCtx, fasterSession, request);
                    }
                    fasterSession.UnsafeSuspendThread();
                }
                else
                {
                    request = await opCtx.readyResponses.DequeueAsync(token);

                    fasterSession.UnsafeResumeThread();
                    InternalCompletePendingRequest(opCtx, currentCtx, fasterSession, request);
                    fasterSession.UnsafeSuspendThread();
                }
            }
        }

        internal void InternalCompletePendingRequest<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx, 
            FasterExecutionContext<Input, Output, Context> currentCtx, 
            FasterSession fasterSession, 
            AsyncIOContext<Key, Value> request)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (opCtx.ioPendingRequests.TryGetValue(request.id, out var pendingContext))
            {
                // Remove from pending dictionary
                opCtx.ioPendingRequests.Remove(request.id);
                InternalCompletePendingRequestFromContext(opCtx, currentCtx, fasterSession, request, ref pendingContext, false, out _);
                pendingContext.Dispose();
            }
        }

        /// <summary>
        /// Caller is expected to dispose pendingContext after this method completes
        /// </summary>
        internal Status InternalCompletePendingRequestFromContext<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            FasterSession fasterSession,
            AsyncIOContext<Key, Value> request,
            ref PendingContext<Input, Output, Context> pendingContext, bool asyncOp, out AsyncIOContext<Key, Value> newRequest)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            newRequest = default;
            ref Key key = ref pendingContext.key.Get();
            OperationStatus internalStatus;

            // Issue the continue command
            if (pendingContext.type == OperationType.READ)
            {
                internalStatus = InternalContinuePendingRead(opCtx, request, ref pendingContext, fasterSession, currentCtx);
            }
            else
            {
                internalStatus = InternalContinuePendingRMW(opCtx, request, ref pendingContext, fasterSession, currentCtx);
            }

            request.Dispose();

            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            Status status;
            // Handle operation status
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(opCtx, currentCtx, ref pendingContext, fasterSession, internalStatus, asyncOp, out newRequest);
            }

            // If done, callback user code
            if (status == Status.OK || status == Status.NOTFOUND)
            {
                if (pendingContext.heldLatch == LatchOperation.Shared)
                    ReleaseSharedLatch(key);

                if (pendingContext.type == OperationType.READ)
                {
                    fasterSession.ReadCompletionCallback(ref key,
                                                     ref pendingContext.input.Get(),
                                                     ref pendingContext.output,
                                                     pendingContext.userContext,
                                                     status, default);
                }
                else
                {
                    fasterSession.RMWCompletionCallback(ref key,
                                                    ref pendingContext.input.Get(),
                                                    pendingContext.userContext,
                                                    status);
                }
            }
            return status;
        }
        #endregion
    }
}

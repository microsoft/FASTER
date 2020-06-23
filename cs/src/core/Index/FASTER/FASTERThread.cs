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
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal CommitPoint InternalContinue(string guid, out FasterExecutionContext ctx)
        {
            ctx = null;

            if (_recoveredSessions != null)
            {
                if (_recoveredSessions.TryGetValue(guid, out _))
                {
                    // We have recovered the corresponding session. 
                    // Now obtain the session by first locking the rest phase
                    var currentState = SystemState.Copy(ref _systemState);
                    if (currentState.phase == Phase.REST)
                    {
                        var intermediateState = SystemState.Make(Phase.INTERMEDIATE, currentState.version);
                        if (MakeTransition(currentState, intermediateState))
                        {
                            // No one can change from REST phase
                            if (_recoveredSessions.TryRemove(guid, out CommitPoint cp))
                            {
                                // We have atomically removed session details. 
                                // No one else can continue this session
                                ctx = new FasterExecutionContext();
                                InitContext(ctx, guid);
                                ctx.prevCtx = new FasterExecutionContext();
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
        internal void InternalRefresh(FasterExecutionContext ctx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession = null)
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref _systemState);
            if (ctx.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && ctx.version == newPhaseInfo.version)
            {
                return;
            }

            
            // await is never invoked when calling the function with async = false
            #pragma warning disable 4014
            ThreadStateMachineStep(ctx, clientSession, false);
            #pragma warning restore 4014
        }


        internal void InitContext(FasterExecutionContext ctx, string token, long lsn = -1)
        {
            ctx.phase = Phase.REST;
            ctx.version = _systemState.version;
            ctx.markers = new bool[8];
            ctx.serialNum = lsn;
            ctx.guid = token;

            if (RelaxedCPR)
            {
                if (ctx.retryRequests == null)
                {
                    ctx.retryRequests = new Queue<PendingContext>();
                    ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                    ctx.ioPendingRequests = new Dictionary<long, PendingContext>();
                    ctx.pendingReads = new AsyncCountDown();
                }
            }
            else
            {
                ctx.totalPending = 0;
                ctx.retryRequests = new Queue<PendingContext>();
                ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                ctx.ioPendingRequests = new Dictionary<long, PendingContext>();
                ctx.pendingReads = new AsyncCountDown();
            }
        }

        internal void CopyContext(FasterExecutionContext src, FasterExecutionContext dst)
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

        internal bool InternalCompletePending(FasterExecutionContext ctx, bool wait = false)
        {
            do
            {
                bool done = true;

                #region Previous pending requests
                if (!RelaxedCPR)
                {
                    if (ctx.phase == Phase.IN_PROGRESS || ctx.phase == Phase.WAIT_PENDING)
                    {
                        InternalCompletePendingRequests(ctx.prevCtx, ctx);
                        InternalCompleteRetryRequests(ctx.prevCtx, ctx);
                        InternalRefresh(ctx);

                        done &= (ctx.prevCtx.HasNoPendingRequests);
                    }
                }
                #endregion

                InternalCompletePendingRequests(ctx, ctx);
                InternalCompleteRetryRequests(ctx, ctx);
                InternalRefresh(ctx);

                done &= (ctx.HasNoPendingRequests);

                if (done)
                {
                    return true;
                }

                if (wait)
                {
                    // Yield before checking again
                    Thread.Yield();
                }
            } while (wait);

            return false;
        }

        internal bool InRestPhase() => _systemState.phase == Phase.REST;

        #region Complete Retry Requests
        internal void InternalCompleteRetryRequests(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession = null)
        {
            int count = opCtx.retryRequests.Count;

            if (count == 0) return;

            clientSession?.UnsafeResumeThread();
            for (int i = 0; i < count; i++)
            {
                var pendingContext = opCtx.retryRequests.Dequeue();
                InternalCompleteRetryRequest(opCtx, currentCtx, pendingContext);
            }
            clientSession?.UnsafeSuspendThread();
        }

        internal void InternalCompleteRetryRequest(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, PendingContext pendingContext)
        {
            var internalStatus = default(OperationStatus);
            ref Key key = ref pendingContext.key.Get();
            ref Value value = ref pendingContext.value.Get();

            // Issue retry command
            switch (pendingContext.type)
            {
                case OperationType.RMW:
                    internalStatus = InternalRMW(ref key, ref pendingContext.input, ref pendingContext.userContext,
                                                 ref pendingContext, currentCtx, pendingContext.serialNum,
                                                 ref pendingContext.psfUpdateArgs);
                    break;
                case OperationType.UPSERT:
                    internalStatus = InternalUpsert(ref key, ref value, ref pendingContext.userContext, ref pendingContext,
                                                    currentCtx, pendingContext.serialNum, ref pendingContext.psfUpdateArgs);
                    break;
                case OperationType.PSF_INSERT:
                    internalStatus = PsfInternalInsert(ref key, ref value, ref pendingContext.input,
                                                       ref pendingContext, currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.DELETE:
                    internalStatus = InternalDelete(ref key, ref pendingContext.userContext, ref pendingContext, currentCtx,
                                                    pendingContext.serialNum, ref pendingContext.psfUpdateArgs);
                    break;
                case OperationType.PSF_READ_ADDRESS:
                    // This is a special case for RECORD_ON_DISK in ContextPsfReadAddress during CPR;
                    // it is the only Read operation that goes through Retry.
                    internalStatus = this.PsfInternalReadAddress(ref pendingContext.psfReadArgs, ref pendingContext,
                                                                 currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.PSF_READ_KEY:
                case OperationType.READ:
                    throw new FasterException("Reads go through the Pending route, not Retry, so this cannot happen!");
            }

            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(opCtx, currentCtx, pendingContext, internalStatus);

            if (status == Status.OK && this.PSFManager.HasPSFs)
            {
                switch (pendingContext.type)
                {
                    case OperationType.UPSERT:
                        // Successful Upsert must have its PSFs executed and their keys stored.
                        status = this.PSFManager.Upsert(new FasterKVProviderData<Key, Value>(this.hlog, ref key, ref value),
                                                       pendingContext.psfUpdateArgs.LogicalAddress,
                                                       pendingContext.psfUpdateArgs.ChangeTracker);
                        break;
                    case OperationType.PSF_INSERT:
                        var psfInput = (IPSFInput<Key>)pendingContext.input;
                        var updateOp = pendingContext.psfUpdateArgs.ChangeTracker.UpdateOp;
                        if (psfInput.IsDelete && (updateOp == UpdateOperation.IPU || updateOp == UpdateOperation.RCU))
                        {
                            // RCU Insert of a tombstoned old record is followed by Insert of the new record.
                            if (pendingContext.psfUpdateArgs.ChangeTracker.FindGroup(psfInput.GroupId, out var ordinal))
                            {
                                ref GroupKeysPair groupKeysPair = ref pendingContext.psfUpdateArgs.ChangeTracker.GetGroupRef(ordinal);
                                GetAfterRecordId(pendingContext.psfUpdateArgs.ChangeTracker, ref value);
                                var pcontext = default(PendingContext);
                                PsfRcuInsert(groupKeysPair.After, ref value, ref pendingContext.input,
                                             ref pcontext, currentCtx, pendingContext.serialNum + 1);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            // If done, callback user code.
            if (status == Status.OK || status == Status.NOTFOUND)
            {
                if (pendingContext.heldLatch == LatchOperation.Shared)
                    ReleaseSharedLatch(key);

                switch (pendingContext.type)
                {
                    case OperationType.RMW:
                        functions.RMWCompletionCallback(ref key,
                                                ref pendingContext.input,
                                                pendingContext.userContext, status);
                        break;
                    case OperationType.UPSERT:
                        functions.UpsertCompletionCallback(ref key,
                                                 ref value,
                                                 pendingContext.userContext);
                        break;
                    case OperationType.DELETE:
                        functions.DeleteCompletionCallback(ref key,
                                                 pendingContext.userContext);
                        break;
                    default:
                        throw new FasterException("Operation type not allowed for retry");
                }
            }
        }
        #endregion

        #region Complete Pending Requests
        internal void InternalCompletePendingRequests(FasterExecutionContext opCtx, FasterExecutionContext currentCtx)
        {
            if (opCtx.readyResponses.Count == 0) return;

            while (opCtx.readyResponses.TryDequeue(out AsyncIOContext<Key, Value> request))
            {
                InternalCompletePendingRequest(opCtx, currentCtx, request);
            }
        }

        internal async ValueTask InternalCompletePendingRequestsAsync(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
        {
            while (opCtx.SyncIoPendingCount > 0)
            {
                AsyncIOContext<Key, Value> request;

                if (opCtx.readyResponses.Count > 0)
                {
                    clientSession.UnsafeResumeThread();
                    while (opCtx.readyResponses.Count > 0)
                    {
                        opCtx.readyResponses.TryDequeue(out request);
                        InternalCompletePendingRequest(opCtx, currentCtx, request);
                    }
                    clientSession.UnsafeSuspendThread();
                }
                else
                {
                    request = await opCtx.readyResponses.DequeueAsync(token);

                    clientSession.UnsafeResumeThread();
                    InternalCompletePendingRequest(opCtx, currentCtx, request);
                    clientSession.UnsafeSuspendThread();
                }
            }
        }

        internal void InternalCompletePendingRequest(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, AsyncIOContext<Key, Value> request)
        {
            if (opCtx.ioPendingRequests.TryGetValue(request.id, out PendingContext pendingContext))
            {
                ref Key key = ref pendingContext.key.Get();

                // Remove from pending dictionary
                opCtx.ioPendingRequests.Remove(request.id);

                OperationStatus internalStatus;
                // Issue the continue command
                if (pendingContext.type == OperationType.READ ||
                    pendingContext.type == OperationType.PSF_READ_KEY ||
                    pendingContext.type == OperationType.PSF_READ_ADDRESS)
                {
                    internalStatus = InternalContinuePendingRead(opCtx, request, ref pendingContext, currentCtx);
                }
                else
                {
                    Debug.Assert(pendingContext.type == OperationType.RMW);
                    internalStatus = InternalContinuePendingRMW(opCtx, request, ref pendingContext, currentCtx); ;
                }

                request.Dispose();

                Status status;
                // Handle operation status
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    status = (Status)internalStatus;
                }
                else
                {
                    status = HandleOperationStatus(opCtx, currentCtx, pendingContext, internalStatus);
                }

                // If done, callback user code
                if (status == Status.OK || status == Status.NOTFOUND)
                {
                    if (pendingContext.heldLatch == LatchOperation.Shared)
                        ReleaseSharedLatch(key);

                    if (pendingContext.type == OperationType.READ)
                    {
                        functions.ReadCompletionCallback(ref key,
                                                         ref pendingContext.input,
                                                         ref pendingContext.output,
                                                         pendingContext.userContext,
                                                         status);
                    }
                    else
                    {
                        functions.RMWCompletionCallback(ref key,
                                                        ref pendingContext.input,
                                                        pendingContext.userContext,
                                                        status);
                    }
                }
                pendingContext.Dispose();
            }
        }

        internal (Status, Output) InternalCompletePendingReadRequest(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, AsyncIOContext<Key, Value> request, PendingContext pendingContext)
        {
            (Status, Output) s = default;

            ref Key key = ref pendingContext.key.Get();

            OperationStatus internalStatus = InternalContinuePendingRead(opCtx, request, ref pendingContext, currentCtx);

            request.Dispose();

            Status status;
            // Handle operation status
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                throw new Exception($"Unexpected {nameof(OperationStatus)} while reading => {internalStatus}");
            }

            if (pendingContext.heldLatch == LatchOperation.Shared)
                ReleaseSharedLatch(key);

            functions.ReadCompletionCallback(ref key,
                                             ref pendingContext.input,
                                             ref pendingContext.output,
                                             pendingContext.userContext,
                                             status);

            s.Item1 = status;
            s.Item2 = pendingContext.output;
            pendingContext.Dispose();

            return s;
        }
        #endregion
    }
}

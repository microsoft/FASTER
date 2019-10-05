// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal Guid InternalAcquire()
        {
            epoch.Acquire();
            threadCtx.InitializeThread();
            Phase phase = _systemState.phase;
            if (phase != Phase.REST)
            {
                throw new Exception("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            threadCtx.Value = new FasterExecutionContext();
            InitContext(threadCtx.Value, guid);

            threadCtx.Value.prevCtx = new FasterExecutionContext();
            InitContext(threadCtx.Value.prevCtx, guid);
            threadCtx.Value.prevCtx.version--;
            InternalRefresh(threadCtx.Value);
            return threadCtx.Value.guid;
        }

        internal CommitPoint InternalContinue(Guid guid, out FasterExecutionContext ctx)
        {
            ctx = null;

            if (_recoveredSessions != null)
            {
                if (_recoveredSessions.TryGetValue(guid, out CommitPoint cp))
                {
                    // We have recovered the corresponding session. 
                    // Now obtain the session by first locking the rest phase
                    var currentState = SystemState.Copy(ref _systemState);
                    if(currentState.phase == Phase.REST)
                    {
                        var intermediateState = SystemState.Make(Phase.INTERMEDIATE, currentState.version);
                        if(MakeTransition(currentState,intermediateState))
                        {
                            // No one can change from REST phase
                            if(_recoveredSessions.TryRemove(guid, out cp))
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
        internal void InternalRefresh(FasterExecutionContext ctx)
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref _systemState);
            if (ctx.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && ctx.version == newPhaseInfo.version)
            {
                return;
            }

            // Moving to non-checkpointing phases
            if (newPhaseInfo.phase == Phase.GC || newPhaseInfo.phase == Phase.PREPARE_GROW || newPhaseInfo.phase == Phase.IN_PROGRESS_GROW)
            {
                ctx.phase = newPhaseInfo.phase;
                return;
            }

            HandleCheckpointingPhases(ctx);
        }

        internal void InternalRelease(FasterExecutionContext ctx)
        {
            Debug.Assert(ctx.retryRequests.Count == 0 && ctx.ioPendingRequests.Count == 0);
            if (ctx.prevCtx != null)
            {
                Debug.Assert(ctx.prevCtx.retryRequests.Count == 0 && ctx.prevCtx.ioPendingRequests.Count == 0);
            }
            Debug.Assert(ctx.phase == Phase.REST);

            epoch.Release();
        }

        internal void InitContext(FasterExecutionContext ctx, Guid token)
        {
            ctx.phase = Phase.REST;
            ctx.version = _systemState.version;
            ctx.markers = new bool[8];
            ctx.serialNum = 0;
            ctx.guid = token;

            if (RelaxedCPR)
            {
                if (ctx.retryRequests == null)
                {
                    ctx.retryRequests = new Queue<PendingContext>();
                    ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                    ctx.ioPendingRequests = new Dictionary<long, PendingContext>();
                }
            }
            else
            {
                ctx.totalPending = 0;
                ctx.retryRequests = new Queue<PendingContext>();
                ctx.readyResponses = new AsyncQueue<AsyncIOContext<Key, Value>>();
                ctx.ioPendingRequests = new Dictionary<long, PendingContext>();
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
                        CompleteIOPendingRequests(ctx.prevCtx, ctx);
                        CompleteRetryRequests(ctx.prevCtx, ctx);
                        InternalRefresh(ctx);

                        done &= (ctx.prevCtx.ioPendingRequests.Count == 0);
                        done &= (ctx.prevCtx.retryRequests.Count == 0);
                    }
                }
                #endregion

                CompleteIOPendingRequests(ctx, ctx);
                CompleteRetryRequests(ctx, ctx);
                InternalRefresh(ctx);

                done &= (ctx.ioPendingRequests.Count == 0);
                done &= (ctx.retryRequests.Count == 0);

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

        internal void CompleteRetryRequests(FasterExecutionContext opCtx, FasterExecutionContext currentCtx)
        {
            int count = opCtx.retryRequests.Count;

            if (count == 0) return;

            for (int i = 0; i < count; i++)
            {
                var pendingContext = opCtx.retryRequests.Dequeue();
                InternalRetryRequestAndCallback(opCtx, currentCtx, pendingContext);
            }
        }

        internal void CompleteRetryRequests(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default(CancellationToken))
        {
            int count = opCtx.retryRequests.Count;

            if (count == 0) return;

            clientSession.UnsafeResumeThread();
            for (int i = 0; i < count; i++)
            {
                var pendingContext = opCtx.retryRequests.Dequeue();
                InternalRetryRequestAndCallback(opCtx, currentCtx, pendingContext);
            }
            clientSession.UnsafeSuspendThread();
        }

        internal void CompleteIOPendingRequests(FasterExecutionContext opCtx, FasterExecutionContext currentCtx)
        {
            if (opCtx.readyResponses.Count == 0) return;

            while (opCtx.readyResponses.TryDequeue(out AsyncIOContext<Key, Value> request))
            {
                InternalContinuePendingRequestAndCallback(opCtx, currentCtx, request);
            }
        }

        internal async ValueTask CompleteIOPendingRequestsAsync(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default(CancellationToken))
        {
            while (opCtx.ioPendingRequests.Count > 0)
            {
                AsyncIOContext<Key, Value> request;

                if (opCtx.readyResponses.Count > 0)
                {
                    clientSession.UnsafeResumeThread();
                    while (opCtx.readyResponses.Count > 0)
                    {
                        opCtx.readyResponses.TryDequeue(out request);
                        InternalContinuePendingRequestAndCallback(opCtx, currentCtx, request);
                    }
                    clientSession.UnsafeSuspendThread();
                }
                else
                {
                    request = await opCtx.readyResponses.DequeueAsync(token);

                    clientSession.UnsafeResumeThread();
                    InternalContinuePendingRequestAndCallback(opCtx, currentCtx, request);
                    clientSession.UnsafeSuspendThread();
                }

            }
        }

        internal void InternalRetryRequestAndCallback(
                                    FasterExecutionContext opCtx,
                                    FasterExecutionContext currentCtx,
                                    PendingContext pendingContext)
        {
            var status = default(Status);
            var internalStatus = default(OperationStatus);
            ref Key key = ref pendingContext.key.Get();
            ref Value value = ref pendingContext.value.Get();

            bool handleLatches = false;

            if (!RelaxedCPR)
            {
                #region Entry latch operation
                
                if ((opCtx.version < currentCtx.version) // Thread has already shifted to (v+1)
                    ||
                    (currentCtx.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
                {
                    handleLatches = true;
                }
                #endregion
            }

            // Issue retry command
            switch(pendingContext.type)
            {
                case OperationType.RMW:
                    internalStatus = InternalRetryPendingRMW(opCtx, ref pendingContext, currentCtx);
                    break;
                case OperationType.UPSERT:
                    internalStatus = InternalUpsert(ref key, 
                                                    ref value, 
                                                    ref pendingContext.userContext, 
                                                    ref pendingContext, currentCtx);
                    break;
                case OperationType.DELETE:
                    internalStatus = InternalDelete(ref key,
                                                    ref pendingContext.userContext,
                                                    ref pendingContext, currentCtx);
                    break;
                case OperationType.READ:
                    throw new Exception("Cannot happen!");
            }
            

            // Handle operation status
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(opCtx, currentCtx, pendingContext, internalStatus);
            }

            // If done, callback user code.
            if (status == Status.OK || status == Status.NOTFOUND)
            {
                if (handleLatches)
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
                        throw new Exception("Operation type not allowed for retry");
                }
                
            }
        }

        internal void InternalContinuePendingRequestAndCallback(
                                    FasterExecutionContext opCtx,
                                    FasterExecutionContext currentCtx,
                                    AsyncIOContext<Key, Value> request)
        {
            bool handleLatches = false;

            if (!RelaxedCPR)
            {
                if ((opCtx.version < currentCtx.version) // Thread has already shifted to (v+1)
                    ||
                    (currentCtx.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
                {
                    handleLatches = true;
                }
            }

            if (opCtx.ioPendingRequests.TryGetValue(request.id, out PendingContext pendingContext))
            {
                var status = default(Status);
                var internalStatus = default(OperationStatus);
                ref Key key = ref pendingContext.key.Get();

                // Remove from pending dictionary
                opCtx.ioPendingRequests.Remove(request.id);

                // Issue the continue command
                if (pendingContext.type == OperationType.READ)
                {
                    internalStatus = InternalContinuePendingRead(opCtx, request, ref pendingContext, currentCtx);
                }
                else
                {
                    internalStatus = InternalContinuePendingRMW(opCtx, request, ref pendingContext, currentCtx); ;
                }

                request.Dispose();

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
                if(status == Status.OK || status == Status.NOTFOUND)
                {
                    if (handleLatches)
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

    }
}

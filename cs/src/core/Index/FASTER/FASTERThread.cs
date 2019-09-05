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
            prevThreadCtx.InitializeThread();
            Phase phase = _systemState.phase;
            if (phase != Phase.REST)
            {
                throw new Exception("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            threadCtx.Value = new FasterExecutionContext();
            InitContext(threadCtx.Value, guid);
            prevThreadCtx.Value = new FasterExecutionContext();
            InitContext(prevThreadCtx.Value, guid);
            prevThreadCtx.Value.version--;
            InternalRefresh();
            return threadCtx.Value.guid;
        }

        internal CommitPoint InternalContinue(Guid guid)
        {
            epoch.Acquire();
            threadCtx.InitializeThread();
            prevThreadCtx.InitializeThread();
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
                                threadCtx.Value = new FasterExecutionContext();
                                InitContext(threadCtx.Value, guid);
                                prevThreadCtx.Value = new FasterExecutionContext();
                                InitContext(prevThreadCtx.Value, guid);
                                prevThreadCtx.Value.version--;
                                threadCtx.Value.serialNum = cp.UntilSerialNo;
                            }
                            else
                            {
                                // Someone else continued this session
                                cp = new CommitPoint { UntilSerialNo = -1 };
                                Debug.WriteLine("Session already continued by another thread!");
                            }

                            MakeTransition(intermediateState, currentState);
                            InternalRefresh();
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
        internal void InternalRefresh()
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref _systemState);
            if (threadCtx.Value.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && threadCtx.Value.version == newPhaseInfo.version)
            {
                return;
            }

            // Moving to non-checkpointing phases
            if (newPhaseInfo.phase == Phase.GC || newPhaseInfo.phase == Phase.PREPARE_GROW || newPhaseInfo.phase == Phase.IN_PROGRESS_GROW)
            {
                threadCtx.Value.phase = newPhaseInfo.phase;
                return;
            }

            HandleCheckpointingPhases();
        }

        internal void InternalRelease()
        {
            /*
            Debug.Assert(threadCtx.Value.retryRequests.Count == 0 &&
                    threadCtx.Value.ioPendingRequests.Count == 0);
            if (prevThreadCtx.Value != default(FasterExecutionContext))
            {
                Debug.Assert(prevThreadCtx.Value.retryRequests.Count == 0 &&
                    prevThreadCtx.Value.ioPendingRequests.Count == 0);
            }
            Debug.Assert(threadCtx.Value.phase == Phase.REST);*/

            threadCtx.DisposeThread();
            prevThreadCtx.DisposeThread();
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

        internal bool InternalCompletePending(bool wait = false)
        {
            do
            {
                bool done = true;

                if (!RelaxedCPR)
                {
                    #region Previous pending requests
                    if (threadCtx.Value.phase == Phase.IN_PROGRESS
                        ||
                        threadCtx.Value.phase == Phase.WAIT_PENDING)
                    {
                        CompleteIOPendingRequests(prevThreadCtx.Value);
                        Refresh();
                        CompleteRetryRequests(prevThreadCtx.Value);

                        done &= (prevThreadCtx.Value.ioPendingRequests.Count == 0);
                        done &= (prevThreadCtx.Value.retryRequests.Count == 0);
                    }
                    #endregion
                }

                if (!(threadCtx.Value.phase == Phase.IN_PROGRESS
                      || 
                      threadCtx.Value.phase == Phase.WAIT_PENDING))
                {
                    CompleteIOPendingRequests(threadCtx.Value);
                }
                InternalRefresh();
                CompleteRetryRequests(threadCtx.Value);

                done &= (threadCtx.Value.ioPendingRequests.Count == 0);
                done &= (threadCtx.Value.retryRequests.Count == 0);

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

        internal void CompleteRetryRequests(FasterExecutionContext context)
        {
            int count = context.retryRequests.Count;

            if (count == 0) return;

            for (int i = 0; i < count; i++)
            {
                var pendingContext = context.retryRequests.Dequeue();
                InternalRetryRequestAndCallback(context, pendingContext);
            }
        }

        internal void CompleteRetryRequests(FasterExecutionContext context, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default(CancellationToken))
        {
            int count = context.retryRequests.Count;

            if (count == 0) return;

            clientSession.UnsafeResumeThread();
            for (int i = 0; i < count; i++)
            {
                var pendingContext = context.retryRequests.Dequeue();
                InternalRetryRequestAndCallback(context, pendingContext);
            }
            clientSession.UnsafeSuspendThread();
        }

        internal void CompleteIOPendingRequests(FasterExecutionContext context)
        {
            if (context.readyResponses.Count == 0) return;

            while (context.readyResponses.TryDequeue(out AsyncIOContext<Key, Value> request))
            {
                InternalContinuePendingRequestAndCallback(context, request);
            }
        }

        internal async ValueTask CompleteIOPendingRequestsAsync(FasterExecutionContext context, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default(CancellationToken))
        {
            while (context.ioPendingRequests.Count > 0)
            {
                AsyncIOContext<Key, Value> request;

                if (context.readyResponses.Count > 0)
                {
                    clientSession.UnsafeResumeThread();
                    while (context.readyResponses.Count > 0)
                    {
                        context.readyResponses.TryDequeue(out request);
                        InternalContinuePendingRequestAndCallback(context, request);
                    }
                    clientSession.UnsafeSuspendThread();
                }
                else
                {
                    request = await context.readyResponses.DequeueAsync(token);

                    clientSession.UnsafeResumeThread();
                    InternalContinuePendingRequestAndCallback(context, request);
                    clientSession.UnsafeSuspendThread();
                }

            }
        }

        internal void InternalRetryRequestAndCallback(
                                    FasterExecutionContext ctx,
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
                
                if ((ctx.version < threadCtx.Value.version) // Thread has already shifted to (v+1)
                    ||
                    (threadCtx.Value.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
                {
                    handleLatches = true;
                }
                #endregion
            }

            // Issue retry command
            switch(pendingContext.type)
            {
                case OperationType.RMW:
                    internalStatus = InternalRetryPendingRMW(ctx, ref pendingContext);
                    break;
                case OperationType.UPSERT:
                    internalStatus = InternalUpsert(ref key, 
                                                    ref value, 
                                                    ref pendingContext.userContext, 
                                                    ref pendingContext);
                    break;
                case OperationType.DELETE:
                    internalStatus = InternalDelete(ref key,
                                                    ref pendingContext.userContext,
                                                    ref pendingContext);
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
                status = HandleOperationStatus(ctx, pendingContext, internalStatus);
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
                                    FasterExecutionContext ctx,
                                    AsyncIOContext<Key, Value> request)
        {
            bool handleLatches = false;

            if (!RelaxedCPR)
            {
                if ((ctx.version < threadCtx.Value.version) // Thread has already shifted to (v+1)
                    ||
                    (threadCtx.Value.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
                {
                    handleLatches = true;
                }
            }

            if (ctx.ioPendingRequests.TryGetValue(request.id, out PendingContext pendingContext))
            {
                var status = default(Status);
                var internalStatus = default(OperationStatus);
                ref Key key = ref pendingContext.key.Get();

                // Remove from pending dictionary
                ctx.ioPendingRequests.Remove(request.id);

                // Issue the continue command
                if (pendingContext.type == OperationType.READ)
                {
                    internalStatus = InternalContinuePendingRead(ctx, request, ref pendingContext);
                }
                else
                {
                    internalStatus = InternalContinuePendingRMW(ctx, request, ref pendingContext); ;
                }

                request.Dispose();

                // Handle operation status
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    status = (Status)internalStatus;
                }
                else
                {
                    status = HandleOperationStatus(ctx, pendingContext, internalStatus);
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

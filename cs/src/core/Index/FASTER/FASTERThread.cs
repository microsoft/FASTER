// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal Guid InternalAcquire()
        {
            epoch.Acquire();
            overflowBucketsAllocator.Acquire();
            threadCtx.InitializeThread();
            prevThreadCtx.InitializeThread();
            Phase phase = _systemState.phase;
            if (phase != Phase.REST)
            {
                throw new Exception("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            InitLocalContext(guid);
            InternalRefresh();
            return threadCtx.Value.guid;
        }

        internal long InternalContinue(Guid guid)
        {
            epoch.Acquire();
            overflowBucketsAllocator.Acquire();
            threadCtx.InitializeThread();
            prevThreadCtx.InitializeThread();
            if (_recoveredSessions != null)
            {
                if (_recoveredSessions.TryGetValue(guid, out long serialNum))
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
                            if(_recoveredSessions.TryRemove(guid, out serialNum))
                            {
                                // We have atomically removed session details. 
                                // No one else can continue this session
                                InitLocalContext(guid);
                                threadCtx.Value.serialNum = serialNum;
                                InternalRefresh();
                            }
                            else
                            {
                                // Someone else continued this session
                                serialNum = -1;
                                Debug.WriteLine("Session already continued by another thread!");
                            }

                            MakeTransition(intermediateState, currentState);
                            return serialNum;
                        }
                    }

                    // Need to try again when in REST
                    Debug.WriteLine("Can continue only in REST phase");
                    return -1;
                }
            }

            Debug.WriteLine("No recovered sessions!");
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRefresh()
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref _systemState);
            if (threadCtx.Value.phase == Phase.REST && newPhaseInfo.phase == Phase.REST)
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
            Debug.Assert(threadCtx.Value.retryRequests.Count == 0 &&
                    threadCtx.Value.ioPendingRequests.Count == 0);
            if (prevThreadCtx.Value != default(FasterExecutionContext))
            {
                Debug.Assert(prevThreadCtx.Value.retryRequests.Count == 0 &&
                    prevThreadCtx.Value.ioPendingRequests.Count == 0);
            }
            Debug.Assert(threadCtx.Value.phase == Phase.REST);
            threadCtx.DisposeThread();
            prevThreadCtx.DisposeThread();
            epoch.Release();
            overflowBucketsAllocator.Release();
        }

        internal void InitLocalContext(Guid token)
        {
            var ctx = 
                new FasterExecutionContext
                {
                    phase = Phase.REST,
                    version = _systemState.version,
                    markers = new bool[8],
                    serialNum = 0,
                    totalPending = 0,
                    guid = token,
                    retryRequests = new Queue<PendingContext>(),
                    readyResponses = new BlockingCollection<AsyncIOContext<Key, Value>>(),
                    ioPendingRequests = new Dictionary<long, PendingContext>()
                };

            for(int i = 0; i < 8; i++)
            {
                ctx.markers[i] = false;
            }

            threadCtx.Value = ctx;
        }

        internal bool InternalCompletePending(bool wait = false)
        {
            do
            {
                bool done = true;

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
            for (int i = 0; i < count; i++)
            {
                var pendingContext = context.retryRequests.Dequeue();
                InternalRetryRequestAndCallback(context, pendingContext);
            }
        }

        internal void CompleteIOPendingRequests(FasterExecutionContext context)
        {
            if (context.readyResponses.Count == 0) return;

            while (context.readyResponses.TryTake(out AsyncIOContext<Key, Value> request))
            {
                InternalContinuePendingRequestAndCallback(context, request);
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

            #region Entry latch operation
            var handleLatches = false;
            if ((ctx.version < threadCtx.Value.version) // Thread has already shifted to (v+1)
                ||
                (threadCtx.Value.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
            {
                handleLatches = true;
            }
            #endregion

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
            var handleLatches = false;
            if ((ctx.version < threadCtx.Value.version) // Thread has already shifted to (v+1)
                ||
                (threadCtx.Value.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
            {
                handleLatches = true;
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

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
            Phase phase = _systemState.phase;
            if (phase != Phase.REST)
            {
                throw new Exception("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            InitLocalContext(ref threadCtx, guid);
            InternalRefresh();
            return threadCtx.guid;
        }

        internal long InternalContinue(Guid guid)
        {
            epoch.Acquire();
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
                                InitLocalContext(ref threadCtx, guid);
                                threadCtx.serialNum = serialNum;
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
            if (threadCtx.phase == Phase.REST && newPhaseInfo.phase == Phase.REST)
            {
                return;
            }

            // Moving to non-checkpointing phases
            if (newPhaseInfo.phase == Phase.GC || newPhaseInfo.phase == Phase.PREPARE_GROW || newPhaseInfo.phase == Phase.IN_PROGRESS_GROW)
            {
                threadCtx.phase = newPhaseInfo.phase;
                return;
            }

            HandleCheckpointingPhases();
        }

        internal void InternalRelease()
        {
            Debug.Assert(threadCtx.retryRequests.Count == 0 &&
                    threadCtx.ioPendingRequests.Count == 0);
            if (prevThreadCtx != default(FasterExecutionContext))
            {
                Debug.Assert(prevThreadCtx.retryRequests.Count == 0 &&
                    prevThreadCtx.ioPendingRequests.Count == 0);
            }
            Debug.Assert(threadCtx.phase == Phase.REST);
            prevThreadCtx = null;
            threadCtx = null;
            epoch.Release();
        }

        internal void InitLocalContext(ref FasterExecutionContext context, Guid token)
        {
            context = new FasterExecutionContext
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
                context.markers[i] = false;
            }
        }

        internal bool InternalCompletePending(bool wait = false)
        {
            do
            {
                bool done = true;

                #region Previous pending requests
                if (threadCtx.phase == Phase.IN_PROGRESS
                    ||
                    threadCtx.phase == Phase.WAIT_PENDING)
                {
                    CompleteIOPendingRequests(prevThreadCtx);
                    Refresh();
                    CompleteRetryRequests(prevThreadCtx);

                    done &= (prevThreadCtx.ioPendingRequests.Count == 0);
                    done &= (prevThreadCtx.retryRequests.Count == 0);
                }
                #endregion

                if (!(threadCtx.phase == Phase.IN_PROGRESS
                      || 
                      threadCtx.phase == Phase.WAIT_PENDING))
                {
                    CompleteIOPendingRequests(threadCtx);
                }
                InternalRefresh();
                CompleteRetryRequests(threadCtx);

                done &= (threadCtx.ioPendingRequests.Count == 0);
                done &= (threadCtx.retryRequests.Count == 0);

                if (done)
                {
                    return true;
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

            #region Entry latch operation
            var handleLatches = false;
            if ((ctx.version < threadCtx.version) // Thread has already shifted to (v+1)
                ||
                (threadCtx.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
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
                    internalStatus = InternalUpsert(ref pendingContext.key, 
                                                    ref pendingContext.value, 
                                                    ref pendingContext.userContext, 
                                                    ref pendingContext);
                    break;
                case OperationType.DELETE:
                    internalStatus = InternalDelete(ref pendingContext.key,
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
                    ReleaseSharedLatch(pendingContext.key);

                switch (pendingContext.type)
                {
                    case OperationType.RMW:
                        functions.RMWCompletionCallback(ref pendingContext.key,
                                                ref pendingContext.input,
                                                pendingContext.userContext, status);
                        break;
                    case OperationType.UPSERT:
                        functions.UpsertCompletionCallback(ref pendingContext.key,
                                                 ref pendingContext.value,
                                                 pendingContext.userContext);
                        break;
                    case OperationType.DELETE:
                        functions.DeleteCompletionCallback(ref pendingContext.key,
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
            if ((ctx.version < threadCtx.version) // Thread has already shifted to (v+1)
                ||
                (threadCtx.phase == Phase.PREPARE)) // Thread still in version v, but acquired shared-latch 
            {
                handleLatches = true;
            }

            if (ctx.ioPendingRequests.TryGetValue(request.id, out PendingContext pendingContext))
            {
                var status = default(Status);
                var internalStatus = default(OperationStatus);

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
                
                request.record.Return();

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
                        ReleaseSharedLatch(pendingContext.key);

                    if (pendingContext.type == OperationType.READ)
                    {
                        functions.ReadCompletionCallback(ref pendingContext.key, 
                                                         ref pendingContext.input, 
                                                         ref pendingContext.output, 
                                                         pendingContext.userContext,
                                                         status);
                    }
                    else
                    {
                        functions.RMWCompletionCallback(ref pendingContext.key,
                                                        ref pendingContext.input,
                                                        pendingContext.userContext,
                                                        status);
                    }
                }
            }
        }

    }
}

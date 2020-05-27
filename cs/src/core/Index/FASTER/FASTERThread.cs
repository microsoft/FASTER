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
    public partial class FasterKV<Key, Value, Input, Output, Context> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
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
                    var currentState = SystemState.Copy(ref systemState);
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
        internal void InternalRefresh(FasterExecutionContext ctx, ISynchronizationListener listener)
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref systemState);
            if (ctx.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && ctx.version == newPhaseInfo.version)
            {
                return;
            }

            // await is never invoked when calling the function with async = false
//#pragma warning disable 4014
            var task = ThreadStateMachineStep(ctx, listener, false);
            Debug.Assert(task.IsCompleted);
//#pragma warning restore 4014
        }

        internal void InitContext(FasterExecutionContext ctx, string token, long lsn = -1)
        {
            ctx.phase = Phase.REST;
            ctx.version = systemState.version;
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

        internal bool InternalCompletePending<Functions>(FasterExecutionContext ctx, Functions functions, ISynchronizationListener listener, bool wait = false)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            do
            {
                bool done = true;

                #region Previous pending requests
                if (!RelaxedCPR)
                {
                    if (ctx.phase == Phase.IN_PROGRESS || ctx.phase == Phase.WAIT_PENDING)
                    {
                        InternalCompletePendingRequests(ctx.prevCtx, ctx, functions, listener);
                        InternalCompleteRetryRequests(ctx.prevCtx, ctx, functions, listener);
                        InternalRefresh(ctx, listener);

                        done &= (ctx.prevCtx.HasNoPendingRequests);
                    }
                }
                #endregion

                InternalCompletePendingRequests(ctx, ctx, functions, listener);
                InternalCompleteRetryRequests(ctx, ctx, functions, listener);
                InternalRefresh(ctx, listener);

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

        internal bool InRestPhase() => systemState.phase == Phase.REST;

        #region Complete Retry Requests
        internal void InternalCompleteRetryRequests<Functions>(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, Functions functions, ISynchronizationListener listener)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            int count = opCtx.retryRequests.Count;

            if (count == 0) return;

            listener?.UnsafeResumeThread();
            for (int i = 0; i < count; i++)
            {
                var pendingContext = opCtx.retryRequests.Dequeue();
                InternalCompleteRetryRequest(opCtx, currentCtx, pendingContext, functions, listener);
            }
            listener?.UnsafeSuspendThread();
        }

        internal void InternalCompleteRetryRequest<Functions>(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, PendingContext pendingContext, Functions functions,
            ISynchronizationListener listener)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var internalStatus = default(OperationStatus);
            ref Key key = ref pendingContext.key.Get();
            ref Value value = ref pendingContext.value.Get();

            // Issue retry command
            switch (pendingContext.type)
            {
                case OperationType.RMW:
                    internalStatus = InternalRMW(ref key, ref pendingContext.input, ref pendingContext.userContext, ref pendingContext, functions, currentCtx, listener, pendingContext.serialNum);
                    break;
                case OperationType.UPSERT:
                    internalStatus = InternalUpsert(ref key, ref value, ref pendingContext.userContext, ref pendingContext, functions, currentCtx, listener, pendingContext.serialNum);
                    break;
                case OperationType.DELETE:
                    internalStatus = InternalDelete(ref key, ref pendingContext.userContext, ref pendingContext, functions, currentCtx, listener, pendingContext.serialNum);
                    break;
                case OperationType.READ:
                    throw new FasterException("Cannot happen!");
            }


            Status status;
            // Handle operation status
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(opCtx, currentCtx, pendingContext, functions, listener, internalStatus);
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
        internal void InternalCompletePendingRequests<Functions>(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, Functions functions, ISynchronizationListener listener)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (opCtx.readyResponses.Count == 0) return;

            while (opCtx.readyResponses.TryDequeue(out AsyncIOContext<Key, Value> request))
            {
                InternalCompletePendingRequest(opCtx, currentCtx, functions, listener, request);
            }
        }

        internal async ValueTask InternalCompletePendingRequestsAsync<Functions>(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, Functions functions, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
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
                        InternalCompletePendingRequest(opCtx, currentCtx, functions, clientSession, request);
                    }
                    clientSession.UnsafeSuspendThread();
                }
                else
                {
                    request = await opCtx.readyResponses.DequeueAsync(token);

                    clientSession.UnsafeResumeThread();
                    InternalCompletePendingRequest(opCtx, currentCtx, functions, clientSession, request);
                    clientSession.UnsafeSuspendThread();
                }
            }
        }

        internal void InternalCompletePendingRequest<Functions>(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, Functions functions, ISynchronizationListener listener, AsyncIOContext<Key, Value> request)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (opCtx.ioPendingRequests.TryGetValue(request.id, out PendingContext pendingContext))
            {
                ref Key key = ref pendingContext.key.Get();

                // Remove from pending dictionary
                opCtx.ioPendingRequests.Remove(request.id);

                OperationStatus internalStatus;
                // Issue the continue command
                if (pendingContext.type == OperationType.READ)
                {
                    internalStatus = InternalContinuePendingRead(opCtx, request, ref pendingContext, functions, currentCtx, listener);
                }
                else
                {
                    internalStatus = InternalContinuePendingRMW(opCtx, request, ref pendingContext, functions, currentCtx, listener);
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
                    status = HandleOperationStatus(opCtx, currentCtx, pendingContext, functions, listener, internalStatus);
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

        internal (Status, Output) InternalCompletePendingReadRequestAsync<Functions>(FasterExecutionContext opCtx, FasterExecutionContext currentCtx, Functions functions, ISynchronizationListener listener, AsyncIOContext<Key, Value> request, PendingContext pendingContext)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            (Status, Output) s = default;

            ref Key key = ref pendingContext.key.Get();

            OperationStatus internalStatus = InternalContinuePendingRead(opCtx, request, ref pendingContext, functions, currentCtx, listener);

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

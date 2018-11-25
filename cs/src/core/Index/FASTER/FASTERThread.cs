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
    public unsafe partial class FasterKV : FasterBase, IFasterKV
    {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Guid InternalAcquire()
        {
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long InternalContinue(Guid guid)
        {
            if (_hybridLogCheckpoint.info.continueTokens != null)
            {
                if (_hybridLogCheckpoint.info.continueTokens.TryGetValue(guid, out long serialNum))
                {
                    return serialNum;
                }
            }

            Debug.WriteLine("Unable to continue session " + guid.ToString());
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRelease()
        {
            Debug.Assert(threadCtx.retryRequests.Count == 0 &&
                    threadCtx.ioPendingRequests.Count == 0);
            if (prevThreadCtx != default(ExecutionContext))
            {
                Debug.Assert(prevThreadCtx.retryRequests.Count == 0 &&
                    prevThreadCtx.ioPendingRequests.Count == 0);
            }
            Debug.Assert(threadCtx.phase == Phase.REST);
            epoch.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitLocalContext(ref ExecutionContext context, Guid token)
        {
            context = new ExecutionContext
            {
                phase = _systemState.phase,
                version = _systemState.version,
                markers = new bool[8],
                serialNum = 0,
                totalPending = 0,
                guid = token,
                retryRequests = new Queue<PendingContext>(),
                readyResponses = new BlockingCollection<AsyncIOContext>(),
                ioPendingRequests = new Dictionary<long, PendingContext>()
            };

            for(int i = 0; i < 8; i++)
            {
                context.markers[i] = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CompleteRetryRequests(ExecutionContext context)
        {
            int count = context.retryRequests.Count;
            for (int i = 0; i < count; i++)
            {
                var pendingContext = context.retryRequests.Dequeue();
                InternalRetryRequestAndCallback(context, pendingContext);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CompleteIOPendingRequests(ExecutionContext context)
        {
            while (context.readyResponses.TryTake(out AsyncIOContext request))
            {
                InternalContinuePendingRequestAndCallback(context, request);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalRetryRequestAndCallback(
                                    ExecutionContext ctx,
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
                                                    pendingContext.value, 
                                                    pendingContext.userContext, 
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
                                                pendingContext.input,
                                                pendingContext.userContext, status);
                        break;
                    case OperationType.UPSERT:
                        functions.UpsertCompletionCallback(ref pendingContext.key,
                                                 pendingContext.value,
                                                 pendingContext.userContext);
                        break;
                    default:
                        throw new Exception("Operation type not allowed for retry");
                }
                
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalContinuePendingRequestAndCallback(
                                    ExecutionContext ctx,
                                    AsyncIOContext request)
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
                
                // Delete key, value, record
                if (default(Key).HasObjectsToSerialize())
                {
                    var physicalAddress = (long)request.record.GetValidPointer();
                    Layout.GetKey(physicalAddress).Free();
                }
                if (Value.HasObjectsToSerialize())
                {
                    var physicalAddress = (long)request.record.GetValidPointer();
                    Value.Free(Layout.GetValue(physicalAddress));
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
                                                         pendingContext.input, 
                                                         pendingContext.output, 
                                                         pendingContext.userContext,
                                                         status);
                    }
                    else
                    {
                        functions.RMWCompletionCallback(ref pendingContext.key,
                                                        pendingContext.input,
                                                        pendingContext.userContext,
                                                        status);
                    }
                }
            }
        }

    }
}

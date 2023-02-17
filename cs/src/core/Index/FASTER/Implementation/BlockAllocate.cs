// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool BlockAllocate<Input, Output, Context>(
                int recordSize,
                out long logicalAddress,
                ref PendingContext<Input, Output, Context> pendingContext,
                out OperationStatus internalStatus)
            => TryBlockAllocate(hlog, recordSize, out logicalAddress, ref pendingContext, out internalStatus);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool BlockAllocateReadCache<Input, Output, Context>(
                int recordSize,
                out long logicalAddress,
                ref PendingContext<Input, Output, Context> pendingContext,
                out OperationStatus internalStatus)
            => TryBlockAllocate(readcache, recordSize, out logicalAddress, ref pendingContext, out internalStatus);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryBlockAllocate<Input, Output, Context>(
                AllocatorBase<Key, Value> allocator,
                int recordSize,
                out long logicalAddress,
                ref PendingContext<Input, Output, Context> pendingContext,
                out OperationStatus internalStatus)
        {
            pendingContext.flushEvent = allocator.FlushEvent;
            logicalAddress = allocator.TryAllocate(recordSize);
            if (logicalAddress > 0)
            {
                pendingContext.flushEvent = default;
                internalStatus = OperationStatus.SUCCESS;
                return true;
            }

            if (logicalAddress == 0)
            {
                // We expect flushEvent to be signaled.
                internalStatus = OperationStatus.ALLOCATE_FAILED;
                return false;
            }

            // logicalAddress is < 0 so we do not expect flushEvent to be signaled; return RETRY_LATER to refresh the epoch.
            pendingContext.flushEvent = default;
            allocator.TryComplete();
            internalStatus = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAllocateRecord<Input, Output, Context>(ref PendingContext<Input, Output, Context> pendingContext, ref OperationStackContext<Key, Value> stackCtx,
                                                       int allocatedSize, bool recycle, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
        {
            status = OperationStatus.SUCCESS;
            if (recycle && GetAllocationForRetry(ref pendingContext, stackCtx.hei.Address, allocatedSize, out newLogicalAddress, out newPhysicalAddress))
                return true;

            // Spin to make sure newLogicalAddress is > recSrc.LatestLogicalAddress (the .PreviousAddress and CAS comparison value).
            for (; ; Thread.Yield() )
            {
                if (!BlockAllocate(allocatedSize, out newLogicalAddress, ref pendingContext, out status))
                    break;

                newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (newLogicalAddress > stackCtx.recSrc.LatestLogicalAddress)
                        return true;

                    // This allocation is below the necessary address so abandon it and repeat the loop.
                    hlog.GetInfo(newPhysicalAddress).SetInvalid();  // Skip on log scan
                    continue;
                }

                // In-memory source dropped below HeadAddress during BlockAllocate.
                if (recycle)
                    SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
                else
                    hlog.GetInfo(newPhysicalAddress).SetInvalid();  // Skip on log scan
                status = OperationStatus.RETRY_LATER;
                break;
            }

            return AllocationFailed(ref stackCtx, out newPhysicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAllocateRecordReadCache<Input, Output, Context>(ref PendingContext<Input, Output, Context> pendingContext, ref OperationStackContext<Key, Value> stackCtx,
                                                       int allocatedSize, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
        {
            // Spin to make sure the start of the tag chain is not readcache, or that newLogicalAddress is > the first address in the tag chain.
            for (; ; Thread.Yield())
            {
                if (!BlockAllocateReadCache(allocatedSize, out newLogicalAddress, ref pendingContext, out status))
                    break;

                newPhysicalAddress = readcache.GetPhysicalAddress(newLogicalAddress);
                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (!stackCtx.hei.IsReadCache || newLogicalAddress > stackCtx.hei.AbsoluteAddress)
                        return true;

                    // This allocation is below the necessary address so abandon it and repeat the loop.
                    ReadCacheAbandonRecord(newPhysicalAddress);
                    continue;
                }

                // In-memory source dropped below HeadAddress during BlockAllocate.
                ReadCacheAbandonRecord(newPhysicalAddress);                // We don't save readcache addresses (they'll eventually be evicted)
                status = OperationStatus.RETRY_LATER;
                break;
            }

            return AllocationFailed(ref stackCtx, out newPhysicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool AllocationFailed(ref OperationStackContext<Key, Value> stackCtx, out long newPhysicalAddress)
        {
            // Either BlockAllocate returned false or an in-memory source dropped below HeadAddress. If we have an in-memory lock it
            // means we are doing EphemeralOnly locking and if the recordInfo has gone below where we can reliably access it due to
            // BlockAllocate causing an epoch refresh, the page may have been evicted. Therefore, clear any in-memory lock flag before
            // we return RETRY_LATER. This eviction doesn't happen if other threads also have S locks on this address, because in that
            // case they will hold the epoch and prevent BlockAllocate from running OnPagesClosed.
            Debug.Assert(!stackCtx.recSrc.HasInMemoryLock || this.RecordInfoLocker.IsEnabled, "In-memory locks should be acquired only in EphemeralOnly locking mode");
            if (stackCtx.recSrc.InMemorySourceWasEvicted())
                stackCtx.recSrc.HasInMemoryLock = false;
            newPhysicalAddress = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SaveAllocationForRetry<Input, Output, Context>(ref PendingContext<Input, Output, Context> pendingContext, long logicalAddress, long physicalAddress, int allocatedSize)
        {
            ref var recordInfo = ref hlog.GetInfo(physicalAddress);
            recordInfo.SetInvalid();    // so log scan will skip it

            *(int*)Unsafe.AsPointer(ref hlog.GetValue(physicalAddress)) = allocatedSize;
            pendingContext.retryNewLogicalAddress = logicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool GetAllocationForRetry<Input, Output, Context>(ref PendingContext<Input, Output, Context> pendingContext, long minAddress, int minSize, out long newLogicalAddress, out long newPhysicalAddress)
        {
            // Use an earlier allocation from a failed operation, if possible.
            newLogicalAddress = pendingContext.retryNewLogicalAddress;
            newPhysicalAddress = 0;
            pendingContext.retryNewLogicalAddress = 0;
            if (newLogicalAddress < hlog.HeadAddress || newLogicalAddress <= minAddress)
                return false;
            newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
            int recordSize = *(int*)Unsafe.AsPointer(ref hlog.GetValue(newPhysicalAddress));
            return recordSize >= minSize;
        }
    }
}

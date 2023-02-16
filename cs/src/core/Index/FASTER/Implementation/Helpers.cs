// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static FASTER.core.Utility;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        private enum LatchDestination
        {
            CreateNewRecord,
            CreatePendingContext,
            NormalProcessing,
            Retry
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ref RecordInfo WriteNewRecordInfo(ref Key key, AllocatorBase<Key, Value> log, long newPhysicalAddress, bool inNewVersion, bool tombstone, long previousAddress)
        {
            ref RecordInfo recordInfo = ref log.GetInfo(newPhysicalAddress);
            RecordInfo.WriteInfo(ref recordInfo, inNewVersion, tombstone, previousAddress);
            log.Serialize(ref key, newPhysicalAddress);
            return ref recordInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkPage<Input, Output, Context>(long logicalAddress, FasterExecutionContext<Input, Output, Context> sessionCtx)
        {
            if (sessionCtx.phase == Phase.REST)
                hlog.MarkPage(logicalAddress, sessionCtx.version);
            else
                hlog.MarkPageAtomic(logicalAddress, sessionCtx.version);
        }

        /// <summary>
        /// This is a wrapper for checking the record's version instead of just peeking at the latest record at the tail of the bucket.
        /// By calling with the address of the traced record, we can prevent a different key sharing the same bucket from deceiving 
        /// the operation to think that the version of the key has reached v+1 and thus to incorrectly update in place.
        /// </summary>
        /// <param name="logicalAddress">The logical address of the traced record for the key</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CheckEntryVersionNew(long logicalAddress)
        {
            HashBucketEntry entry = new() { word = logicalAddress };
            return CheckBucketVersionNew(ref entry);
        }

        /// <summary>
        /// Check the version of the passed-in entry. 
        /// The semantics of this function are to check the tail of a bucket (indicated by entry), so we name it this way.
        /// </summary>
        /// <param name="entry">the last entry of a bucket</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CheckBucketVersionNew(ref HashBucketEntry entry)
        {
            // A version shift can only in an address after the checkpoint starts, as v_new threads RCU entries to the tail.
            if (entry.Address < _hybridLogCheckpoint.info.startLogicalAddress) 
                return false;

            // Read cache entries are not in new version
            if (UseReadCache && entry.ReadCache) 
                return false;

            // Check if record has the new version bit set
            var _addr = hlog.GetPhysicalAddress(entry.Address);
            if (entry.Address >= hlog.HeadAddress)
                return hlog.GetInfo(_addr).InNewVersion;
            else
                return false;
        }

        internal enum LatchOperation : byte
        {
            None,
            Shared,
            Exclusive
        }

        internal void SetRecordInvalid(long logicalAddress)
        {
            // This is called on exception recovery for a newly-inserted record.
            var localLog = IsReadCache(logicalAddress) ? readcache : hlog;
            ref var recordInfo = ref localLog.GetInfo(localLog.GetPhysicalAddress(AbsoluteAddress(logicalAddress)));
            recordInfo.SetInvalid();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CASRecordIntoChain(ref OperationStackContext<Key, Value> stackCtx, long newLogicalAddress)
        {
            return stackCtx.recSrc.LowestReadCachePhysicalAddress == Constants.kInvalidAddress
                ? stackCtx.hei.TryCAS(newLogicalAddress)
                : SpliceIntoHashChainAtReadCacheBoundary(ref stackCtx.recSrc, newLogicalAddress);
        }

        // Called after BlockAllocate or anything else that could shift HeadAddress, to adjust addresses or return false for RETRY as needed.
        // This refreshes the HashEntryInfo, so the caller needs to recheck to confirm the BlockAllocated address is still > hei.Address.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool VerifyInMemoryAddresses(ref OperationStackContext<Key, Value> stackCtx, long skipReadCacheStartAddress = Constants.kInvalidAddress)
        {
            // If we have an in-memory source that was evicted, return false and the caller will RETRY.
            if (stackCtx.recSrc.InMemorySourceWasEvicted())
            {
                // We are doing EphemeralOnly locking and the recordInfo has gone below where we can reliably access due to BlockAllocate
                // causing an epoch refresh (or similar); the page may have been evicted. Therefore, clear any in-memory lock flag before
                // we return RETRY_LATER. Note that this doesn't happen if other threads also have S locks on this address, because in that
                // case they will hold the epoch and prevent BlockAllocate from running OnPages Closed.
                Debug.Assert(!stackCtx.recSrc.HasInMemoryLock || this.RecordInfoLocker.IsEnabled, "In-memory locks should be acquired only in EphemeralOnly locking mode");
                stackCtx.recSrc.HasInMemoryLock = false;
                return false;
            }

            // If we're not using readcache or the splice point is still above readcache.HeadAddress, we're good.
            if (!UseReadCache || stackCtx.recSrc.LowestReadCacheLogicalAddress >= readcache.HeadAddress)
                return true;

            // Make sure skipReadCacheStartAddress is a readcache address (it likely is not only in the case where there are no readcache records).
            // This also ensures the comparison to readcache.HeadAddress below works correctly.
            if ((skipReadCacheStartAddress & Constants.kReadCacheBitMask) == 0)
                skipReadCacheStartAddress = Constants.kInvalidAddress;
            else
                skipReadCacheStartAddress &= ~Constants.kReadCacheBitMask;

            // The splice-point readcache record was evicted, so re-get it.
            while (true)
            {
                stackCtx.hei.SetToCurrent();
                stackCtx.recSrc.LatestLogicalAddress = stackCtx.hei.Address;
                if (!stackCtx.hei.IsReadCache)
                {
                    stackCtx.recSrc.LowestReadCacheLogicalAddress = Constants.kInvalidAddress;
                    stackCtx.recSrc.LowestReadCachePhysicalAddress = 0;
                    Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "ReadCacheSrc should not be evicted before SpinWaitUntilAddressIsClosed is called");
                    return true;
                }

                // Skip from the start address if it's valid, but do not overwrite the Has*Src information in recSrc.
                // We stripped the readcache bit from it above, so add it back here (if it's valid).
                if (skipReadCacheStartAddress < readcache.HeadAddress)
                    skipReadCacheStartAddress = Constants.kInvalidAddress;
                else
                    stackCtx.recSrc.LatestLogicalAddress = skipReadCacheStartAddress | Constants.kReadCacheBitMask;

                if (UseReadCache && SkipReadCache(ref stackCtx.recSrc.LatestLogicalAddress, out stackCtx.recSrc.LowestReadCacheLogicalAddress, out stackCtx.recSrc.LowestReadCachePhysicalAddress))
                {
                    Debug.Assert(stackCtx.hei.IsReadCache || stackCtx.hei.Address == stackCtx.recSrc.LatestLogicalAddress, "For non-readcache chains, recSrc.LatestLogicalAddress should == hei.Address");
                    return true;
                }

                // A false return from SkipReadCache means we traversed to where recSrc.LatestLogicalAddress is still in
                // the readcache but is < readcache.HeadAddress, so wait until it is evicted.
                SpinWaitUntilAddressIsClosed(stackCtx.recSrc.LatestLogicalAddress, readcache);

                // If we have an in-memory source that was evicted, return false and the caller will RETRY.
                if (stackCtx.recSrc.InMemorySourceWasEvicted())
                    return false;
            }
        }
    }
}

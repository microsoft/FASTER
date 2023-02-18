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
        private bool VerifyInMemoryAddresses(ref OperationStackContext<Key, Value> stackCtx)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemoryLock || this.RecordInfoLocker.IsEnabled, "In-memory locks should be acquired only in EphemeralOnly locking mode");

            // If we have an in-memory source that was evicted, return false and the caller will RETRY.
            if (stackCtx.recSrc.InMemorySourceWasEvicted())
                return false;

            // If we're not using readcache or the splice point is still above readcache.HeadAddress, we're good.
            if (!UseReadCache || stackCtx.recSrc.LowestReadCacheLogicalAddress >= readcache.HeadAddress)
                return true;

            // The splice-point readcache record may have been evicted, so re-get it.
            stackCtx.hei.SetToCurrent();

            // If we have a readcache source address then we can start skipping there (we've verified above it's still >= HeadAddress).
            // Otherwise we must start from the beginning of the readcache. The initial address must have kReadCacheBitMask.
            stackCtx.recSrc.LatestLogicalAddress = stackCtx.recSrc.HasReadCacheSrc
                ? stackCtx.recSrc.LogicalAddress | Constants.kReadCacheBitMask
                : stackCtx.hei.Address;

            SkipReadCache(ref stackCtx);
            Debug.Assert(stackCtx.hei.IsReadCache || stackCtx.hei.Address == stackCtx.recSrc.LatestLogicalAddress, "For non-readcache chains, recSrc.LatestLogicalAddress should == hei.Address");
            stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            return true;
        }
    }
}

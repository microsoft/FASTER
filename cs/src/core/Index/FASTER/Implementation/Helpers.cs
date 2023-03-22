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
            recordInfo.WriteInfo(inNewVersion, tombstone, previousAddress);
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
        private bool IsRecordVersionNew(long logicalAddress)
        {
            HashBucketEntry entry = new() { word = logicalAddress };
            return IsEntryVersionNew(ref entry);
        }

        /// <summary>
        /// Check the version of the passed-in entry. 
        /// The semantics of this function are to check the tail of a bucket (indicated by entry), so we name it this way.
        /// </summary>
        /// <param name="entry">the last entry of a bucket</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsEntryVersionNew(ref HashBucketEntry entry)
        {
            // A version shift can only happen in an address after the checkpoint starts, as v_new threads RCU entries to the tail.
            if (entry.Address < _hybridLogCheckpoint.info.startLogicalAddress) 
                return false;

            // Read cache entries are not in new version
            if (UseReadCache && entry.ReadCache) 
                return false;

            // If the record is in memory, check if it has the new version bit set
            if (entry.Address < hlog.HeadAddress)
                return false;
            return hlog.GetInfo(hlog.GetPhysicalAddress(entry.Address)).IsInNewVersion;
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
        private bool CASRecordIntoChain(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long newLogicalAddress, ref RecordInfo newRecordInfo)
        {
            // If Ephemeral locking, we consider this insertion to the mutable portion of the log as a "concurrent" operation, and
            // we don't want other threads accessing this record until we complete Post* (which unlock if doing Ephemeral locking).
            if (DoEphemeralLocking)
                newRecordInfo.InitializeLockExclusive();

            return stackCtx.recSrc.LowestReadCachePhysicalAddress == Constants.kInvalidAddress
                ? stackCtx.hei.TryCAS(newLogicalAddress)
                : SpliceIntoHashChainAtReadCacheBoundary(ref key, ref stackCtx, newLogicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostInsertAtTail(ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
        {
            if (stackCtx.recSrc.HasReadCacheSrc)
                srcRecordInfo.CloseAtomic();

            // If we are not using the LockTable, then we ElideAndReinsertReadCacheChain ensured no conflict between the readcache
            // and the newly-inserted record. Otherwise we spliced it in directly, in which case a competing readcache record may
            // have been inserted; if so, invalidate it.
            if (UseReadCache && LockTable.IsEnabled)
                ReadCacheCheckTailAfterSplice(ref key, ref stackCtx.hei);
        }

        // Called after BlockAllocate or anything else that could shift HeadAddress, to adjust addresses or return false for RETRY as needed.
        // This refreshes the HashEntryInfo, so the caller needs to recheck to confirm the BlockAllocated address is still > hei.Address.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool VerifyInMemoryAddresses(ref OperationStackContext<Key, Value> stackCtx)
        {
            // If we have an in-memory source that fell below HeadAddress, return false and the caller will RETRY_LATER.
            if (stackCtx.recSrc.HasInMemorySrc && stackCtx.recSrc.LogicalAddress < stackCtx.recSrc.Log.HeadAddress)
                return false;

            // If we're not using readcache or we don't have a splice point or it is still above readcache.HeadAddress, we're good.
            if (!UseReadCache || stackCtx.recSrc.LowestReadCacheLogicalAddress == Constants.kInvalidAddress || stackCtx.recSrc.LowestReadCacheLogicalAddress >= readcache.HeadAddress)
                return true;

            // If the splice point went below readcache.HeadAddress, we would have to wait for the chain to be fixed up by eviction,
            // so just return RETRY_LATER and restart the operation.
            return false;
        }
    }
}

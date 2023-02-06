// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static FASTER.core.LockUtility;
using static FASTER.core.Utility;

namespace FASTER.core
{
    // Partial file for readcache functions
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindInReadCache(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long untilAddress, bool alwaysFindLatestLA = true, bool waitForTentative = true)
        {
            Debug.Assert(UseReadCache, "Should not call FindInReadCache if !UseReadCache");
        RestartChain:
            // 'recSrc' has already been initialized to the address in 'hei'.
            if (!stackCtx.hei.IsReadCache)
                return false;

            stackCtx.recSrc.LogicalAddress = Constants.kInvalidAddress;
            stackCtx.recSrc.PhysicalAddress = 0;

            stackCtx.recSrc.LatestLogicalAddress &= ~Constants.kReadCacheBitMask;
            if (stackCtx.recSrc.LatestLogicalAddress < readcache.HeadAddress)
            {
                // The first entry in the hash chain is a readcache entry that is targeted for eviction.
                SpinWaitUntilAddressIsClosed(stackCtx.recSrc.LatestLogicalAddress, readcache);
                stackCtx.UpdateRecordSourceToCurrentHashEntry();
                goto RestartChain;
            }

            stackCtx.recSrc.LowestReadCacheLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            stackCtx.recSrc.LowestReadCachePhysicalAddress = readcache.GetPhysicalAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress);

            // untilAddress, if present, comes from the pre-pendingIO entry.Address; there may have been no readcache entries then.
            Debug.Assert((untilAddress & Constants.kReadCacheBitMask) != 0 || untilAddress == Constants.kInvalidAddress, "untilAddress must be readcache or kInvalidAddress");
            untilAddress &= ~Constants.kReadCacheBitMask;

            while (true)
            {
                // Use a non-ref local, because we update it below to remove the readcache bit.
                RecordInfo recordInfo = readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);

                // When traversing the readcache, we skip Invalid records. The semantics of Seal are that the operation is retried, so if we leave
                // Sealed records in the readcache, we'll never get past them. Therefore, we go from Tentative to Invalid if the Tentative record
                // has to be invalidated. There is only one scenario where we go Tentative -> Invalid in the readcache: when an updated record was
                // added to the main log. This record is *after* the Invalidated one, so it is safe to proceed. We don't go Tentative -> Invalid for
                // Read/CopyToReadCache; InternalContinuePendingRead makes sure there is not already a record in the readcache for a record just read
                // from disk, and the usual CAS-into-hashbucket operation to add a new readcache record will catch the case a subsequent one was added.
                if (recordInfo.Tentative && waitForTentative)
                {
                    // This is not a ref, so we have to re-get it.
                    ref var ri = ref readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);
                    SpinWaitWhileTentativeAndReturnValidity(ref ri);
                    recordInfo = ri;
                }

                // Return true if we find a read cache entry matching the key. Skip Invalid but *not* Intermediate; that's tested as part of lock acquisition.
                if (!recordInfo.Invalid && stackCtx.recSrc.LatestLogicalAddress > untilAddress && !stackCtx.recSrc.HasReadCacheSrc
                    && comparer.Equals(ref key, ref readcache.GetKey(stackCtx.recSrc.LowestReadCachePhysicalAddress)))
                {
                    // Keep these at the current readcache location; they'll be the caller's source record.
                    stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LowestReadCacheLogicalAddress;
                    stackCtx.recSrc.PhysicalAddress = stackCtx.recSrc.LowestReadCachePhysicalAddress;
                    stackCtx.recSrc.HasReadCacheSrc = true;
                    stackCtx.recSrc.Log = readcache;

                    // Read() does not need to continue past the found record; updaters need to continue to find latestLogicalAddress and lowestReadCache*Address.
                    if (!alwaysFindLatestLA)
                        return true;
                }

                // Is the previous record a main log record? If so, break out.
                if (!recordInfo.PreviousAddressIsReadCache)
                {
                    Debug.Assert(recordInfo.PreviousAddress >= hlog.BeginAddress, "Read cache chain should always end with a main-log entry");
                    stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
                    goto InMainLog;
                }

                recordInfo.PreviousAddress &= ~Constants.kReadCacheBitMask;
                if (recordInfo.PreviousAddress < readcache.HeadAddress)
                {
                    // We must wait until possible locks are transferred to the lock table by ReadCacheEvict. Due to address ordering, waiting for
                    // latestLogicalAddress also waits for any lower-address readcache records. This wait also ensures that ReadCacheEvict is complete
                    // for this chain, so the returned "lowest readcache addresses" are correct.
                    var prevHeadAddress = readcache.HeadAddress;
                    SpinWaitUntilAddressIsClosed(recordInfo.PreviousAddress, readcache);
                    if (readcache.HeadAddress == prevHeadAddress)
                    {
                        // HeadAddress is the same, so ReadCacheEvict should have updated the lowest readcache entry's .PreviousAddress to point to the main log.
                        recordInfo = readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);  // refresh the local copy
                        Debug.Assert(!recordInfo.PreviousAddressIsReadCache, "SpinWaitUntilRecordIsClosed should set recordInfo.PreviousAddress to a main-log entry if Headaddress is unchanged");
                        stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
                        goto InMainLog;
                    }

                    // SpinWaitUntilRecordIsClosed updated readcache.HeadAddress, so we must restart the chain.
                    stackCtx.UpdateRecordSourceToCurrentHashEntry();
                    goto RestartChain;
                }

                stackCtx.recSrc.LatestLogicalAddress = recordInfo.PreviousAddress;
                stackCtx.recSrc.LowestReadCacheLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                stackCtx.recSrc.LowestReadCachePhysicalAddress = readcache.GetPhysicalAddress(stackCtx.recSrc.LowestReadCacheLogicalAddress);
            }

        InMainLog:
            if (stackCtx.recSrc.HasReadCacheSrc)
                return true;

            // We did not find the record in the readcache, so set these to the start of the main log entries, and the caller will call TracebackForKeyMatch
            stackCtx.recSrc.LogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            stackCtx.recSrc.PhysicalAddress = 0; // do *not* call hlog.GetPhysicalAddress(); LogicalAddress may be below hlog.HeadAddress. Let the caller decide when to do this.
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SpliceIntoHashChainAtReadCacheBoundary(ref RecordSource<Key, Value> recSrc, long newLogicalAddress)
        {
            // Splice into the gap of the last readcache/first main log entries.
            Debug.Assert(recSrc.LowestReadCachePhysicalAddress >= readcache.HeadAddress, "LowestReadCachePhysicalAddress must be >= readcache.HeadAddress; caller should have called VerifyReadCacheSplicePoint");
            ref RecordInfo rcri = ref readcache.GetInfo(recSrc.LowestReadCachePhysicalAddress);
            return rcri.TryUpdateAddress(recSrc.LatestLogicalAddress, newLogicalAddress);
        }

        // Skip over all readcache records in this key's chain (advancing logicalAddress to the first non-readcache record we encounter).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SkipReadCache(ref HashEntryInfo hei, ref long logicalAddress)
        {
            while (!SkipReadCache(ref logicalAddress, out _, out _))
            {
                hei.SetToCurrent();
                logicalAddress = hei.Address;
            }
        }

        // Skip over all readcache records in this key's chain (advancing logicalAddress to the first non-readcache record we encounter
        // and returning the lowest readcache logical and phyical addresses). If we go below readcache.HeadAddress we can't find the
        // 'lowestReadCache*' output params, so return false and let the caller issue a retry. Otherwise, we reached the end of the 
        // readcache chain (if any), so return true.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SkipReadCache(ref long logicalAddress, out long lowestReadCacheLogicalAddress, out long lowestReadCachePhysicalAddress)
        {
            Debug.Assert(UseReadCache, "Should not call SkipReadCache if !UseReadCache");
            var entry = new HashBucketEntry() { word = logicalAddress };
            logicalAddress = entry.AbsoluteAddress;
            if (!entry.ReadCache || logicalAddress < readcache.HeadAddress)
            {
                lowestReadCacheLogicalAddress = Constants.kInvalidAddress;
                lowestReadCachePhysicalAddress = 0;
                return !entry.ReadCache;
            }

            var physicalAddress = readcache.GetPhysicalAddress(logicalAddress);

            while (true)
            {
                lowestReadCacheLogicalAddress = logicalAddress;
                lowestReadCachePhysicalAddress = physicalAddress;

                var recordInfo = readcache.GetInfo(physicalAddress);
                if (recordInfo.Tentative)
                {
                    // This is not a ref, so we have to re-get it.
                    ref var ri = ref readcache.GetInfo(physicalAddress);
                    SpinWaitWhileTentativeAndReturnValidity(ref ri);
                    recordInfo = ri;
                }

                // Look ahead to see if we're at the end of the readcache chain.
                entry.word = recordInfo.PreviousAddress;
                logicalAddress = entry.AbsoluteAddress;

                if (!entry.ReadCache || logicalAddress < readcache.HeadAddress)
                    return !entry.ReadCache;

                physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
            }
        }

        // Skip over all readcache records in all key chains in this bucket, updating the bucket to point to the first main log record.
        // Called during checkpointing; we create a copy of the hash table page, eliminate read cache pointers from this copy, then write this copy to disk.
        private void SkipReadCacheBucket(HashBucket* bucket)
        {
            for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
            {
                HashBucketEntry* entry = (HashBucketEntry*)&bucket->bucket_entries[index];
                if (0 == entry->word)
                    continue;

                if (!entry->ReadCache) continue;
                var logicalAddress = entry->Address;
                var physicalAddress = readcache.GetPhysicalAddress(AbsoluteAddress(logicalAddress));

                while (true)
                {
                    logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                    entry->Address = logicalAddress;
                    if (!entry->ReadCache)
                        break;
                    physicalAddress = readcache.GetPhysicalAddress(AbsoluteAddress(logicalAddress));
                }
            }
        }

        // Called after a readcache insert, to make sure there was no race with another session that added a main-log record at the same time.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool EnsureNoMainLogRecordWasAddedDuringReadCacheInsert(ref Key key, RecordSource<Key, Value> recSrc, long untilLogicalAddress, ref OperationStatus failStatus)
        {
            bool success = true;
            ref RecordInfo lowest_rcri = ref readcache.GetInfo(recSrc.LowestReadCachePhysicalAddress);
            Debug.Assert(!lowest_rcri.PreviousAddressIsReadCache, "lowest-rcri.PreviousAddress should be a main-log address");
            if (lowest_rcri.PreviousAddress > untilLogicalAddress)
            {
                // Someone added a new record in the splice region. It won't be readcache; that would've been added at tail. See if it's our key.
                // We want this whether it's Tentative or not, so don't wait for Tentative.
                var minAddress = untilLogicalAddress > hlog.HeadAddress ? untilLogicalAddress : hlog.HeadAddress;
                if (TraceBackForKeyMatch(ref key, lowest_rcri.PreviousAddress, minAddress + 1, out long prevAddress, out _, waitForTentative: false))
                    success = false;
                else if (prevAddress > untilLogicalAddress && prevAddress < hlog.HeadAddress)
                {
                    // One or more records were inserted and escaped to disk during the time of this Read/PENDING operation, untilLogicalAddress
                    // is below hlog.HeadAddress, and there are one or more inserted records between them:
                    //     hlog.HeadAddress -> [prevAddress is somewhere in here] -> untilLogicalAddress
                    // (If prevAddress is == untilLogicalAddress, we know there is nothing more recent, so the new readcache record should stay.)
                    // recSrc.HasLockTableLock may or may not be true. The new readcache record must be invalidated; then we return ON_DISK;
                    // this abandons the attempt to CopyToTail, and the caller proceeds with the possibly-stale value that was read (and any
                    // LockTable lock is released, with the LockTable entry remaining).
                    success = false;
                    failStatus = OperationStatus.RECORD_ON_DISK;
                }
            }
            return success;
        }
 
        // Called to check if another session added a readcache entry from a pending read while we were inserting an updated record. If so, then if
        // it is not locked, it is obsolete and can be Invalidated, and the update continues. Otherwise, the inserted record is either obsolete or
        // its update is disallowed because a read lock or better exists on that key, and so the inserted record must be invalidated.
        // Note: The caller will do no epoch-refreshing operations after re-verifying the readcache chain following record allocation, so it is not
        // possible for the chain to be disrupted and the new insertion lost, or for hei.Address to be below readcache.HeadAddress.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ReadCacheCompleteTwoPhaseUpdate(ref Key key, ref HashEntryInfo hei)
        {
            Debug.Assert(UseReadCache, "Should not call ReadCacheCompleteTwoPhaseUpdate if !UseReadCache");
            HashBucketEntry entry = new() { word = hei.CurrentAddress };
            HashBucketEntry untilEntry = new() { word = hei.Address };

            // Traverse for the key above untilAddress (which may not be in the readcache if there were no readcache records when it was retrieved).
            while (entry.ReadCache && (entry.Address > untilEntry.Address || !untilEntry.ReadCache))
            {
                var physicalAddress = readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (!recordInfo.Invalid && comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    if (SpinWaitWhileTentativeAndReturnValidity(ref recordInfo))
                        return recordInfo.SetInvalidAtomicIfNoLocks();
                }
                entry.word = recordInfo.PreviousAddress;
            }

            // If we're here, no record for 'key' was found.
            return true;
        }

        // Called to check if another session added a readcache entry from a pending read while we were doing CopyToTail of a pending read.
        // If so and it is unlocked or has only Read locks, we can transfer any locks to the new record. If it is XLocked, we fail.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ReadCacheCompleteTwoPhaseCopyToTail(ref Key key, ref HashEntryInfo hei, ref RecordInfo newRecordInfo, bool allowXLock, bool removeEphemeralLock)
        {
            Debug.Assert(UseReadCache, "Should not call ReadCacheCompleteTwoPhaseCopyToTail if !UseReadCache");
            HashBucketEntry entry = new() { word = hei.CurrentAddress };
            HashBucketEntry untilEntry = new() { word = hei.Address };

            // Traverse for the key above untilAddress (which may not be in the readcache if there were no readcache records when it was retrieved).
            while (entry.ReadCache && (entry.Address > untilEntry.Address || !untilEntry.ReadCache))
            {
                var physicalAddress = readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                ref RecordInfo recordInfo = ref readcache.GetInfo(physicalAddress);
                if (!recordInfo.Invalid && comparer.Equals(ref key, ref readcache.GetKey(physicalAddress)))
                {
                    if (SpinWaitWhileTentativeAndReturnValidity(ref recordInfo))
                        return newRecordInfo.CopyReadLocksFromAndMarkSourceAtomic(ref recordInfo, allowXLock, seal: false, removeEphemeralLock);
                }
                entry.word = recordInfo.PreviousAddress;
            }

            // If we're here, no (valid, non-Tentative) record for 'key' was found.
            return true;
        }

        internal void ReadCacheEvict(long rcLogicalAddress, long rcToLogicalAddress)
        {
            // Iterate readcache entries in the range rcFrom/ToLogicalAddress, and remove them from the hash chain.
            while (rcLogicalAddress < rcToLogicalAddress)
            {
                var rcPhysicalAddress = readcache.GetPhysicalAddress(rcLogicalAddress);
                var (_, rcAllocatedSize) = readcache.GetRecordSize(rcPhysicalAddress);
                var rcRecordInfo = readcache.GetInfo(rcPhysicalAddress);

                // Check PreviousAddress for null to handle the info.IsNull() "partial record at end of page" case as well as readcache CAS failures
                // (such failed records are not in the hash chain, so we must not process them here). We do process other Invalid records here.
                if (rcRecordInfo.PreviousAddress <= Constants.kTempInvalidAddress)
                    goto NextRecord;

                // If there are any readcache entries for this key, the hash chain will always be of the form:
                //                   |----- readcache records -----|    |------ main log records ------|
                //      hashtable -> rcN -> ... -> rc3 -> rc2 -> rc1 -> mN -> ... -> m3 -> m2 -> m1 -> 0

                // This diagram shows that this readcache record's PreviousAddress (in 'entry') is always a lower-readcache or non-readcache logicalAddress,
                // and therefore this record and the entire sub-chain "to the right" should be evicted. The sequence of events is:
                //  1. Get the key from the readcache for this to-be-evicted record.
                //  2. Call FindTag on that key in the main fkv to get the start of the hash chain.
                //  3. Walk the hash chain's readcache entries, removing records in the "to be removed" range.
                //     Do not remove Invalid records outside this range; that leads to race conditions.
                Debug.Assert(!rcRecordInfo.PreviousAddressIsReadCache || rcRecordInfo.AbsolutePreviousAddress < rcLogicalAddress, "Invalid record ordering in readcache");

                // Find the hash index entry for the key in the FKV's hash table.
                ref Key key = ref readcache.GetKey(rcPhysicalAddress);
                HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
                if (!FindTag(ref hei))
                    goto NextRecord;

                // Traverse the chain of readcache entries for this key, looking "ahead" to .PreviousAddress to see if it is less than readcache.HeadAddress.
                // nextPhysicalAddress remains Constants.kInvalidAddress if hei.Address is < HeadAddress; othrwise, it is the lowest-address readcache record
                // remaining following this eviction, and its .PreviousAddress is updated to each lower record in turn until we hit a non-readcache record.
                long nextPhysicalAddress = Constants.kInvalidAddress;
                HashBucketEntry entry = new() { word = hei.entry.word };
                while (entry.ReadCache)
                {
                    var la = entry.AbsoluteAddress;
                    var pa = readcache.GetPhysicalAddress(la);
                    ref RecordInfo ri = ref readcache.GetInfo(pa);

#if DEBUG
                    // Due to collisions, we can compare the hash code *mask* (i.e. the hash bucket index), not the key
                    var mask = state[resizeInfo.version].size_mask;
                    var rc_mask = hei.hash & mask;
                    var pa_mask = comparer.GetHashCode64(ref readcache.GetKey(pa)) & mask;
                    Debug.Assert(rc_mask == pa_mask, "The keyHash mask of the hash-chain ReadCache entry does not match the one obtained from the initial readcache address");
#endif

                    // If the record's address is above the eviction range, leave it there and track nextPhysicalAddress.
                    if (la >= rcToLogicalAddress)
                    {
                        nextPhysicalAddress = pa;
                        entry.word = ri.PreviousAddress;
                        continue;
                    }

                    // The record is being evicted. First transfer any locks. Other threads do not conflict; traversal, lock, and unlock operations all
                    // check for readcache addresses below readcache.HeadAddress and call SpinWaitUntilRecordIsClosed() as needed.
                    if (!ri.Invalid && ri.IsLocked)
                        this.LockTable.TransferFromLogRecord(ref readcache.GetKey(pa), ri);

                    // If we have a higher readcache record that is not being evicted, unlink 'la' by setting (nextPhysicalAddress).PreviousAddress to (la).PreviousAddress.
                    if (nextPhysicalAddress != Constants.kInvalidAddress)
                    {
                        ref RecordInfo nextri = ref readcache.GetInfo(nextPhysicalAddress);
                        if (nextri.TryUpdateAddress(entry.Address, ri.PreviousAddress))
                            ri.PreviousAddress = Constants.kTempInvalidAddress;     // The record is no longer in the chain
                        entry.word = nextri.PreviousAddress;
                        continue;
                    }

                    // We are evicting the record whose address is in the hash bucket; unlink 'la' by setting the hash bucket to point to (la).PreviousAddress.
                    if (hei.TryCAS(ri.PreviousAddress))
                        ri.PreviousAddress = Constants.kTempInvalidAddress;     // The record is no longer in the chain
                    else
                        hei.SetToCurrent();
                    entry.word = hei.entry.word;
                }

            NextRecord:
                if ((rcLogicalAddress & readcache.PageSizeMask) + rcAllocatedSize > readcache.PageSize)
                {
                    rcLogicalAddress = (1 + (rcLogicalAddress >> readcache.LogPageSizeBits)) << readcache.LogPageSizeBits;
                    continue;
                }
                rcLogicalAddress += rcAllocatedSize;
            }
        }
    }
}
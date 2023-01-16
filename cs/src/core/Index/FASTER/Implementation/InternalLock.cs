// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Manual Lock operation. Locks the record corresponding to 'key'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="lockOp">Lock operation being done.</param>
        /// <param name="lockInfo">Receives the recordInfo of the record being locked</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalLock(ref Key key, LockOperation lockOp, out RecordInfo lockInfo)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // If the record is in memory, then there can't be a LockTable lock.
            if (TryFindAndLockRecordInMemory(ref key, lockOp, out lockInfo, ref stackCtx, out OperationStatus lockStatus))
                return lockStatus;

            // Not in memory. First make sure the record has been transferred to the lock table if we did not find it because it was in the eviction region.
            var prevLogHA = hlog.HeadAddress;
            var prevReadCacheHA = UseReadCache ? readcache.HeadAddress : 0;
            if (stackCtx.recSrc.LogicalAddress >= stackCtx.recSrc.Log.BeginAddress)
                SpinWaitUntilRecordIsClosed(ref key, stackCtx.hei.hash, stackCtx.recSrc.LogicalAddress, stackCtx.recSrc.Log);

            // Do LockTable operations
            if (lockOp.LockOperationType == LockOperationType.IsLocked)
                return (!this.LockTable.IsActive || this.LockTable.TryGet(ref key, stackCtx.hei.hash, out lockInfo)) ? OperationStatus.SUCCESS : OperationStatus.RETRY_LATER;

            if (lockOp.LockOperationType == LockOperationType.Unlock)
            {
                if (this.LockTable.Unlock(ref key, stackCtx.hei.hash, lockOp.LockType))
                    return OperationStatus.SUCCESS;

                // We may need to recheck in-memory, due to a race where, when T1 started this InternalLock call, the key was not in the hash table
                // (it was a nonexistent key) but was in the LockTable:
                //  T1 did TryFindAndUnlockRecordInMemory above, and did not find the key in the hash table
                //  T2 did an Upsert of the key, which inserted a tentative entry into the log, then transferred the lock from the LockTable to that log record
                //      Or, T2 completed a pending Read and did CopyToTail or CopyToReadCache
                //  T1 would fail LockTable.Unlock and leave a locked record in the log
                // If the address in the HashEntryInfo has changed, or if hei has a readcache address and either we can't navigate from the lowest readcache
                // address (due to it being below HeadAddress) or its previous address does not point to the same address as when we started (which means a
                // new log entry was spliced in, then we retry in-memory.
                if (stackCtx.hei.IsNotCurrent || 
                        (stackCtx.hei.IsReadCache
                        && (stackCtx.recSrc.LowestReadCachePhysicalAddress < readcache.HeadAddress 
                            || readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress).PreviousAddress != stackCtx.recSrc.LatestLogicalAddress)))
                {
                    stackCtx.hei.SetToCurrent();
                    stackCtx.SetRecordSourceToHashEntry(hlog);
                    if (TryFindAndLockRecordInMemory(ref key, lockOp, out lockInfo, ref stackCtx, out lockStatus))
                        return lockStatus;

                    // If the HeadAddresses have changed, then the key may have dropped below it and was/will be evicted back to the LockTable.
                    if (hlog.HeadAddress != prevLogHA || (UseReadCache && readcache.HeadAddress != prevReadCacheHA))
                        return OperationStatus.RETRY_LATER;
                }

                Debug.Fail("Trying to unlock a nonexistent key");
                return OperationStatus.SUCCESS; // SUCCEED so we don't continue the loop; TODO change to OperationStatus.NOTFOUND and return false from Lock API
            }

            // Try to lock. One of the following things can happen here:
            //  - We find a record in the LockTable and:
            //    - It is tentative; we fail the lock and return RETRY_LATER
            //    - It is not tentative; we either:
            //      - Succeed with the lock (probably an additional S lock) and return SUCCESS
            //      - Fail the lock and return RETRY_LATER
            //  - The LockTable failed to insert a record
            //  - We did not find a record so we added one, so proceed with two-phase insert protocol below.
            if (!this.LockTable.TryLockManual(ref key, stackCtx.hei.hash, lockOp.LockType, out bool tentativeLock))
                return OperationStatus.RETRY_LATER;

            // We got the lock. If a new record with this key was inserted into the main log or readcache after we started, remove the lock we just added and RETRY.
            OperationStackContext<Key, Value> stackCtx2 = new(stackCtx.hei.hash);
            if (FindTag(ref stackCtx2.hei))
            {
                stackCtx2.SetRecordSourceToHashEntry(hlog);

                // First look in the readcache, then in memory. If there's any record there, Tentative or not, we back off this lock and retry.
                // The way two-phase insertion to the log (or readcache) works, the inserters will see our LockTable record and wait for it to become
                // non-tentative, which means the lock is permanent. If so, we won the race here, and it must be assumed our caller proceeded under
                // the assumption they had the lock. (Otherwise, we remove the lock table entry here, and the other thread proceeds). That means we
                // can't wait for tentative records here; that would deadlock (we wait for them to become non-tentative and they wait for us to become
                // non-tentative). So we must bring the records back here even if they are tentative, then bail on them.
                // Note: We don't use TryFindRecordInMemory here because we only want to scan the tail portion of the hash chain; we've already searched
                // below that, with the TryFindAndLockRecordInMemory call above.
                var found = false;
                if (stackCtx2.hei.IsReadCache && (!stackCtx.hei.IsReadCache || stackCtx2.hei.Address > stackCtx.hei.Address))
                {
                    // stackCtx2 has readcache records. If stackCtx.hei is a readcache record, then we just have to search down to that record;
                    // otherwise we search the entire readcache. We only need to find the latest logical address if stackCtx.hei is *not* a readcache record.
                    var untilAddress = stackCtx.hei.IsReadCache ? stackCtx.hei.Address : Constants.kInvalidAddress;
                    found = FindInReadCache(ref key, ref stackCtx2, untilAddress, alwaysFindLatestLA: !stackCtx.hei.IsReadCache, waitForTentative: false);
                }

                if (!found)
                {
                    // Search the main log. Since we did not find the key in the readcache, we have either:
                    //  - stackCtx.hei is not a readcache record: we have the most current LowestReadCache info in stackCtx2 (which may be none, if there are no readcache records)
                    //  - stackCtx.hei is a readcache record: stackCtx2 stopped searching before that, so stackCtx1 has the most recent readcache info
                    var lowestRcPhysicalAddress = stackCtx.hei.IsReadCache ? stackCtx.recSrc.LowestReadCachePhysicalAddress : stackCtx2.recSrc.LowestReadCachePhysicalAddress;
                    var latestlogicalAddress = lowestRcPhysicalAddress != 0 ? readcache.GetInfo(lowestRcPhysicalAddress).PreviousAddress : stackCtx2.hei.Address;
                    if (latestlogicalAddress > stackCtx.recSrc.LatestLogicalAddress)
                    {
                        var minAddress = stackCtx.recSrc.LatestLogicalAddress > hlog.HeadAddress ? stackCtx.recSrc.LatestLogicalAddress : hlog.HeadAddress;
                        found = TraceBackForKeyMatch(ref key, stackCtx2.hei.Address, minAddress + 1, out _, out _, waitForTentative: false);
                    }
                }

                if (found)
                {
                    LockTable.UnlockOrRemoveTentativeEntry(ref key, stackCtx.hei.hash, lockOp.LockType, tentativeLock);
                    return OperationStatus.RETRY_LATER;
                }
            }

            // Success
            if (tentativeLock && !this.LockTable.ClearTentativeBit(ref key, stackCtx.hei.hash))
                return OperationStatus.RETRY_LATER;     // The tentative record was not found, so the lock has not been done; retry
            return OperationStatus.SUCCESS;
        }

        /// <summary>Locks the record if it can find it in memory.</summary>
        /// <returns>True if the key was found in memory, else false. 'lockStatus' returns the lock status, if found, else should be ignored.</returns>
        private bool TryFindAndLockRecordInMemory(ref Key key, LockOperation lockOp, out RecordInfo lockInfo, ref OperationStackContext<Key, Value> stackCtx, out OperationStatus lockStatus)
        {
            lockInfo = default;
            if (TryFindRecordInMemory(ref key, ref stackCtx, minOffset: hlog.HeadAddress))
            {
                ref RecordInfo recordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                if (!recordInfo.IsIntermediate(out lockStatus))
                {
                    if (lockOp.LockOperationType == LockOperationType.IsLocked)
                        lockStatus = OperationStatus.SUCCESS;
                    else if (!recordInfo.TryLockOperation(lockOp))
                    {
                        // TODO: Consider eliding the record (as in InternalRMW) from the hash table if we are X-unlocking a Tombstoned record.
                        lockStatus = OperationStatus.RETRY_LATER;
                        return true;
                    }
                }
                if (lockOp.LockOperationType == LockOperationType.IsLocked)
                    lockInfo = recordInfo;
                return true;
            }
            lockStatus = OperationStatus.SUCCESS;
            return false;
        }
    }
}

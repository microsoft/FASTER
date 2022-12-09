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
        /// <param name ="oneMiss">Indicates whether we had a missing record once before. We go around to the top to retry once if an expected LockTable record does not exist;
        ///     this handles the race where we try to unlock as lock records are transferred out of the lock table, but the caller got in before we inserted the Tentative record.</param>
        /// <param name="lockInfo">Receives the recordInfo of the record being locked</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalLock(ref Key key, LockOperation lockOp, ref bool oneMiss, out RecordInfo lockInfo)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have epoch protected");

            OperationStackContext<Key, Value> stackCtx = new (comparer.GetHashCode64(ref key));
            FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            lockInfo = default;
            if (FindRecordInMemory(ref key, ref stackCtx, minOffset: hlog.HeadAddress))
            { 
                ref RecordInfo recordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                if (!recordInfo.IsIntermediate(out OperationStatus status))
                {
                    if (lockOp.LockOperationType == LockOperationType.IsLocked)
                        status = OperationStatus.SUCCESS;
                    else if (!recordInfo.TryLockOperation(lockOp))
                        return OperationStatus.RETRY_LATER;
                    // TODO: Consider eliding the record (as in InternalRMW) from the hash table if we are X-unlocking a Tombstoned record.
                }
                if (lockOp.LockOperationType == LockOperationType.IsLocked)
                    lockInfo = recordInfo;
                return status;
            }

            // Not in memory. Do LockTable operations
            if (lockOp.LockOperationType == LockOperationType.IsLocked)
                return (!this.LockTable.IsActive || this.LockTable.TryGet(ref key, stackCtx.hei.hash, out lockInfo)) ? OperationStatus.SUCCESS : OperationStatus.RETRY_LATER;

            if (lockOp.LockOperationType == LockOperationType.Unlock)
            {
                if (this.LockTable.Unlock(ref key, stackCtx.hei.hash, lockOp.LockType))
                    return OperationStatus.SUCCESS;
                if (oneMiss)
                {
                    Debug.Fail("Trying to unlock a nonexistent key");
                    return OperationStatus.SUCCESS; // SUCCEED so we don't continue the loop; TODO change to OperationStatus.NOTFOUND and return false from Lock API
                }
                oneMiss = true;
                return OperationStatus.RETRY_NOW;   // oneMiss does not need an epoch refresh as there should be a (possibly tentative) record inserted at tail
            }

            // Try to lock
            if (!this.LockTable.TryLockManual(ref key, stackCtx.hei.hash, lockOp.LockType, out bool tentativeLock))
                return OperationStatus.RETRY_LATER;

            // We got the lock. If a new record with this key was inserted into the main log or readcache after we started, remove the lock we just added and RETRY.
            OperationStackContext<Key, Value> stackCtx2 = new(stackCtx.hei.hash);
            if (FindTag(ref stackCtx2.hei))
            {
                stackCtx2.SetRecordSourceToHashEntry(hlog);

                // First look in the readcache, then in memory. If there's any record there, Tentative or not, we back off this lock and retry.
                // The way two-phase insertion to the log (or readcache) works, the inserters will see our LockTable record and wait for it to become
                // non-tentative, then see if the lock was permanent. If so, we won the race here, and it must be assumed our caller proceeded under
                // the assumption they had the lock. (Otherwise, we remove the lock table entry here, and the other thread proceeds). That means we
                // can't wait for tentative records here; that would deadlock (we wait for them to become non-tentative and they wait for us to become
                // non-tentative). So we must bring the records back here even if they are tentative, then bail on them.
                var found = false;
                if (stackCtx2.hei.IsReadCache && (!stackCtx.hei.IsReadCache || stackCtx2.hei.Address > stackCtx.hei.Address))
                {
                    var untilAddress = stackCtx.hei.IsReadCache ? stackCtx.hei.Address : Constants.kInvalidAddress;
                    found = FindInReadCache(ref key, ref stackCtx2, untilAddress, alwaysFindLatestLA: !stackCtx.hei.IsReadCache, waitForTentative: false);
                }

                if (!found)
                {
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
            if (tentativeLock)
            {
                if (this.LockTable.ClearTentativeBit(ref key, stackCtx.hei.hash))
                    return OperationStatus.SUCCESS;

                Debug.Fail("Should have found our tentative record");
                return OperationStatus.RETRY_NOW;   // The tentative record was not there, so someone else removed it; retry does not need epoch refresh
            }
            return OperationStatus.SUCCESS;
        }
    }
}

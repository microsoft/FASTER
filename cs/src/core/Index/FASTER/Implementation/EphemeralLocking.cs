// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus TryFindAndEphemeralLockAuxiliaryRecord<Input, Output, Context, FasterSession>(
                FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                LockType lockType, long prevHighestKeyHashAddress = Constants.kInvalidAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (UseReadCache && FindInReadCache(ref key, ref stackCtx, 
                                                untilAddress: (prevHighestKeyHashAddress & Constants.kReadCacheBitMask) != 0 ? prevHighestKeyHashAddress : Constants.kInvalidAddress))
            {
                ref var recordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                var ok = lockType == LockType.Shared
                    ? fasterSession.TryLockEphemeralShared(ref recordInfo)
                    : fasterSession.TryLockEphemeralExclusive(ref recordInfo);
                if (!ok)
                    return OperationStatus.RETRY_LATER;
                stackCtx.recSrc.HasInMemoryLock = true;
            }
            else if (LockTable.IsActive)
            {
                if (!fasterSession.DisableEphemeralLocking && !LockTable.TryLockEphemeral(ref key, stackCtx.hei.hash, lockType, out stackCtx.recSrc.HasLockTableLock))
                    return OperationStatus.RETRY_LATER;
            }
            return OperationStatus.SUCCESS;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool TryEphemeralXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (fasterSession.DisableEphemeralLocking)
                return true;

            // A failed lockOp means this is an intermediate record, e.g. Tentative or Sealed, or we exhausted the spin count. All these must RETRY_LATER.
            if (!fasterSession.TryLockEphemeralExclusive(ref recordInfo))
                status = OperationStatus.RETRY_LATER;
            else if (!IsRecordValid(recordInfo, out status))
                fasterSession.UnlockEphemeralExclusive(ref recordInfo);
            else
                recSrc.HasInMemoryLock = true;
            return recSrc.HasInMemoryLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool TryEphemeralSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (fasterSession.DisableEphemeralLocking)
                return true;

            // A failed lockOp means this is an intermediate record, e.g. Tentative or Sealed, or we exhausted the spin count. All these must RETRY_LATER.
            if (!fasterSession.TryLockEphemeralShared(ref recordInfo))
                status = OperationStatus.RETRY_LATER;
            else if (!IsRecordValid(recordInfo, out status))
                fasterSession.TryUnlockEphemeralShared(ref recordInfo);
            else
                recSrc.HasInMemoryLock = true;
            return recSrc.HasInMemoryLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void EphemeralSUnlock(ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo recordInfo)
        {
            if (!stackCtx.recSrc.HasInMemoryLock)
                return;

            // Updaters (Upsert, RMW, Delete) XLock records. Readers do not, including anything calling InternalTryCopyToTail. This means the record may
            // be transferred from the readcache to the main log (or even to the LockTable, if the record was in the (SafeHeadAddress, ClosedUntilAddress)
            // interval when a Read started).

            // If the record dived below HeadAddress, we must wait for it to enter the lock table before unlocking.
            if (stackCtx.recSrc.LogicalAddress < stackCtx.recSrc.Log.HeadAddress)
            {
                SpinWaitUntilRecordIsClosed(ref key, stackCtx.hei.hash, stackCtx.recSrc.LogicalAddress, stackCtx.recSrc.Log);
                LockTable.Unlock(ref key, stackCtx.hei.hash, LockType.Shared);
            }
            else if (!recordInfo.TryUnlockShared())
            {
                // Normal unlock failed, so the locks were transferred to another recordInfo; do a standard unlock to chase the key through the full process.
                bool oneMiss = false;
                InternalLock(ref key, new(LockOperationType.Unlock, LockType.Shared), ref oneMiss, out _);
            }
            stackCtx.recSrc.HasInMemoryLock = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlockAfterUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key,
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (fasterSession.DisableEphemeralLocking)
            {
                Debug.Assert(!stackCtx.recSrc.HasLockTableLock, "HasLockTableLock should only be true if we are doing ephemeral locking");
                return;
            }

            // Unlock exclusive locks, if any. 
            if (stackCtx.recSrc.HasInMemoryLock)
            {
                // This unlocks the source (old) record; the new record may already be operated on by other threads, which is fine.
                if (stackCtx.recSrc.LogicalAddress >= stackCtx.recSrc.Log.HeadAddress)
                {
                    // The record is now Invalid or Sealed, but we have to unlock it so any threads waiting on it can continue.
                    fasterSession.UnlockEphemeralExclusive(ref srcRecordInfo);
                }
                else
                {
                    // We must always wait until the lock table entry is in place; it will be orphaned because we've transferred the record,
                    // so we must remove it from the LockTable.
                    SpinWaitUntilRecordIsClosed(ref key, stackCtx.hei.hash, stackCtx.recSrc.LogicalAddress, stackCtx.recSrc.Log);
                    LockTable.Remove(ref key, stackCtx.hei.hash);
                }
                stackCtx.recSrc.HasInMemoryLock = false;
                return;
            }

            if (stackCtx.recSrc.HasLockTableLock)
            {
                LockTable.Unlock(ref key, stackCtx.hei.hash, LockType.Exclusive);
                stackCtx.recSrc.HasLockTableLock = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!fasterSession.DisableEphemeralLocking)
                fasterSession.UnlockEphemeralExclusive(ref srcRecordInfo);
            recSrc.ClearSrc();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralSUnlockAfterPendingIO<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key,
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (fasterSession.DisableEphemeralLocking)
            {
                Debug.Assert(!stackCtx.recSrc.HasLockTableLock, "HasLockTableLock should only be true if we are doing ephemeral locking");
                return;
            }

            // Unlock read locks, if any.
            if (stackCtx.recSrc.HasInMemoryLock)
            {
                // This unlocks the source (old) record; the new record may already be operated on by other threads, which is fine.
                EphemeralSUnlock(ref key, ref stackCtx, ref srcRecordInfo);
                return;
            }

            if (stackCtx.recSrc.HasLockTableLock)
                LockTable.Unlock(ref key, stackCtx.hei.hash, LockType.Shared);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CompleteTwoPhaseUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // We don't check for ephemeral xlocking here; we know we had that lock, but we don't need to actually lock the new record because
            // we know this is the last step and we are going to unlock it immediately; it is protected until we remove the Tentative bit.

            if (fasterSession.IsManualLocking)
            {
                // For manual locking, we should already have made sure there is an XLock for this. Preserve it on the new record.
                // If there is a LockTable entry, transfer from it (which will remove it from the LockTable); otherwise just set the bit directly.
                if (!LockTable.IsActive || !LockTable.TransferToLogRecord(ref key, stackCtx.hei.hash, ref newRecordInfo))
                    newRecordInfo.InitializeLockExclusive();
            }
            else if ((LockTable.IsActive && !LockTable.CompleteTwoPhaseUpdate(ref key, stackCtx.hei.hash))
                    || (UseReadCache && !ReadCacheCompleteTwoPhaseUpdate(ref key, ref stackCtx.hei)))
            {
                // A permanent LockTable entry or a ReadCache entry with a lock was added before we inserted the tentative record, so we must invalidate the new record and retry.
                // We cannot reuse the allocation because it's in the hash chain. // TODO consider eliding similar to InternalDelete
                stackCtx.SetNewRecordInvalidAtomic(ref newRecordInfo);
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            status = OperationStatus.SUCCESS;
            stackCtx.recSrc.MarkSourceRecordAfterSuccessfulCopyUpdate<Input, Output, Context, FasterSession>(fasterSession, ref srcRecordInfo);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CompleteTwoPhaseCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // Transfer locks; these will always be read locks and include the caller's read lock if we've not disabled ephemeral locking.
            bool success = true;
            if (stackCtx.recSrc.HasInMemorySrc)
            {
                // We're copying from immutable or readcache. If the locked record has gone below HeadAddress due to the BlockAllocate,
                // we must wait until the record is closed and transferred to the lock table, then transfer the locks from there.
                if (stackCtx.recSrc.LogicalAddress >= stackCtx.recSrc.Log.HeadAddress)  // TODO: This may not need to be checked, since we passed VerifyInMemoryAddresses
                {
                    // Unlock the ephemeral lock here; we mark the source so we *know* we will have an invalid unlock on srcRecordInfo and would have to chase
                    // through InternalLock to unlock it, so we save the time by not transferring our ephemeral lock; 'Tentative' still protects the new record.
                    newRecordInfo.TransferReadLocksFromAndMarkSourceAtomic(ref srcRecordInfo, allowXLock: fasterSession.IsManualLocking,
                                                                  seal: stackCtx.recSrc.HasMainLogSrc, removeEphemeralLock: stackCtx.recSrc.HasInMemoryLock);
                }
                else
                {
                    SpinWaitUntilRecordIsClosed(ref key, stackCtx.hei.hash, stackCtx.recSrc.LogicalAddress, stackCtx.recSrc.Log);
                    success = !LockTable.IsActive || LockTable.CompleteTwoPhaseCopyToTail(ref key, stackCtx.hei.hash, ref newRecordInfo,
                                                        allowXLock: fasterSession.IsManualLocking, removeEphemeralLock: stackCtx.recSrc.HasInMemoryLock);  // we acquired the lock via HasInMemoryLock
                }
                stackCtx.recSrc.HasInMemoryLock = false;
            }
            else
            {
                if (fasterSession.IsManualLocking)
                {
                    // For manual locking, we should already have made sure there is at least an SLock for this; with no HasInMemorySrc, it is in the Lock Table.
                    if (LockTable.IsActive)
                        LockTable.TransferToLogRecord(ref key, stackCtx.hei.hash, ref newRecordInfo);
                }
                else
                {
                    // XLocks are not allowed, because another thread owns them.
                    success = (!LockTable.IsActive || LockTable.CompleteTwoPhaseCopyToTail(ref key, stackCtx.hei.hash, ref newRecordInfo, allowXLock: fasterSession.IsManualLocking,
                                                                                          removeEphemeralLock: stackCtx.recSrc.HasLockTableLock))
                              &&
                              (!UseReadCache || ReadCacheCompleteTwoPhaseCopyToTail(ref key, ref stackCtx.hei, ref newRecordInfo, allowXLock: fasterSession.IsManualLocking,
                                                                                    removeEphemeralLock: stackCtx.recSrc.HasLockTableLock));
                    stackCtx.recSrc.HasLockTableLock = false;
                }
            }
            return success;
        }
    }
}

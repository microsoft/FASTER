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
        private bool TryFindRecordInMemoryAfterPendingIO(ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                            LockType lockType, long prevHighestKeyHashAddress = Constants.kInvalidAddress)
        {
            if (UseReadCache && FindInReadCache(ref key, ref stackCtx,
                                                untilAddress: (prevHighestKeyHashAddress & Constants.kReadCacheBitMask) != 0 ? prevHighestKeyHashAddress : Constants.kInvalidAddress))
                return true;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minOffset: hlog.HeadAddress, waitForTentative: true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryEphemeralOnlyXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (!this.EphemeralOnlyLocker.IsEnabled)
                return false;

            // A failed lockOp means this is a Sealed record or we exhausted the spin count. Either must RETRY_LATER, as must a record that was Invalidated while we spun.
            if (!this.EphemeralOnlyLocker.TryLockExclusive(ref recordInfo))
                status = OperationStatus.RETRY_LATER;
            else if (!IsRecordValid(recordInfo, out status))
            {
                this.EphemeralOnlyLocker.UnlockExclusive(ref recordInfo);
                status = OperationStatus.RETRY_LATER;
            }
            else
                recSrc.HasInMemoryLock = true;
            return recSrc.HasInMemoryLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryEphemeralOnlySLock(ref RecordSource<Key, Value> recSrc, ref RecordInfo recordInfo, out OperationStatus status)
        {
            status = OperationStatus.SUCCESS;
            if (!this.EphemeralOnlyLocker.IsEnabled)
                return false;

            // A failed lockOp means this is a Sealed record or we exhausted the spin count. Either must RETRY_LATER, as must a record that was Invalidated while we spun.
            if (!this.EphemeralOnlyLocker.TryLockShared(ref recordInfo))
                status = OperationStatus.RETRY_LATER;
            else if (!IsRecordValid(recordInfo, out status))
            {
                this.EphemeralOnlyLocker.UnlockShared(ref recordInfo);
                status = OperationStatus.RETRY_LATER;
            }
            else
                recSrc.HasInMemoryLock = true;
            return recSrc.HasInMemoryLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryEphemeralSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (this.ManualLockTable.IsEnabled)
            {
                if (fasterSession.TryLockEphemeralExclusive(ref key, ref stackCtx))
                {
                    status = OperationStatus.SUCCESS;
                    return true;
                }
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            return TryEphemeralOnlySLock(ref stackCtx.recSrc, ref recordInfo, out status);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void EphemeralSUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, 
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo recordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasLock)
                return;

            if (this.EphemeralOnlyLocker.IsEnabled)
            {
                // We precheck that the address was above HeadAddress before we get this far, so even if HeadAddress changes, the record can't be evicted
                // while we hold the epoch.
                Debug.Assert(stackCtx.recSrc.HasInMemoryLock, "Should have an InMemoryLock when we have an ephemeral lock with RecordInfoLocker enabled");

                // TODO (see comments in CompleteCopyToTail also) Because this is an S lock, it is possible that the record was transferred from by another
                // session on CopyToTail or CopyToReadCache (it would not have been a copy due to update, because that would've taken an X lock). We do not
                // Seal or Invalidate here, so if the record is Sealed or Invalid after the unlock, it means we have to go find it wherever it is now (still
                // in memory) and unlock it there. However this could cause problems if the new location goes below HeadAddress (even though it can't be
                // evicted while we hold the epoch.. which could potentially cause deadlock). The alternative is to spin in CompleteCopyToTail until all
                // readers are drained.. which also may be a deadlock danger).
                this.EphemeralOnlyLocker.UnlockShared(ref recordInfo);

                stackCtx.recSrc.HasInMemoryLock = false;
            }
            else
            {
                Debug.Assert(this.ManualLockTable.IsEnabled, "If we're here, one of the locking systems should be enabled");
                Debug.Assert(stackCtx.recSrc.HasLockTableLock, "Should have an InMemoryLock when we have an ephemeral lock with ManualLockTable enabled");
                fasterSession.UnlockEphemeralShared(ref key, ref stackCtx);
                stackCtx.recSrc.HasLockTableLock = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlockAfterUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key,
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasLock)
                return;

            // Note that this unlocks the *old* record, not the one copied to.
            if (this.EphemeralOnlyLocker.IsEnabled)
            {
                // We precheck that the address was above HeadAddress before we get this far, so even if HeadAddress changes, the record can't be evicted
                // while we hold the epoch.
                Debug.Assert(stackCtx.recSrc.HasInMemoryLock, "Should have an InMemoryLock when we have an ephemeral lock with RecordInfoLocker enabled");
                this.EphemeralOnlyLocker.UnlockExclusive(ref srcRecordInfo);
                stackCtx.recSrc.HasInMemoryLock = false;
            }
            else
            {
                Debug.Assert(this.ManualLockTable.IsEnabled, "If we're here, one of the locking systems should be enabled");
                Debug.Assert(stackCtx.recSrc.HasLockTableLock, "Should have an InMemoryLock when we have an ephemeral lock with ManualLockTable enabled");
                fasterSession.UnlockEphemeralExclusive(ref key, ref stackCtx);
                stackCtx.recSrc.HasLockTableLock = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            EphemeralXUnlockAfterUpdate<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
            stackCtx.recSrc.ClearSrc();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralSUnlockAfterPendingIO<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, 
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // This unlocks the source (old) record; the new record may already be operated on by other threads, which is fine.
            EphemeralSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (UseReadCache)
                ReadCacheCompleteUpdate(ref key, ref stackCtx.hei);

            // Iff we are doing EphemeralOnly locking, we have to "transfer" the X lock from a readonly record--either from the
            // main-log immutable region or readcache--to the new record. But we don't need to actually lock the new record because
            // we know this is the last step and we are going to unlock it immediately; it is protected until we remove the Tentative bit.
            // So all that is needed is to mark the source record as no longer in use.
            // If we are doing SessionControlled locking, then we will have a lock table entry separate from the recordInfo for both
            // manual and ephemeral locks; no transfer or marking is needed.
            if (stackCtx.recSrc.HasInMemorySrc)
                stackCtx.recSrc.MarkSourceRecordAfterSuccessfulCopy<Input, Output, Context, FasterSession>(fasterSession, ref srcRecordInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (UseReadCache)
                ReadCacheCompleteCopyToTail(ref key, ref stackCtx.hei);

            // TODO (see also comments in EphemeralSUnlock): Iff we are doing EphemeralOnly locking, we have to transfer the S locks from a
            // readonly record--either from the main-log immutable region or readcache--to the new record. However, there's a catch: other
            // sessions holding thhose read locks have to be able to release them. If we transfer while those locks are still held, then the
            // existing lock holders would have to go find the new location, and the new location could go below HeadAddress before the lock
            // holders get there. So, spin here waiting for share locks to drain. However, this has potential deadlock implications too, if
            // it prevents a flush. TODO need to finalize this design; current assumption is that we will disallow any below-HeadAddress operations
            // *and* CopyToTailFromReadOnly if EphemeralOnly.

            // If we are doing SessionControlled locking, then we will have a lock table entry separate from the recordInfo for both
            // manual and ephemeral locks; no transfer or marking is needed.
            if (stackCtx.recSrc.HasInMemorySrc)
            {
#if false // Part of the TODO
                // Unlock the current ephemeral lock here; we mark the source so we *know* we will have an invalid unlock on srcRecordInfo and would have to chase
                // through InternalLock to unlock it, so we save the time by not transferring our ephemeral lock; 'Tentative' still protects the new record.
                newRecordInfo.TransferReadLocksFromAndMarkSourceAtomic(ref srcRecordInfo, seal: stackCtx.recSrc.HasMainLogSrc, removeEphemeralLock: true);
#else
                // Spin wait to drain the reader locks; 'Tentative' still protects the new record.
                while (srcRecordInfo.IsLockedShared)
                    Thread.Yield();
                stackCtx.recSrc.MarkSourceRecordAfterSuccessfulCopy<Input, Output, Context, FasterSession>(fasterSession, ref srcRecordInfo);
#endif
            }
        }
    }
}

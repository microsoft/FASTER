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
        private bool TryLockTableEphemeralXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (!this.LockTable.IsEnabled || fasterSession.TryLockTableEphemeralXLock(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryEphemeralSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;

            if (this.LockTable.IsEnabled)
            {
                if (!fasterSession.TryLockTableEphemeralSLock(ref key, ref stackCtx))
                {
                    status = OperationStatus.RETRY_LATER;
                    return false;
                }
                return true;
            }

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
                stackCtx.recSrc.HasInMemoryLock = true;
            return stackCtx.recSrc.HasInMemoryLock;
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

                this.EphemeralOnlyLocker.UnlockShared(ref recordInfo);

                // (See comments in CompleteCopyToTail also) Because this is an S lock, then if the source record is in ReadOnly or ReadCache, it is possible
                // that the source record (specifically its locks) was transferred by another session that did CopyToTail or CopyToReadCache (it would not have
                // been a copy due to update, because that would've taken an X lock). If this happens then the record is Closed (Sealed or Invalid) after the
                // unlock and we have to go find it wherever it is now (still in memory) and unlock it there.
                //
                // If this happens in a tightly memory-constrained environment, the new location could drop below HeadAddress. Despite this, it cannot be
                // evicted while we hold the epoch, so use ClosedUntilAddress when searching.
                if (recordInfo.IsClosed)
                    FindAndUnlockTransferredRecord(ref key, stackCtx.hei.hash);

                stackCtx.recSrc.HasInMemoryLock = false;
            }
            else
            {
                Debug.Assert(this.LockTable.IsEnabled, "If we're here, one of the locking systems should be enabled");
                Debug.Assert(stackCtx.recSrc.HasLockTableLock, "Should have an InMemoryLock when we have an ephemeral lock with ManualLockTable enabled");
                fasterSession.LockTableEphemeralSUnlock(ref key, ref stackCtx);
                stackCtx.recSrc.HasLockTableLock = false;
            }
        }

        private void FindAndUnlockTransferredRecord(ref Key key, long hash)
        {
            // Loop because it may have transferred again in a highly threaded and memory-constrained scenario.
            while (true)
            { 
                OperationStackContext<Key, Value> stackCtx = new(hash);
                FindOrCreateTag(ref stackCtx.hei, hlog.BeginAddress);
                stackCtx.SetRecordSourceToHashEntry(hlog);
                var found = TryFindRecordInMemory(ref key, ref stackCtx, minOffset: hlog.ClosedUntilAddress);
                Debug.Assert(found, "We should always find the new record if the old one is Closed");
                if (!found)
                    break;

                stackCtx.recSrc.SetPhysicalAddress();
                var recordInfo = stackCtx.recSrc.GetSrcRecordInfo();
                recordInfo.UnlockShared();
                if (!recordInfo.IsClosed)
                    break;
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
                Debug.Assert(this.LockTable.IsEnabled, "If we're here, one of the locking systems should be enabled");
                Debug.Assert(stackCtx.recSrc.HasLockTableLock, "Should have an InMemoryLock when we have an ephemeral lock with ManualLockTable enabled");
                fasterSession.LockTableEphemeralXUnlock(ref key, ref stackCtx);
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
            if (!this.EphemeralOnlyLocker.IsEnabled)
                return;
            if (UseReadCache)
                ReadCacheCompleteUpdate(ref key, ref stackCtx.hei);

            // If we are doing EphemeralOnly locking, we have to "transfer" the X lock from a readonly record--either from the main-log immutable
            // region or readcache--to the new record. But we don't need to actually lock the new record because we know this is the last step and
            // we are going to unlock it immediately. So all that is needed is to mark the source record as no longer in use.
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
            if (!this.EphemeralOnlyLocker.IsEnabled)
                return;

            if (UseReadCache)
                ReadCacheCompleteCopyToTail(ref key, ref stackCtx.hei);

            // (See also comments in EphemeralSUnlock): If we are doing EphemeralOnly locking, we have to transfer the S locks from a
            // readonly record--either from the main-log immutable region or readcache--to the new record. However, other sessions holding
            // those read locks have to be able to release them; EphemeralSUnlock will handle that.
            // If we are doing SessionControlled locking, then we will have a lock table entry separate from the recordInfo for both
            // manual and ephemeral locks; no transfer or marking is needed.
            if (stackCtx.recSrc.HasInMemorySrc)
            {
                // Unlock the current ephemeral lock here; we mark the source so we *know* we will have an invalid unlock on srcRecordInfo and would have to chase
                // through InternalLock to unlock it, so we save the time by not transferring our ephemeral lock; 'Tentative' still protects the new record.
                newRecordInfo.TransferReadLocksFromAndMarkSourceAtomic(ref srcRecordInfo, seal: stackCtx.recSrc.HasMainLogSrc, removeEphemeralLock: true);
            }
        }
    }
}

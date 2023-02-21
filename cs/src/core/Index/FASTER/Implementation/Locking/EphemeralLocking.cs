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
        private bool TryEphemeralXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;

            if (this.LockTable.IsEnabled)
            {
                // Note: In MixedMode we ignore "Tentative" locks in the RecordInfo; we use LockTable locks so no intermediate state is visible.
                if (fasterSession.TryLockEphemeralExclusive(ref key, ref stackCtx))
                    return true;
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            // If neither locking mode is enabled, return true so the operation continues, but don't set any Has*Lock
            if (!this.RecordInfoLocker.IsEnabled || !stackCtx.recSrc.HasInMemorySrc)
                return true;

            // A failed lockOp means this is a Sealed record or we exhausted the spin count. Either must RETRY_LATER, as must a record that was Invalidated while we spun.
            if (!this.RecordInfoLocker.TryLockExclusive(ref srcRecordInfo))
            {
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            Debug.Assert(!srcRecordInfo.IsClosed, "RecordInfo XLock should have failed if the record is closed");
            return stackCtx.recSrc.HasRecordInfoLock = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryEphemeralSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                                                              ref RecordInfo srcRecordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // This routine applies to all locking modes, because for reads we have already gotten the logicalAddress.
            status = OperationStatus.SUCCESS;

            if (this.LockTable.IsEnabled)
            {
                // Note: In MixedMode we ignore "Tentative" locks in the RecordInfo; for Reads we can use the value from the readcache or fall through to IO.

                if (fasterSession.TryLockEphemeralShared(ref key, ref stackCtx))
                    return true;
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            // If neither locking mode is enabled, return true so the operation continues, but don't set any Has*Lock
            if (!this.RecordInfoLocker.IsEnabled || !stackCtx.recSrc.HasInMemorySrc)
                return true;

            // A failed lockOp means this is a Sealed record or we exhausted the spin count. Either must RETRY_LATER, as must a record that was Invalidated while we spun.
            if (!this.RecordInfoLocker.TryLockShared(ref srcRecordInfo))
            { 
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            // The record may have been closed by another share locker immediately after we obtained our share lock, or at
            // any time thereafter. That's OK; we're just reading. That's why we take a shared lock instead of exclusive.
            return stackCtx.recSrc.HasRecordInfoLock = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void EphemeralSUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, 
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasLock)
                return;

            // This unlocks the source (old) record; the new record may already be operated on by other threads, which is fine.
            if (this.RecordInfoLocker.IsEnabled)
            {
                // We precheck that the address was above HeadAddress before we get this far, so even if HeadAddress changes, the record can't be evicted
                // while we hold the epoch.
                Debug.Assert(stackCtx.recSrc.HasRecordInfoLock, "Should have a RecordInfo Lock when we have an ephemeral lock with RecordInfoLocker enabled");

                // (See comments in CompleteCopyToTail also) Because this is an S lock, then if the source record is in ReadOnly or ReadCache, it is possible
                // that the source record (specifically its locks) was transferred by another session that did CopyToTail or CopyToReadCache (it would not have
                // been a copy due to update, because that would've taken an X lock). If this happens then the record is Closed (Sealed or Invalid) after the
                // unlock and we have to go find it wherever it is now (still in memory) and unlock it there.
                if (!this.RecordInfoLocker.TryUnlockShared(ref srcRecordInfo)) 
                    FindAndUnlockTransferredRecord(ref key, stackCtx.hei.hash);
                stackCtx.recSrc.HasRecordInfoLock = false;
                return;
            }

            Debug.Assert(this.LockTable.IsEnabled, "If we're here, one of the locking systems should be enabled");
            Debug.Assert(stackCtx.recSrc.HasLockTableLock, "Should have a LockTable lock when we have an ephemeral lock with ManualLockTable enabled");
            fasterSession.UnlockEphemeralShared(ref key, ref stackCtx);
        }

        private void FindAndUnlockTransferredRecord(ref Key key, long hash)
        {
            // Loop because it may have transferred again in a highly threaded and memory-constrained scenario.
            for (; ; Thread.Yield())
            { 
                OperationStackContext<Key, Value> stackCtx = new(hash);
                var found = FindTag(ref stackCtx.hei);
                Debug.Assert(found, "We should always find the tag for the new record if the old one is Closed");
                stackCtx.SetRecordSourceToHashEntry(hlog);

                // If this happens in a tightly memory-constrained environment, the new location could drop below HeadAddress. Despite this, it cannot be
                // evicted while we hold the epoch, so use SafeHeadAddress when searching.
                found = TryFindRecordInMemory(ref key, ref stackCtx, minAddress: hlog.SafeHeadAddress);
                Debug.Assert(found, "We should always find the new record if the old one is Closed");
                if (!found)
                    break;

                stackCtx.recSrc.SetPhysicalAddress();
                ref var recordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                if (recordInfo.TryUnlockShared())
                    break;

                // Not doing an epoch refresh here because if the record was invalid, that means another was already CAS'd in.
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key,
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasLock)
                return;

            // Note that this unlocks the *old* record, not the one copied to.
            if (this.RecordInfoLocker.IsEnabled)
            {
                // We precheck that the address was above HeadAddress before we get this far, so even if HeadAddress changes, the record can't be evicted
                // while we hold the epoch.
                Debug.Assert(stackCtx.recSrc.HasRecordInfoLock, "Should have a RecordInfo Lock when we have an ephemeral lock with RecordInfoLocker enabled");
                this.RecordInfoLocker.UnlockExclusive(ref srcRecordInfo);
                stackCtx.recSrc.HasRecordInfoLock = false;
                return;
            }

            Debug.Assert(this.LockTable.IsEnabled, "If we're here, one of the locking systems should be enabled");
            Debug.Assert(stackCtx.recSrc.HasLockTableLock, "Should have a LockTable lock when we have an ephemeral lock with ManualLockTable enabled");
            fasterSession.UnlockEphemeralExclusive(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(!srcRecordInfo.IsLockedShared, "Should not have a shared lock at CompleteUpdate");

            if (stackCtx.recSrc.HasInMemorySrc)
            {
                // We are doing EphemeralOnly locking, so we have to "transfer" the X lock from a readonly record--either from the main-log immutable
                // region or readcache--to the new record. But we don't need to actually lock the new record because we know this is the last step and
                // we are going to unlock it immediately. So all that is needed is to mark the source record as no longer in use.
                // (Note: If we are doing MixedMode locking, then we will have a lock table entry separate from the recordInfo for both
                // manual and ephemeral locks; no transfer or marking is needed.)
                stackCtx.recSrc.MarkSourceRecordAfterSuccessfulCopy<Input, Output, Context, FasterSession>(fasterSession, ref srcRecordInfo);
                // Don't clear HasInMemorySrc; the XLock wasn't removed from the record or copied to the new record, and XUnlock does not return failure.
                return;
            }
            
            // We did not have a source lock, so it is possible that a readcache record was inserted and possibly locked.
            if (UseReadCache)
                ReadCacheCompleteInsertAtTail(ref key, ref stackCtx.hei, ref newRecordInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // (See also comments in EphemeralSUnlock): If we are doing EphemeralOnly locking, we have to transfer the S locks from a
            // readonly record--either from the main-log immutable region or readcache--to the new record. However, other sessions holding
            // those read locks have to be able to release them; EphemeralSUnlock will handle that.
            // (Note: If we are doing MixedMode locking, then we will have a lock table entry separate from the recordInfo for both
            // manual and ephemeral locks; no transfer or marking is needed.)
            if (stackCtx.recSrc.HasInMemorySrc)
            {
                // Unlock the current ephemeral lock here; because we mark the source, we *know* we will have an invalid unlock on srcRecordInfo
                // and would have to chase through FindAndUnlockTransferredRecord to unlock it, so we save the time by not transferring our ephemeral lock.
                newRecordInfo.CopyReadLocksFromAndMarkSourceAtomic(ref srcRecordInfo, seal: stackCtx.recSrc.HasMainLogSrc, removeEphemeralLock: stackCtx.recSrc.HasRecordInfoLock);
                stackCtx.recSrc.HasRecordInfoLock = false;
                return;
            }

            // We did not have a source lock, so it is possible that a readcache record was inserted and possibly locked.
            if (UseReadCache)
                ReadCacheCompleteInsertAtTail(ref key, ref stackCtx.hei, ref newRecordInfo);
        }
    }
}

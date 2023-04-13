// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransientXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                    out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (!this.LockTable.IsEnabled || fasterSession.TryLockTransientExclusive(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TransientXUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (stackCtx.recSrc.HasTransientLock)
                fasterSession.UnlockTransientExclusive(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTransientSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                    out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (!this.LockTable.IsEnabled || fasterSession.TryLockTransientShared(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransientSUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (stackCtx.recSrc.HasTransientLock)
                fasterSession.UnlockTransientShared(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref OperationStackContext<Key, Value> stackCtx, ref Key key, ref RecordInfo recordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            stackCtx.recSrc.ephemeralLockResult = EphemeralLockResult.None;
            stackCtx.recSrc.HasTransientLock = false;
            if (fasterSession.Store.DoTransientLocking)
            {
                stackCtx = new(comparer.GetHashCode64(ref key));
                fasterSession.Store.FindTag(ref stackCtx.hei);
                stackCtx.SetRecordSourceToHashEntry(this.hlog);
                while (!fasterSession.Store.TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out OperationStatus status))
                    fasterSession.Store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, FasterSession>(status, fasterSession);
                stackCtx.recSrc.HasTransientLock = true;
            }
            else if (fasterSession.Store.DoEphemeralLocking)
            {
                while (!recordInfo.TryLockShared())
                    fasterSession.Store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, FasterSession>(OperationStatus.RETRY_LATER, fasterSession);
                stackCtx.recSrc.ephemeralLockResult = EphemeralLockResult.Success;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<Key, Value> stackCtx, ref Key key, ref RecordInfo recordInfo)
        {
            stackCtx.recSrc.ephemeralLockResult = EphemeralLockResult.None;
            stackCtx.recSrc.HasTransientLock = false;
            if (this.DoTransientLocking)
            {
                stackCtx = new(comparer.GetHashCode64(ref key));
                this.FindTag(ref stackCtx.hei);
                stackCtx.SetRecordSourceToHashEntry(this.hlog);

                while (!this.LockTable.TryLockTransientShared(ref key, ref stackCtx.hei))
                    this.epoch.ProtectAndDrain();

                stackCtx.recSrc.HasTransientLock = true;
            }
            else if (this.DoEphemeralLocking)
            {
                while (!recordInfo.TryLockShared())
                    this.epoch.ProtectAndDrain();
                stackCtx.recSrc.ephemeralLockResult = EphemeralLockResult.Success;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref OperationStackContext<Key, Value> stackCtx, ref Key key, ref RecordInfo recordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (stackCtx.recSrc.HasTransientLock)
                fasterSession.Store.TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
            else if (stackCtx.recSrc.ephemeralLockResult == EphemeralLockResult.Success)
            { 
                recordInfo.UnlockShared();
                stackCtx.recSrc.ephemeralLockResult = EphemeralLockResult.None;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<Key, Value> stackCtx, ref Key key, ref RecordInfo recordInfo)
        {
            if (stackCtx.recSrc.HasTransientLock)
            {
                this.LockTable.UnlockShared(ref key, ref stackCtx.hei);
                stackCtx.recSrc.HasTransientLock = false;
            }
            else if (stackCtx.recSrc.ephemeralLockResult == EphemeralLockResult.Success)
            { 
                recordInfo.UnlockShared();
                stackCtx.recSrc.ephemeralLockResult = EphemeralLockResult.None;
            }
        }
    }
}

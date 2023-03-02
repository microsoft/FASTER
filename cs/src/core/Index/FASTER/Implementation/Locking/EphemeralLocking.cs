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
        private bool TryTransientXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                    out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;

            if (!this.LockTable.IsEnabled)
                return true;

            if (fasterSession.TryLockTransientExclusive(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransientSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                    out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;

            if (!this.LockTable.IsEnabled)
                return true;

            if (fasterSession.TryLockTransientShared(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void TransientSUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, 
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (stackCtx.recSrc.HasTransientLock)
                fasterSession.UnlockTransientShared(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TransientXUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (stackCtx.recSrc.HasTransientLock)
                fasterSession.UnlockTransientExclusive(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteUpdate(ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
        {
            stackCtx.recSrc.CloseSourceRecordAfterCopy(ref srcRecordInfo);

            // If we did not have a source lock, it is possible that a readcache record was inserted.
            if (UseReadCache && !stackCtx.recSrc.HasTransientLock)
                ReadCacheCheckTailAfterSplice(ref key, ref stackCtx.hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompleteCopyToTail(ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
        {
            stackCtx.recSrc.CloseSourceRecordAfterCopy(ref srcRecordInfo);

            // If we did not have a source lock, it is possible that a readcache record was inserted.
            if (UseReadCache && !stackCtx.recSrc.HasTransientLock)
                ReadCacheCheckTailAfterSplice(ref key, ref stackCtx.hei);
        }
    }
}

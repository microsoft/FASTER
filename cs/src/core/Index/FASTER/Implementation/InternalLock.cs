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
        /// <param name="lockState">Receives the lock state of the record being locked or queried</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalLock(ref Key key, LockOperation lockOp, out LockState lockState)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            Debug.Assert(this.ManualLockTable.IsEnabled, "ManualLockTable must be enabled for InternalLock");
            lockState = default;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            if (lockOp.LockOperationType == LockOperationType.IsLocked)
            { 
                lockState = this.ManualLockTable.GetLockState(ref key, ref stackCtx.hei);
                return OperationStatus.SUCCESS;
            }

            if (!this.ManualLockTable.TryLockManual(ref key, ref stackCtx.hei, lockOp.LockType))
                return OperationStatus.RETRY_LATER;

            return OperationStatus.SUCCESS;
        }
    }
}

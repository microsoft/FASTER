// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal static class LockUtility
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryLockOperation(this ref RecordInfo recordInfo, LockOperation lockOp)
        {
            if (lockOp.LockOperationType == LockOperationType.Lock)
                return recordInfo.TryLock(lockOp.LockType);

            if (lockOp.LockOperationType == LockOperationType.Unlock)
                recordInfo.Unlock(lockOp.LockType);
            else
                Debug.Fail($"Unexpected LockOperation {lockOp.LockOperationType}");
            return true;
        }
    }
}

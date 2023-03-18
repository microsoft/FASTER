// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    internal static class LockUtility
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsIntermediate(this ref RecordInfo recordInfo, out OperationStatus internalStatus, bool isReadingAtAddress = false)
        {
            // First a fast check so there is only one "if"
            internalStatus = OperationStatus.SUCCESS;
            if (!recordInfo.IsIntermediate)
                return false;

            // Separate routine to reduce impact on inlining decision.
            return HandleIntermediate(ref recordInfo, out internalStatus, isReadingAtAddress);
        }

        internal static bool HandleIntermediate(this ref RecordInfo recordInfo, out OperationStatus internalStatus, bool isReadingAtAddress = false)
        {
            SpinWaitWhileTentativeAndReturnValidity(ref recordInfo);

            // We don't want to jump out on Sealed/Invalid and restart if we are traversing the "read by address" chain
            if ((recordInfo.Sealed || recordInfo.Invalid) && !isReadingAtAddress)
            {
                Thread.Yield();

                // A record is only Sealed or Invalidated in the hash chain after the new record has been successfully inserted.
                internalStatus = OperationStatus.RETRY_LATER;
                return true;
            }
            internalStatus = OperationStatus.SUCCESS;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool SpinWaitWhileTentativeAndReturnValidity(ref RecordInfo recordInfo)
        {
            // This is called for Tentative records encountered in the hash chain, and no epoch-changing allocations should be done after they have been
            // added to the hash chain. Therefore, it is safe to spin. This routine centralizes this, in the event it needs to change, e.g. by limiting spin
            // count and returning bool.
            while (recordInfo.Tentative)
                Thread.Yield();
            return recordInfo.Valid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryLockOperation(this ref RecordInfo recordInfo, LockOperation lockOp)
        {
            if (lockOp.LockOperationType == LockOperationType.Lock)
                return recordInfo.TryLock(lockOp.LockType);

            if (lockOp.LockOperationType == LockOperationType.Unlock)
                recordInfo.TryUnlock(lockOp.LockType);
            else
                Debug.Fail($"Unexpected LockOperation {lockOp.LockOperationType}");
            return true;
        }
    }
}

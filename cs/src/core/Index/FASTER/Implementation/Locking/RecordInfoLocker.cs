// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct RecordInfoLocker
    {
        internal bool IsEnabled;

        internal RecordInfoLocker(bool enabled) => this.IsEnabled = enabled;

        [Conditional("DEBUG")]
        void AssertLockAllowed() => Debug.Assert(IsEnabled, "Attempt to do EphemeralOnly-locking lock when locking mode is not LockingMode.EphemeralOnly");

        [Conditional("DEBUG")]
        void AssertUnlockAllowed() => Debug.Assert(IsEnabled, "Attempt to do EphemeralOnly-locking unlock when locking mode is not LockingMode.EphemeralOnly");

        [Conditional("DEBUG")]
        void AssertQueryAllowed() => Debug.Assert(IsEnabled, "Attempt to do EphemeralOnly-locking query when locking mode is not LockingMode.EphemeralOnly");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockShared(ref RecordInfo recordInfo)
        {
            AssertLockAllowed();
            return recordInfo.TryLockShared();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockExclusive(ref RecordInfo recordInfo)
        {
            AssertLockAllowed();
            return recordInfo.TryLockExclusive();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryUnlockShared(ref RecordInfo recordInfo)
        {
            AssertUnlockAllowed();
            return recordInfo.TryUnlockShared();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockExclusive(ref RecordInfo recordInfo)
        {
            AssertUnlockAllowed();
            recordInfo.UnlockExclusive();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedShared(ref RecordInfo recordInfo)
        {
            AssertQueryAllowed();
            return recordInfo.IsLockedShared;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedExclusive(ref RecordInfo recordInfo)
        {
            AssertQueryAllowed();
            return recordInfo.IsLockedExclusive;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLocked(ref RecordInfo recordInfo)
        {
            AssertQueryAllowed();
            return recordInfo.IsLocked;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LockState GetLockState(ref RecordInfo recordInfo)
        {
            AssertQueryAllowed();
            return new()
            {
                NumLockedShared = recordInfo.NumLockedShared,
                IsLockedExclusive = recordInfo.IsLockedExclusive
            };
        }
    }
}

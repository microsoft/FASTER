// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct OverflowBucketLockTable<TKey> : IManualLockTable<TKey>
    {
        internal bool IsEnabled;

        internal OverflowBucketLockTable(bool enabled) => this.IsEnabled = enabled;

        [Conditional("DEBUG")]
        void AssertLockAllowed() => Debug.Assert(IsEnabled, "Attempt to do Manual-locking lock when locking mode is LockingMode.EphemeralOnly");

        [Conditional("DEBUG")]
        void AssertUnlockAllowed() => Debug.Assert(IsEnabled, "Attempt to do Manual-locking unlock when locking mode is LockingMode.EphemeralOnly");

        [Conditional("DEBUG")]
        void AssertQueryAllowed() => Debug.Assert(IsEnabled, "Attempt to do Manual-locking query when locking mode is LockingMode.EphemeralOnly");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockManual(ref TKey key, ref HashEntryInfo hei, LockType lockType)
        {
            AssertLockAllowed();
            return lockType switch
            {
                LockType.Shared => HashBucket.TryAcquireSharedLatch(hei.firstBucket),
                LockType.Exclusive => HashBucket.TryAcquireExclusiveLatch(hei.firstBucket),
                _ => throw new FasterException("Attempt to lock with unknown LockType")
            };
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockEphemeralShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertLockAllowed();
            return HashBucket.TryAcquireSharedLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockEphemeralExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertLockAllowed();
            return HashBucket.TryAcquireExclusiveLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockManual(ref TKey key, ref HashEntryInfo hei, LockType lockType)
        {
            AssertUnlockAllowed();
            if (lockType == LockType.Shared)
                HashBucket.ReleaseSharedLatch(hei.bucket);
            else
            {
                Debug.Assert(lockType == LockType.Exclusive, "Attempt to unlock with unknown LockType");
                HashBucket.ReleaseExclusiveLatch(hei.bucket);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockEphemeralShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertUnlockAllowed();
            HashBucket.ReleaseSharedLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockEphemeralExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertUnlockAllowed();
            HashBucket.ReleaseExclusiveLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return HashBucket.NumLatchedShared(hei.firstBucket) > 0;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return HashBucket.IsLatchedExclusive(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLocked(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return HashBucket.IsLatched(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LockState GetLockState(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return new()
            {
                NumLockedShared = HashBucket.NumLatchedShared(hei.firstBucket),
                IsLockedExclusive = HashBucket.IsLatchedExclusive(hei.firstBucket)
            };
        }

        /// <inheritdoc/>
        public void Dispose() { }
    }
}


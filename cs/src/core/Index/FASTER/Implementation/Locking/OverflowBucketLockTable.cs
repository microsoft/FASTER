// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.RuntimeHelpers;

namespace FASTER.core
{
    internal struct OverflowBucketLockTable<TKey, TValue> : ILockTable<TKey>
    {
        FasterKV<TKey, TValue> fht;

        internal long NumBuckets => IsEnabled ? fht.state[fht.resizeInfo.version].size_mask + 1 : 0;

        internal bool IsEnabled => this.fht is not null;

        internal OverflowBucketLockTable(FasterKV<TKey, TValue> f) => this.fht = f;

        [Conditional("DEBUG")]
        void AssertLockAllowed() => Debug.Assert(IsEnabled, $"Attempt to do Manual-locking lock when locking mode is not {LockingMode.Standard}");

        [Conditional("DEBUG")]
        void AssertUnlockAllowed() => Debug.Assert(IsEnabled, $"Attempt to do Manual-locking unlock when locking mode is not {LockingMode.Standard}");

        [Conditional("DEBUG")]
        void AssertQueryAllowed() => Debug.Assert(IsEnabled, $"Attempt to do Manual-locking query when locking mode is not {LockingMode.Standard}");

        internal long GetSize() => fht.state[fht.resizeInfo.version].size_mask;

        public bool NeedKeyLockCode => IsEnabled;

        static OverflowBucketLockTable()
        {
            Debug.Assert(LockType.Exclusive < LockType.Shared, "LockType.Exclusive must be < LockType.Shared, or LockCodeComparer must be changed accordingly");
        }

        /// <inheritdoc/>
        public long GetLockCode(ref TKey key, long hash) => IsEnabled ? hash : 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetBucketIndex(long keyCode, long size_mask)
            => keyCode & size_mask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetBucketIndex(long keyCode)
            => GetBucketIndex(keyCode, fht.state[fht.resizeInfo.version].size_mask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe HashBucket* GetBucket(long keyCode)
            => fht.state[fht.resizeInfo.version].tableAligned + GetBucketIndex(keyCode);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockManual(ref TKey key, ref HashEntryInfo hei, LockType lockType) 
            => TryLockManual(hei.firstBucket, lockType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockManual(long keyCode, LockType lockType) 
            => TryLockManual(GetBucket(keyCode), lockType);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool TryLockManual(HashBucket* bucket, LockType lockType)
        {
            AssertLockAllowed();
            return lockType switch
            {
                LockType.Shared => HashBucket.TryAcquireSharedLatch(bucket),
                LockType.Exclusive => HashBucket.TryAcquireExclusiveLatch(bucket),
                _ => throw new FasterException("Attempt to lock with unknown LockType")
            };
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryPromoteLockManual(long keyCode)
        {
            AssertLockAllowed();
            return HashBucket.TryPromoteLatch(GetBucket(keyCode));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockTransient(ref TKey key, ref HashEntryInfo hei, LockType lockType) 
            => lockType == LockType.Shared ? TryLockTransientShared(ref key, ref hei) : TryLockTransientExclusive(ref key, ref hei);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockTransientShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertLockAllowed();
            return HashBucket.TryAcquireSharedLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockTransientExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertLockAllowed();
            return HashBucket.TryAcquireExclusiveLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Unlock(ref TKey key, ref HashEntryInfo hei, LockType lockType)
        {
            AssertUnlockAllowed();
            if (lockType == LockType.Shared)
                UnlockShared(ref key, ref hei);
            else
            {
                Debug.Assert(lockType == LockType.Exclusive, "Attempt to unlock with unknown LockType");
                UnlockExclusive(ref key, ref hei);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Unlock(long keyCode, LockType lockType)
        {
            AssertUnlockAllowed();
            HashBucket* bucket = GetBucket(keyCode);
            if (lockType == LockType.Shared)
                HashBucket.ReleaseSharedLatch(bucket);
            else
            {
                Debug.Assert(lockType == LockType.Exclusive, "Attempt to unlock with unknown LockType");
                HashBucket.ReleaseExclusiveLatch(bucket);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertUnlockAllowed();
            HashBucket.ReleaseSharedLatch(ref hei);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertUnlockAllowed();
            HashBucket.ReleaseExclusiveLatch(ref hei);
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
                IsFound = true, // Always true for OverflowBucketLockTable
                NumLockedShared = HashBucket.NumLatchedShared(hei.firstBucket),
                IsLockedExclusive = HashBucket.IsLatchedExclusive(hei.firstBucket)
            };
        }

        private static int LockCodeComparer<TLockableKey>(TLockableKey key1, TLockableKey key2, long size_mask)
            where TLockableKey : ILockableKey
        {
            var idx1 = GetBucketIndex(key1.LockCode, size_mask);
            var idx2 = GetBucketIndex(key2.LockCode, size_mask);
            return (idx1 != idx2) ? idx1.CompareTo(idx2) : ((byte)key1.LockType).CompareTo((byte)key2.LockType);
        }

        /// <inheritdoc/>
        internal int CompareLockCodes<TLockableKey>(TLockableKey key1, TLockableKey key2) 
            where TLockableKey : ILockableKey 
            => LockCodeComparer(key1, key2, fht.state[fht.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal int CompareLockCodes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) 
            where TLockableKey : ILockableKey
            => LockCodeComparer(key1, key2, fht.state[fht.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal void SortLockCodes<TLockableKey>(TLockableKey[] keys) 
            where TLockableKey : ILockableKey 
            => Array.Sort(keys, new KeyComparer<TLockableKey>(fht.state[fht.resizeInfo.version].size_mask));

        /// <inheritdoc/>
        internal void SortLockCodes<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey 
            => Array.Sort(keys, start, count, new KeyComparer<TLockableKey>(fht.state[fht.resizeInfo.version].size_mask));

        /// <summary>
        /// Need this struct because the Comparison{T} form of Array.Sort is not available with start and length arguments.
        /// </summary>
        struct KeyComparer<TLockableKey> : IComparer<TLockableKey>
            where TLockableKey : ILockableKey
        {
            readonly long size_mask;

            internal KeyComparer(long s) => size_mask = s;

            public int Compare(TLockableKey key1, TLockableKey key2) => LockCodeComparer(key1, key2, size_mask);
        }

        /// <inheritdoc/>
        public void Dispose() { }
    }
}


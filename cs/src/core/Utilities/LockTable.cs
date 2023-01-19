// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{

#if false
namespace FASTER.core
{
    internal class LockTable<TKey> : IDisposable
    {
    #region IInMemKVUserFunctions implementation
        internal class LockTableFunctions : IInMemKVUserFunctions<TKey, IHeapContainer<TKey>, RecordInfo>, IDisposable
        {
            private readonly IFasterEqualityComparer<TKey> keyComparer;
            private readonly IVariableLengthStruct<TKey> keyLen;
            private readonly SectorAlignedBufferPool bufferPool;

            internal LockTableFunctions(IFasterEqualityComparer<TKey> keyComparer, IVariableLengthStruct<TKey> keyLen)
            {
                this.keyComparer = keyComparer;
                this.keyLen = keyLen;
                if (keyLen is not null)
                    this.bufferPool = new SectorAlignedBufferPool(1, 1);
            }

            public IHeapContainer<TKey> CreateHeapKey(ref TKey key)
                => bufferPool is null ? new StandardHeapContainer<TKey>(ref key) : new VarLenHeapContainer<TKey>(ref key, keyLen, bufferPool);

            public ref TKey GetHeapKeyRef(IHeapContainer<TKey> heapKey) => ref heapKey.Get();

            public bool Equals(ref TKey key, IHeapContainer<TKey> heapKey) => keyComparer.Equals(ref key, ref heapKey.Get());

            public long GetHashCode64(ref TKey key) => keyComparer.GetHashCode64(ref key);

            public bool IsActive(ref RecordInfo recordInfo) => recordInfo.IsLocked;

            public void Dispose(ref IHeapContainer<TKey> key, ref RecordInfo recordInfo)
            {
                key?.Dispose();
                key = default;
                recordInfo = default;
            }

            public void Dispose()
            {
                this.bufferPool?.Free();
            }
        }
    #endregion IInMemKVUserFunctions implementation

        internal readonly InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions> kv;
        internal readonly LockTableFunctions functions;

        internal LockTable(int numBuckets, IFasterEqualityComparer<TKey> keyComparer, IVariableLengthStruct<TKey> keyLen)
        {
            this.functions = new(keyComparer, keyLen);
            this.kv = new InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>(numBuckets, numBuckets >> 4, this.functions);
        }

        public bool IsActive => kv.IsActive;

    #region Internal methods for Test - TODO remove
        internal bool HasEntries(ref TKey key) => kv.HasEntries(ref key);
        internal bool HasEntries(long hash) => kv.HasEntries(hash);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGet(ref TKey key, out RecordInfo recordInfo) => TryGet(ref key, this.functions.GetHashCode64(ref key), out recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGet(ref TKey key, long hash, out RecordInfo recordInfo) => kv.TryGet(ref key, hash, out recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLocked(ref TKey key, long hash) => TryGet(ref key, hash, out var recordInfo) && recordInfo.IsLocked;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLockedShared(ref TKey key, long hash) => TryGet(ref key, hash, out var recordInfo) && recordInfo.IsLockedShared;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLockedExclusive(ref TKey key, long hash) => TryGet(ref key, hash, out var recordInfo) && recordInfo.IsLockedExclusive;
    #endregion Internal methods for Test
    }
#endif
}

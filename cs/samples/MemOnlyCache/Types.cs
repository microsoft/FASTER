// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Threading;

namespace MemOnlyCache
{
    public struct CacheKey : IFasterEqualityComparer<CacheKey>
    {
        public long key;

        public CacheKey(long first)
        {
            key = first;
        }

        public long GetHashCode64(ref CacheKey key) => Utility.GetHashCode(key.key);

        public bool Equals(ref CacheKey k1, ref CacheKey k2) => k1.key == k2.key;
    }

    public sealed class CacheValue
    {
        public byte[] value;

        public CacheValue(int size, byte firstByte)
        {
            value = new byte[size];
            value[0] = firstByte;
        }

        public int GetSize => value.Length + 48; // heap size for byte array incl. ~48 bytes ref/array overheads
    }

    /// <summary>
    /// Callback for FASTER operations
    /// </summary>
    public sealed class CacheFunctions : AdvancedSimpleFunctions<CacheKey, CacheValue>
    {
        readonly CacheSizeTracker sizeTracker;

        public CacheFunctions(CacheSizeTracker sizeTracker)
        {
            this.sizeTracker = sizeTracker;
        }

        public override bool ConcurrentWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst, ref RecordInfo recordInfo, long address)
        {
            var old = Interlocked.Exchange(ref dst, src);
            sizeTracker.AddSize(dst.GetSize - old.GetSize);
            return true;
        }

        public override void SingleWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst)
        {
            dst = src;
            sizeTracker.AddSize(src.GetSize);
        }

        public override void ConcurrentDeleter(ref CacheKey key, ref CacheValue value, ref RecordInfo recordInfo, long address)
        {
            sizeTracker.AddSize(-value.GetSize);
        }
    }
}

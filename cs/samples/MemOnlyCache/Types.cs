// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;

namespace MemOnlyCache
{
    public class CacheKey : IFasterEqualityComparer<CacheKey>
    {
        public long key;
        public byte[] extra;

        public CacheKey() { }

        public CacheKey(long key, int extraSize = 0)
        {
            this.key = key;
            extra = new byte[extraSize];
        }

        public long GetHashCode64(ref CacheKey key) => Utility.GetHashCode(key.key);

        public bool Equals(ref CacheKey k1, ref CacheKey k2) => k1.key == k2.key;

        public int GetSize => sizeof(long) + extra.Length + 48; // heap size incl. ~48 bytes ref/array overheads
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
    public sealed class CacheFunctions : SimpleFunctions<CacheKey, CacheValue>
    {
        readonly CacheSizeTracker sizeTracker;

        public CacheFunctions(CacheSizeTracker sizeTracker)
        {
            this.sizeTracker = sizeTracker;
        }

        public override bool ConcurrentWriter(ref CacheKey key, ref CacheValue input, ref CacheValue src, ref CacheValue dst, ref CacheValue output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo)
        {
            var old = Interlocked.Exchange(ref dst, src);
            sizeTracker.AddTrackedSize(dst.GetSize - old.GetSize);
            return true;
        }

        public override void PostSingleWriter(ref CacheKey key, ref CacheValue input, ref CacheValue src, ref CacheValue dst, ref CacheValue output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            dst = src;
            sizeTracker.AddTrackedSize(key.GetSize + src.GetSize);
        }

        public override bool ConcurrentDeleter(ref CacheKey key, ref CacheValue value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
        {
            sizeTracker.AddTrackedSize(-value.GetSize);

            // Record is marked invalid (failed to insert), dispose key as well
            if (recordInfo.Invalid)
                sizeTracker.AddTrackedSize(-key.GetSize);
            return true;
        }
    }
}

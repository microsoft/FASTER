// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;

namespace MemOnlyCache
{
    public class CacheSizeTracker : IObserver<IFasterScanIterator<CacheKey, CacheValue>>
    {
        readonly FasterKV<CacheKey, CacheValue> store;
        long storeSize;

        public long TotalSize => storeSize + store.OverflowBucketCount * 64;

        public CacheSizeTracker(FasterKV<CacheKey, CacheValue> store, int memorySizeBits)
        {
            this.store = store;
            storeSize = store.IndexSize * 64;
            storeSize += 1L << memorySizeBits;

            // Register subscriber to receive notifications of log evictions from memory
            store.Log.SubscribeEvictions(this);
        }

        public void AddSize(int size) => Interlocked.Add(ref storeSize, size);

        public void OnCompleted() { }

        public void OnError(Exception error) { }

        public void OnNext(IFasterScanIterator<CacheKey, CacheValue> iter)
        {
            int size = 0;
            while (iter.GetNext(out RecordInfo info, out _, out CacheValue value))
            {
                if (!info.Tombstone) // ignore deleted records being evicted
                    size += value.GetSize;
            }
            Interlocked.Add(ref storeSize, -size);
        }
    }
}

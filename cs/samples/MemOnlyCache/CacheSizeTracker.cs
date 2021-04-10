// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;

namespace MemOnlyCache
{
    /// <summary>
    /// Cache size tracker
    /// </summary>
    public class CacheSizeTracker : IObserver<IFasterScanIterator<CacheKey, CacheValue>>
    {
        readonly FasterKV<CacheKey, CacheValue> store;
        long storeSize;

        /// <summary>
        /// Target size request for FASTER
        /// </summary>
        public long TargetSizeBytes { get; private set; }

        /// <summary>
        /// Total size (bytes) used by FASTER including index and log
        /// </summary>
        public long TotalSizeBytes => storeSize + store.OverflowBucketCount * 64;

        /// <summary>
        /// Class to track and update cache size
        /// </summary>
        /// <param name="store">FASTER store instance</param>
        /// <param name="memorySizeBits">Memory size (bits) used by FASTER log settings</param>
        /// <param name="targetMemoryBytes">Target memory size of FASTER in bytes</param>
        public CacheSizeTracker(FasterKV<CacheKey, CacheValue> store, int memorySizeBits, long targetMemoryBytes = long.MaxValue)
        {
            this.store = store;
            this.TargetSizeBytes = targetMemoryBytes;

            storeSize = store.IndexSize * 64;
            storeSize += 1L << memorySizeBits;

            // Register subscriber to receive notifications of log evictions from memory
            store.Log.SubscribeEvictions(this);
        }

        /// <summary>
        /// Set target total memory size for FASTER
        /// </summary>
        /// <param name="newTargetSize">Target size</param>
        public void SetTargetSizeBytes(long newTargetSize)
        {
            if (newTargetSize < TargetSizeBytes)
            {
                TargetSizeBytes = newTargetSize;
                if (store.Log.ExtraLag < store.Log.BufferSize - 1) store.Log.ExtraLag++; // trigger eviction to start the memory reduction process
            }
            else
                TargetSizeBytes = newTargetSize;
        }

        public void AddTrackedSize(int size) => Interlocked.Add(ref storeSize, size);

        public void OnNext(IFasterScanIterator<CacheKey, CacheValue> iter)
        {
            int size = 0;
            while (iter.GetNext(out RecordInfo info, out CacheKey key, out CacheValue value))
            {
                size += key.GetSize;
                if (!info.Tombstone) // ignore deleted records being evicted
                    size += value.GetSize;
            }
            Interlocked.Add(ref storeSize, -size);

            if (TotalSizeBytes > TargetSizeBytes && store.Log.ExtraLag < store.Log.BufferSize - 1)
                store.Log.ExtraLag++;
            else if (TotalSizeBytes < TargetSizeBytes && store.Log.ExtraLag > 0)
                store.Log.ExtraLag--;
        }
        public void OnCompleted() { }
        public void OnError(Exception error) { }
    }
}

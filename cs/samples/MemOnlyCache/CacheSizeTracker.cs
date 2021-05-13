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
        long storeHeapSize;

        /// <summary>
        /// Target size request for FASTER
        /// </summary>
        public long TargetSizeBytes { get; private set; }

        /// <summary>
        /// Total size (bytes) used by FASTER including index and log
        /// </summary>
        public long TotalSizeBytes => storeHeapSize + store.IndexSize * 64 + store.Log.MemorySizeBytes + (store.ReadCache != null ? store.ReadCache.MemorySizeBytes : 0) + store.OverflowBucketCount * 64;

        /// <summary>
        /// Class to track and update cache size
        /// </summary>
        /// <param name="store">FASTER store instance</param>
        /// <param name="targetMemoryBytes">Initial target memory size of FASTER in bytes</param>
        public CacheSizeTracker(FasterKV<CacheKey, CacheValue> store, long targetMemoryBytes = long.MaxValue)
        {
            this.store = store;
            if (targetMemoryBytes < long.MaxValue)
                Console.WriteLine("**** Setting initial target memory: {0,11:N2}KB", targetMemoryBytes / 1024.0);
            this.TargetSizeBytes = targetMemoryBytes;

            Console.WriteLine("Index size: {0}", store.IndexSize * 64);
            Console.WriteLine("Total store size: {0}", TotalSizeBytes);

            // Register subscriber to receive notifications of log evictions from memory
            store.Log.SubscribeEvictions(this);

            // Include the separate read cache, if enabled
            if (store.ReadCache != null)
                store.ReadCache.SubscribeEvictions(this);
        }

        /// <summary>
        /// Set target total memory size (in bytes) for the FASTER store
        /// </summary>
        /// <param name="newTargetSize">Target size</param>
        public void SetTargetSizeBytes(long newTargetSize)
        {
            if (newTargetSize < TargetSizeBytes)
            {
                TargetSizeBytes = newTargetSize;
                store.Log.EmptyPageCount++; // trigger eviction to start the memory reduction process
            }
            else
                TargetSizeBytes = newTargetSize;
        }

        /// <summary>
        /// Add to the tracked size of FASTER. This is called by IFunctions as well as the subscriber to evictions (OnNext)
        /// </summary>
        /// <param name="size"></param>
        public void AddTrackedSize(int size) => Interlocked.Add(ref storeHeapSize, size);

        /// <summary>
        /// Subscriber to pages as they are getting evicted from main memory
        /// </summary>
        /// <param name="iter"></param>
        public void OnNext(IFasterScanIterator<CacheKey, CacheValue> iter)
        {
            int size = 0;
            while (iter.GetNext(out RecordInfo info, out CacheKey key, out CacheValue value))
            {
                size += key.GetSize;
                if (!info.Tombstone) // ignore deleted values being evicted (they are accounted for by ConcurrentDeleter)
                    size += value.GetSize;
            }
            AddTrackedSize(-size);

            // Adjust empty page count to drive towards desired memory utilization
            if (TotalSizeBytes > TargetSizeBytes && store.Log.AllocatableMemorySizeBytes >= store.Log.MemorySizeBytes)
                store.Log.EmptyPageCount++;
            else if (TotalSizeBytes < TargetSizeBytes && store.Log.AllocatableMemorySizeBytes <= store.Log.MemorySizeBytes)
                store.Log.EmptyPageCount--;
        }

        /// <summary>
        /// OnCompleted
        /// </summary>
        public void OnCompleted() { }

        /// <summary>
        /// OnError
        /// </summary>
        /// <param name="error"></param>
        public void OnError(Exception error) { }
    }
}

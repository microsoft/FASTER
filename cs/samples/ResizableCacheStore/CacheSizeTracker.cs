// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace ResizableCacheStore
{
    /// <summary>
    /// Cache size tracker
    /// </summary>
    public class CacheSizeTracker
    {
        readonly FasterKV<CacheKey, CacheValue> store;

        /// <summary>
        /// Total size (bytes) used by FASTER including index and log
        /// </summary>
        public long TotalSizeBytes => 
            IndexSizeBytes +
            mainLog.TotalSizeBytes + 
            (readCache != null ? readCache.TotalSizeBytes : 0);

        public long IndexSizeBytes =>
            store.IndexSize * 64 +
            store.OverflowBucketCount * 64;

        public long LogSizeBytes => mainLog.TotalSizeBytes;
        public long ReadCacheSizeBytes => readCache != null ? readCache.TotalSizeBytes : 0;

        readonly LogSizeTracker<CacheKey, CacheValue> mainLog;
        readonly LogSizeTracker<CacheKey, CacheValue> readCache;

        public void PrintStats()
        {
            Console.WriteLine("Sizes: [store]: {0,8:N2}KB  [index]: {1,8:N2}KB  [hylog]: {2,8:N2}KB  [rcach]: {3,8:N2}KB",
                TotalSizeBytes / 1024.0,
                IndexSizeBytes / 1024.0,
                LogSizeBytes / 1024.0,
                ReadCacheSizeBytes / 1024.0);
        }

        /// <summary>
        /// Class to track and update cache size
        /// </summary>
        /// <param name="store">FASTER store instance</param>
        /// <param name="targetMemoryBytes">Initial target memory size of FASTER in bytes</param>
        public CacheSizeTracker(FasterKV<CacheKey, CacheValue> store, long targetMemoryBytes = long.MaxValue)
        {
            this.store = store;
            this.mainLog = new LogSizeTracker<CacheKey, CacheValue>(store.Log, "mnlog");
            if (store.ReadCache != null)
                this.readCache = new LogSizeTracker<CacheKey, CacheValue>(store.ReadCache, "readc");

            if (targetMemoryBytes < long.MaxValue)
            {
                Console.WriteLine("**** Setting initial target memory: {0,11:N2}KB", targetMemoryBytes / 1024.0);
                SetTargetSizeBytes(targetMemoryBytes);
            }

            PrintStats();
        }

        /// <summary>
        /// Set target total memory size (in bytes) for the FASTER store
        /// </summary>
        /// <param name="newTargetSize">Target size</param>
        public void SetTargetSizeBytes(long newTargetSize)
        {
            // In this sample, we split the residual space equally between the log and the read cache
            long residual = newTargetSize - IndexSizeBytes;
            
            if (residual > 0)
            {
                if (readCache == null)
                    mainLog.SetTargetSizeBytes(residual);
                else
                {
                    mainLog.SetTargetSizeBytes(residual / 2);
                    readCache.SetTargetSizeBytes(residual / 2);
                }
            }
        }
            

        /// <summary>
        /// Add to the tracked size of FASTER. This is called by IFunctions as well as the subscriber to evictions (OnNext)
        /// </summary>
        /// <param name="size"></param>
        public void AddTrackedSize(int size, bool isReadCache = false)
        {
            if (isReadCache)
                readCache.AddTrackedSize(size);
            else
                mainLog.AddTrackedSize(size);
        }
    }
}

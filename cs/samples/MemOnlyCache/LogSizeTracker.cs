// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;

namespace MemOnlyCache
{
    public class LogSizeTracker<TCacheKey, TCacheValue> : IObserver<IFasterScanIterator<TCacheKey, TCacheValue>>
        where TCacheKey : ISizeTracker
        where TCacheValue : ISizeTracker
    {
        readonly string name;
        
        /// <summary>
        /// Number of records in the log
        /// </summary>
        public int NumRecords;
        
        /// <summary>
        /// Total size occupied by log, including heap
        /// </summary>
        public long TotalSizeBytes => Log.MemorySizeBytes + heapSize;

        /// <summary>
        /// Target size request for FASTER
        /// </summary>
        public long TargetSizeBytes { get; private set; }


        int heapSize;
        readonly LogAccessor<TCacheKey, TCacheValue> Log;
        
        public LogSizeTracker(LogAccessor<TCacheKey, TCacheValue> log, string name)
        {
            this.name = name;
            Log = log;
            
            // Register subscriber to receive notifications of log evictions from memory
            Log.SubscribeEvictions(this);
        }

        /// <summary>
        /// Add to the tracked size of FASTER. This is called by IFunctions as well as the subscriber to evictions (OnNext)
        /// </summary>
        /// <param name="size"></param>
        public void AddTrackedSize(int size) => Interlocked.Add(ref heapSize, size);

        /// <summary>
        /// Set target total memory size (in bytes) for the FASTER store
        /// </summary>
        /// <param name="newTargetSize">Target size</param>
        public void SetTargetSizeBytes(long newTargetSize)
        {
            TargetSizeBytes = newTargetSize;
            AdjustAllocation();
        }
        public void OnNext(IFasterScanIterator<TCacheKey, TCacheValue> iter)
        {
            int size = 0;
            while (iter.GetNext(out RecordInfo info, out TCacheKey key, out TCacheValue value))
            {
                size += key.GetSize;
                if (!info.Tombstone) // ignore deleted values being evicted (they are accounted for by ConcurrentDeleter)
                    size += value.GetSize;
            }
            AddTrackedSize(-size);

            AdjustAllocation();
        }

        public void OnCompleted() { }
        
        public void OnError(Exception error) { }

        void AdjustAllocation()
        {
            const long Delta = 1L << 20;
            if (TotalSizeBytes > TargetSizeBytes + Delta)
            {
                while (true)
                {
                    if (Log.AllocatedPageCount > Log.BufferSize - Log.EmptyPageCount + 1)
                    {
                        // Console.WriteLine($"{name}: {Log.EmptyPageCount} (wait++)");
                        return; // wait for allocation to stabilize
                    }
                    Log.EmptyPageCount++;
                    //Console.WriteLine($"{name}: {Log.EmptyPageCount} (++)");
                }
            }
            else if (TotalSizeBytes < TargetSizeBytes - Delta)
            {
                while (true)
                {
                    if (Log.AllocatedPageCount < Log.BufferSize - Log.EmptyPageCount - 1)
                    {
                        // Console.WriteLine($"{name}: {Log.EmptyPageCount} (wait--)");
                        return; // wait for allocation to stabilize
                    }
                    Log.EmptyPageCount--;
                    //Console.WriteLine($"{name}: {Log.EmptyPageCount} (--)");
                }
            }
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FASTER.common;
using FASTER.core;

namespace FASTER.libdpr
{
    internal class DarqEntryBucket
    {
        internal long bucketNum;
        internal long rangeEnd = 0;
        internal int numIncompleteEntries = 0;
        internal bool isSealed = false;

        public void Reset(long bucketNum)
        {
            this.bucketNum = bucketNum;
            rangeEnd = 0;
            numIncompleteEntries = 0;
            isSealed = false;
        }
    }


    public class DarqCompletionTracker
    {
        private SimpleObjectPool<DarqEntryBucket> bucketPool;
        private DarqEntryBucket tailBucket;
        private ConcurrentDictionary<long, DarqEntryBucket> outstandingBuckets;
        private ConcurrentQueue<DarqEntryBucket> bucketQueue;

        private long truncatedHead = 0;
        private int maxSectorRangeBits;
        private int truncationInProgress;

        public DarqCompletionTracker(int maxSectorRangeBits = 20)
        {
            bucketPool = new SimpleObjectPool<DarqEntryBucket>(() => new DarqEntryBucket());
            outstandingBuckets = new ConcurrentDictionary<long, DarqEntryBucket>();
            bucketQueue = new ConcurrentQueue<DarqEntryBucket>();
            this.maxSectorRangeBits = maxSectorRangeBits;
        }

        public long GetTruncateHead() => truncatedHead;

        // Will only be invoked single-threaded
        public void AddEntry(long start, long end)
        {
            while (tailBucket == null || start >> maxSectorRangeBits > tailBucket.bucketNum)
            {
                if (tailBucket != null)
                    tailBucket.isSealed = true;
                tailBucket = bucketPool.Checkout();
                tailBucket.Reset(start >> maxSectorRangeBits);
                outstandingBuckets.TryAdd(tailBucket.bucketNum, tailBucket);
                bucketQueue.Enqueue(tailBucket);
            }
            
            Debug.Assert(start >> maxSectorRangeBits >= tailBucket.bucketNum);
            tailBucket.rangeEnd = Math.Max(tailBucket.rangeEnd, end);
            Interlocked.Increment(ref tailBucket.numIncompleteEntries);
        }

        public bool RemoveEntry(long start)
        {
            var ret = outstandingBuckets.TryGetValue(start >> maxSectorRangeBits, out var bucket);
            if (!ret)
            {
                throw new FasterException("removing nonexistent entries from tracking");
            }
            
            if (Interlocked.Decrement(ref bucket.numIncompleteEntries) == 0 && bucket.isSealed)
                return TryUpdateTruncateHead();
            return false;
        }

        private bool TryUpdateTruncateHead()
        {
            // Ensure only one thread is truncating at a time
            if (Interlocked.CompareExchange(ref truncationInProgress, 1, 0) != 0) return false;

            var changed = false;
            while (bucketQueue.TryPeek(out var bucket))
            {
                if (!bucket.isSealed || bucket.numIncompleteEntries != 0) break;
                core.Utility.MonotonicUpdate(ref truncatedHead, bucket.rangeEnd, out _);
                bucketQueue.TryDequeue(out _);
                outstandingBuckets.TryRemove(bucket.bucketNum, out _);
                bucketPool.Return(bucket);
                changed = true;
            }
            truncationInProgress = 0;
            return changed;
        }
    }
}
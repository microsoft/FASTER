using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace FASTER.libdpr
{
    // Client-local information that keeps tracks of every outstanding (not yet completed) request batch. 
    internal class BatchInfo
    {
        internal const int MaxHeaderSize = 4096;
        internal readonly byte[] header;
        internal bool allocated;
        internal int batchId;

        internal BatchInfo(int batchId)
        {
            this.batchId = batchId;
            allocated = false;
            header = new byte[MaxHeaderSize];
        }
    }

    // Class that tracks outstanding batches per client session. For performance, this class statically allocates 
    // a number of frames that hold batch information as specified in the constructor. 
    internal class ClientBatchTracker : IEnumerable<BatchInfo>
    {
        public const int INVALID_BATCH_ID = -1;
        private readonly BatchInfo[] buffers;
        private readonly ConcurrentQueue<int> freeBuffers;

        internal ClientBatchTracker(int preallocateNumber = 8192)
        {
            buffers = new BatchInfo[preallocateNumber];
            freeBuffers = new ConcurrentQueue<int>();
            for (var i = 0; i < preallocateNumber; i++)
            {
                buffers[i] = new BatchInfo(i);
                freeBuffers.Enqueue(i);
            }
        }

        public IEnumerator<BatchInfo> GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        // Requests a new batch info object to write information into. Returns false if there are too many batches
        // being tracked and the tracker has no space left
        internal bool TryGetBatchInfo(out BatchInfo info)
        {
            info = default;
            if (freeBuffers.TryDequeue(out var id))
            {
                info = buffers[id];
                info.allocated = true;
                return true;
            }

            return false;
        }

        internal BatchInfo GetBatch(int id)
        {
            return buffers[id];
        }

        internal void FinishBatch(int id)
        {
            var info = GetBatch(id);
            // Signals invalid batch
            info.allocated = false;
            freeBuffers.Enqueue(info.batchId);
        }

        // Go through all outstanding batches and decide whether they are resolved, and update the given CommitPoint
        // object accordingly for a rollback to mark lost operations 
        internal void HandleRollback()
        {
            for (var i = 0; i < buffers.Length; i++)
            {
                if (!buffers[i].allocated) continue;
                FinishBatch(i);
            }
        }

        private class Enumerator : IEnumerator<BatchInfo>
        {
            private int i;
            private readonly ClientBatchTracker tracker;

            public Enumerator(ClientBatchTracker tracker)
            {
                this.tracker = tracker;
            }

            public bool MoveNext()
            {
                do
                {
                    i++;
                    if (i >= tracker.buffers.Length) return false;
                } while (!tracker.buffers[i].allocated);

                return true;
            }

            public void Reset()
            {
                i = 0;
            }

            public BatchInfo Current => tracker.buffers[i];

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }
    }
}
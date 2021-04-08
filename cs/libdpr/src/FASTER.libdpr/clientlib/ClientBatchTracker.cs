using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.core;

namespace FASTER.libdpr
{
    internal struct BatchInfo
    {
        private const int MaxHeaderSize = 4096;
        internal int batchId;
        internal bool allocated;
        internal Worker workerId;
        internal long startSeqNum, endSeqNum;
        internal readonly byte[] header;

        internal BatchInfo(int batchId)
        {
            this.batchId = batchId;
            allocated = false;
            workerId = default;
            startSeqNum = 0;
            endSeqNum = 0;
            header = new byte[MaxHeaderSize];
        }
    }

    internal class ClientBatchTracker : IEnumerable<BatchInfo>
    {
        private BatchInfo[] buffers;
        private ConcurrentQueue<int> freeBuffers;

        private class Enumerator : IEnumerator<BatchInfo>
        {
            private ClientBatchTracker tracker;
            private int i = 0;

            public Enumerator(ClientBatchTracker tracker)
            {
                this.tracker = tracker;
            }
            
            public bool MoveNext()
            {
                while (!tracker.buffers[i].allocated)
                {
                    i++;
                    if (i > tracker.buffers.Length) return false;
                }

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

        internal BatchInfo GetBatch(int id) => buffers[id];

        internal void FinishBatch(int id)
        {
            var info = GetBatch(id);
            // Signals invalid batch
            info.allocated = false;
            freeBuffers.Enqueue(info.batchId);
        }

        internal void HandleRollback(ref CommitPoint limit)
        {
            // TODO(Tianyu): Eventually need to account for discrepancies between checkpoint and local checkpoint
            // membership information
            for (var i = 0; i < buffers.Length; i++)
            {
                if (!buffers[i].allocated) continue;
                // TODO(Tianyu): Perhaps be more efficient here in fixing UntilSerialNo
                for (var j = buffers[i].startSeqNum; j < buffers[i].endSeqNum; j++)
                    limit.ExcludedSerialNos.Add(j);
                FinishBatch(i);
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
    }
}
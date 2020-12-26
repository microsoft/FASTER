using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.core;

namespace FASTER.libdpr
{
    public class BatchInfo
    {
        public int batchId;
        public bool allocated;
        public long workerId;
        public long worldLine;
        public long startSeqNum, endSeqNum;
        public List<WorkerVersion> deps = new List<WorkerVersion>();
    }

    public class ClientBatchTracker : IEnumerable<BatchInfo>
    {
        private BatchInfo[] buffers;
        private ConcurrentQueue<int> freeBuffers;

        internal class Enumerator : IEnumerator<BatchInfo>
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

        public ClientBatchTracker(int preallocateNumber = 512)
        {
            buffers = new BatchInfo[preallocateNumber];
            freeBuffers = new ConcurrentQueue<int>();
            for (var i = 0; i < preallocateNumber; i++)
            {
                buffers[i] = new BatchInfo {batchId = i};
                freeBuffers.Enqueue(i);
            }
        }

        public bool TryGetBatchInfo(out BatchInfo info)
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

        public BatchInfo GetBatch(int id) => buffers[id];

        public void FinishBatch(int id)
        {
            var info = GetBatch(id);
            // Signals invalid batch
            info.allocated = false;
            info.deps.Clear();
            freeBuffers.Enqueue(info.batchId);
        }

        public void HandleRollback(ref CommitPoint limit)
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
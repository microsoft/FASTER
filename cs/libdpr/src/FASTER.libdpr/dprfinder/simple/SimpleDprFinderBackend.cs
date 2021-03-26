using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using FASTER.core;

namespace FASTER.libdpr
{
    public class SimpleDprFinderBackend
    {
        internal class DprFinderState
        {
            // TODO(Tianyu): Relate this with other constants in the system
            internal const int MAX_SIZE = 1 << 20;
            internal ConcurrentDictionary<Worker, long> worldLines;
            // Updates to cut is issued single-threadedly
            // TODO(Tianyu): also, need to add workers to it as it is the source of truth for cluster membership
            internal Dictionary<Worker, long> cut;

            public DprFinderState()
            {
                worldLines = new ConcurrentDictionary<Worker, long>();
                cut = new Dictionary<Worker, long>();
            }

            public DprFinderState(byte[] buf)
            {
                var head = 0;
                head = ReadDictionaryFromBytes(buf, head, cut);
                ReadDictionaryFromBytes(buf, head, worldLines);
            }

            public DprFinderState(DprFinderState other)
            {
                worldLines = new ConcurrentDictionary<Worker, long>(other.worldLines);
                cut = new Dictionary<Worker, long>(other.worldLines);
            }

            internal bool Serialize(byte[] buf)
            {
                var head = 0;
                // Check that the buffer is large enough to hold the entries
                if (buf.Length < sizeof(long) * 2 * (worldLines.Count + cut.Count) + 2 * sizeof(int)) return false;
                head = SerializeDictionary(cut, buf, head);
                SerializeDictionary(worldLines, buf, head);
                return true;
            }

            private static int SerializeDictionary(IDictionary<Worker, long> dict, byte[] buf, int head)
            {
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(int)), dict.Count);
                head += sizeof(int);
                foreach (var (worker, val) in dict)
                {
                    BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), worker.guid);
                    head += sizeof(long);
                    BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val);
                    head += sizeof(long);
                }

                return head;
            }

            private static int ReadDictionaryFromBytes(byte[] buf, int head, IDictionary<Worker, long> result)
            {
                var size = BitConverter.ToInt32(buf, head);
                head += sizeof(int);
                for (var i = 0; i < size; i++)
                {
                    var workerId = BitConverter.ToInt64(buf, head);
                    head += sizeof(long);
                    var val = BitConverter.ToInt64(buf, head);
                    head += sizeof(long);
                    result.TryAdd(new Worker(workerId), val);
                }

                return head;
            }
        }

        // Ephemeral data structures
        private ConcurrentDictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph =
            new ConcurrentDictionary<WorkerVersion, List<WorkerVersion>>();
        private ConcurrentDictionary<Worker, long> versionTable = new ConcurrentDictionary<Worker, long>();
        private DprFinderState volatileState;
        private long maxVersion = 0;

        // Persistence
        private IDevice persistentStorage;
        private DprFinderState persistentState;

        // Reused data structure for graph and traversal
        private SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();
        private Queue<WorkerVersion> frontier = new Queue<WorkerVersion>(), uncommittedWvs = new Queue<WorkerVersion>();

        // For serialization
        private byte[] serializationBuffer = new byte[DprFinderState.MAX_SIZE];

        public SimpleDprFinderBackend(IDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
        }

        private bool TryCommitWorkerVersion(WorkerVersion wv)
        {
            if (wv.Version <= volatileState.cut.GetValueOrDefault(wv.Worker, 0))
            {
                // already committed. Remove but do not signal changes to the cut
                if (precedenceGraph.TryRemove(wv, out var list))
                    objectPool.Return(list);
                return false;
            }

            visited.Clear();
            frontier.Enqueue(wv);
            while (frontier.Count != 0)
            {
                var node = frontier.Dequeue();
                if (visited.Contains(node)) continue;

                visited.Add(node);
                if (volatileState.cut.GetValueOrDefault(wv.Worker, 0) > wv.Version) continue;
                if (!precedenceGraph.TryGetValue(wv, out var val)) return false;

                foreach (var dep in val)
                    frontier.Enqueue(dep);
            }

            foreach (var committed in visited)
            {
                var success = volatileState.cut.TryGetValue(committed.Worker, out var version);
                // Must be a known worker
                Debug.Assert(success);
                if (version < committed.Version)
                    volatileState.cut[committed.Worker] = committed.Version;
                
                if (precedenceGraph.TryRemove(committed, out var list))
                    objectPool.Return(list);
            }

            return true;
        }

        public void TryFindDprCut()
        {
            // TODO(Tianyu): Do a min pass
            
            var hasUpdates = false;
            // No guarantees if recovery is in progress
            if (volatileState.worldLines.Any(pair => pair.Value != persistentState.worldLines[Worker.CLUSTER_MANAGER]))
                return;

            for (var i = 0; i < uncommittedWvs.Count; i++)
            {
                var wv = uncommittedWvs.Dequeue();
                if (TryCommitWorkerVersion(wv))
                    hasUpdates = true;
                else
                    uncommittedWvs.Enqueue(wv);
            }
            
            // TODO(Tianyu): write to disk if there are updates
        }

        public void NewCheckpoint(WorkerVersion wv, IEnumerable<WorkerVersion> deps)
        {
            maxVersion = Math.Max(wv.Version, maxVersion);
            var list = objectPool.Checkout();
            list.Clear();
            list.AddRange(deps);
            precedenceGraph.TryAdd(wv, list);
        }

        public void ReportRecovery(WorkerVersion wv, long worldLine)
        {
                volatileState.worldLines[wv.Worker] = wv.Version;
                if (wv.Worker.Equals(Worker.CLUSTER_MANAGER)) return;
                // Remove any rolled back version to avoid accidentally including them in a cut.
                for (var v = wv.Version + 1; v <= maxVersion; v++)
                {
                    // TODO(Tianyu): Should be thread-safe w.r.t concurrent traversal of graph. But verify.
                    if (precedenceGraph.TryRemove(new WorkerVersion(wv.Worker, v), out var list))
                        objectPool.Return(list);
                }
        }

        public void ServeSync(Socket clientConn)
        {
            // TODO(Tianyu): Send back only persistent state
        }
    }
}
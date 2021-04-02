using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    public class SimpleDprFinderBackend : IDisposable
    {
        public class State
        {
            internal Dictionary<Worker, long> worldLines;

            // Updates to cut is issued single-threadedly
            // TODO(Tianyu): also, need to add workers to it as it is the source of truth for cluster membership
            internal Dictionary<Worker, long> cut;

            // Dirty bit to signal that the state has been changed
            internal bool hasUpdates;

            public State()
            {
                worldLines = new Dictionary<Worker, long>();
                cut = new Dictionary<Worker, long>();
            }

            public State(byte[] buf)
            {
                var head = 0;
                head = ReadDictionaryFromBytes(buf, head, cut);
                ReadDictionaryFromBytes(buf, head, worldLines);
            }

            public State(State other)
            {
                worldLines = new Dictionary<Worker, long>(other.worldLines);
                cut = new Dictionary<Worker, long>(other.worldLines);
            }

            internal bool Serialize(byte[] buf, out int size)
            {
                var head = 0;
                // Check that the buffer is large enough to hold the entries
                size = sizeof(long) * 2 * (worldLines.Count + cut.Count) + 2 * sizeof(int);
                if (buf.Length <size) return false;
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

        /* Ephemeral data structures */
        // Precedence graph stores dependency information. All nodes present in the precedence graph's keyset are
        // persistent, but some edges may point to non-existent nodes. Nodes are non-existent either when they 
        // are committed (as determined by checking the current cut), or because they are not yet persistent.
        //
        // This graph is updated and read concurrently because races are benign here --- worker-versions are only
        // added or deleted and never updated; in the face of concurrent writes, readers miss information and arrive
        // at overly conservative answers that are still correct. 
        private ConcurrentDictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph =
            new ConcurrentDictionary<WorkerVersion, List<WorkerVersion>>();

        // Tracks a mapping from worker -> latest committed version, used for the approximate algorithm. This is 
        // cheaper to access than scanning through the entire graph if maintained separately. Ok to be out-of-sync
        // slightly with the graph as the graph is the source of truth and this is used for speed-up / DprFinder
        // fault recovery when some deps are lost. 
        private ConcurrentDictionary<Worker, long> versionTable = new ConcurrentDictionary<Worker, long>();

        // Volatile information, but immediately visible outside to enable version fast-forwarding. This is ok because
        // fast-forwarding has no correctness implications, but helps the cluster stay in sync with their checkpoint
        // schedule.
        private long maxVersion = 0;

        // State that is being updated on the fly (single-threadedly). Not visible to the outside world until 
        // persistent. A thread periodically reads a copy of the volatile state to flush to persistent storage and 
        // cache the snapshot to make available to workers.
        private State volatileState;

        /* Cluster Recovery */
        // Recovery reports are processed on the same thread as graph traversal to avoid concurrency and ensure
        // correctness. As soon as the cluster acknowledges a failure and enters recovery mode, it must stop giving 
        // guarantees. Single-threaded processing makes this easier.
        private ConcurrentQueue<(WorkerVersion, long, Action)> recoveryReports = new ConcurrentQueue<(WorkerVersion, long, Action)>();
        // callbacks to invoke on the next persist completion
        private List<Action> callbacks = new List<Action>();

        /* Persistence */
        private IDevice persistentStorage;
        private State persistentState;

        /* Reused data structure for graph and traversal */
        private SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();
        private Queue<WorkerVersion> frontier = new Queue<WorkerVersion>(), outstandingWvs = new Queue<WorkerVersion>();

        /* Serialization */
        // Start out at some arbitrary size --- the code resizes this buffer to fit
        private byte[] serializationBuffer = new byte[1 << 20];

        public SimpleDprFinderBackend(IDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
        }

        // Try to commit wvs starting from the given wv. Commits and updates the volatile state the entire dependency
        // set of wv if all of them are persistent. Committed versions are removed from the precedence graph.
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

                if (!precedenceGraph.TryGetValue(node, out var val)) return false;

                visited.Add(node);
                if (volatileState.cut.GetValueOrDefault(node.Worker, 0) >= node.Version) continue;
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
            lock (volatileState)
            {
                // Apply any recovery-related updates
                while (recoveryReports.TryDequeue(out var entry))
                {
                    volatileState.worldLines[entry.Item1.Worker] = entry.Item2;
                    // Remove any rolled back version to avoid accidentally including them in a cut.
                    for (var v = entry.Item1.Version + 1; v <= maxVersion; v++)
                    {
                        if (precedenceGraph.TryRemove(new WorkerVersion(entry.Item1.Worker, v), out var list))
                            objectPool.Return(list);
                    }
                    callbacks.Add(entry.Item3);
                }

                // No guarantees if recovery is in progress
                if (volatileState.worldLines.Any(pair =>
                    pair.Value != volatileState.worldLines[Worker.CLUSTER_MANAGER]))
                    return;
            }

            lock (volatileState)
            {
                // Use min to quickly prune outdated vertices
                var minVersion = versionTable.Min(pair => pair.Value);
                // Update entries in the cut to be at least the min. No need to prune the graph as later traversals will take
                // care of that
                foreach (var (worker, version) in volatileState.cut)
                {
                    if (version > minVersion) continue;
                    volatileState.hasUpdates = true;
                    volatileState.cut[worker] = minVersion;
                }
            }

            // Go through the unprocessed wvs and traverse the graph
            for (var i = 0; i < outstandingWvs.Count; i++)
            {
                var wv = outstandingWvs.Dequeue();
                lock (volatileState)
                {
                    if (TryCommitWorkerVersion(wv))
                        volatileState.hasUpdates = true;
                    else
                        outstandingWvs.Enqueue(wv);
                }
            }
        }

        /// <summary>
        /// Writes (a consistent snapshot of) the volatile state to persistent storage so it can be made visible to
        /// workers. Writes complete synchronously on the calling thread.
        /// </summary>
        public void PersistState()
        {
            var completed = new ManualResetEventSlim();
            List<Action> acks;
            State copy;
            lock (volatileState)
            {
                if (!volatileState.hasUpdates) return;
                // Deep copy the state to use for persistence and later as cached state for external access
                copy = new State(volatileState);
                volatileState.hasUpdates = false;
                acks = new List<Action>(callbacks);
                callbacks.Clear();
            }

            int size;
            while (!copy.Serialize(serializationBuffer, out size))
                // Grow buffer until state fits
                serializationBuffer = new byte[Math.Max(size, serializationBuffer.Length * 2)];

            Debug.Assert(size <= persistentStorage.SegmentSize);
            unsafe
            {
                // TODO(Tianyu): Not atomic --- need checksum
                fixed (byte* b = &serializationBuffer[0])
                    persistentStorage.WriteAsync((IntPtr) b, 0, (ulong) 0, (uint) size,
                        (e, n, o) =>
                        {
                            // TODO(Tianyu): error handling?
                            completed.Set();
                        },
                        null);
            }

            completed.Wait();
            Interlocked.Exchange(ref persistentState, copy);
            foreach (var callback in acks)
                callback();
        }

        /// <summary>
        /// Report a new checkpoint to the backend with the given dependencies
        /// </summary>
        /// <param name="wv">worker-version of the checkpoint</param>
        /// <param name="deps">dependencies of the checkpoint</param>
        public void NewCheckpoint(WorkerVersion wv, IEnumerable<WorkerVersion> deps)
        {
            maxVersion = Math.Max(wv.Version, maxVersion);
            var list = objectPool.Checkout();
            list.Clear();
            list.AddRange(deps);
            precedenceGraph.TryAdd(wv, list);
        }

        /// <summary>
        /// Report a new recovery of a worker version. If the given worker is the special value of CLUSTER_MANAGER,
        /// this is treated as an external signal to start a recovery process. 
        /// </summary>
        /// <param name="wv">worker version that is recovered, or (CLUSTER_MANAGER, 0) if triggering recovery for the cluster</param>
        /// <param name="worldLine"> the new worldline the worker is on</param>
        public void ReportRecovery(WorkerVersion wv, long worldLine, Action callback)
        {
            recoveryReports.Enqueue(ValueTuple.Create(wv, worldLine, callback));
        }

        public State GetPersistentState() => persistentState;

        public void Dispose()
        {
            persistentStorage?.Dispose();
        }
    }
}
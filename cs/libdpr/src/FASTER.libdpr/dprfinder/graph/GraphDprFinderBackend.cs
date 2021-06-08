using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    public class GraphDprFinderBackend : IDisposable
    {
        /// <summary>
        ///  Encapsulates the persistent state of GraphDprFinderBackend
        /// </summary>
        public class State
        {
            // Accesses to these are single-threaded
            private Dictionary<Worker, long> worldLines;
            private Dictionary<Worker, long> cut;

            // Dirty bit to signal that the state has been changed
            internal bool hasUpdates;

            /// <summary>
            /// Constructs a new GraphDprFinderBackend.State object that's empty
            /// </summary>
            public State()
            {
                worldLines = new Dictionary<Worker, long>();
                cut = new Dictionary<Worker, long>();
            }

            /// <summary>
            /// Constructs a new GraphDprFinderBackend.State from serialized bytes on the given buffer
            /// </summary>
            /// <param name="buf">buffer holding serialized bytes</param>
            /// <param name="offset">offset within buffer to start reading from</param>
            public State(byte[] buf, int offset)
            {
                var head = offset;
                worldLines = new Dictionary<Worker, long>();
                cut = new Dictionary<Worker, long>();
                head = ReadDictionaryFromBytes(buf, head, cut);
                ReadDictionaryFromBytes(buf, head, worldLines);
            }

            /// <summary>
            /// Constructs a new GraphDprFinderBackend.State as the copy of another State object.
            /// </summary>
            /// <param name="other">State object to copy from</param>
            public State(State other)
            {
                worldLines = new Dictionary<Worker, long>(other.worldLines);
                cut = new Dictionary<Worker, long>(other.cut);
            }

            /// <summary></summary>
            /// <returns>The DPR cut</returns>
            public Dictionary<Worker, long> GetCurrentCut()
            {
                return cut;
            }

            /// <summary></summary>
            /// <returns>A mapping from workers to their current world-line</returns>
            public Dictionary<Worker, long> GetCurrentWorldLines()
            {
                return worldLines;
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

        // State that is being updated on the fly (single-threaded). Not visible to the outside world until 
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
        private PingPongDevice persistentStorage;
        // Only keep the serialized persistent state for shipping to clients, as the DprFinder never needs to interpret
        // this information.
        private byte[] serializedPersistentState;
        private int serializedPersistentStateSize;

        /* Reused data structure for graph and traversal */
        private SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();
        private Queue<WorkerVersion> frontier = new Queue<WorkerVersion>();
        private ConcurrentQueue<WorkerVersion> outstandingWvs = new ConcurrentQueue<WorkerVersion>();

        /* Serialization */
        // Start out at some arbitrary size --- the code resizes this buffer to fit
        private byte[] serializationBuffer = new byte[1 << 20];

        /// <summary>
        /// Creates a new GraphDprFinderBackend that persists to the given backend
        /// </summary>
        /// <param name="persistentStorage">backend persistent storage device</param>
        public GraphDprFinderBackend(PingPongDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
            if (persistentStorage.ReadLatestCompleteWrite(out serializedPersistentState))
            {
                serializedPersistentStateSize = serializedPersistentState.Length;
                volatileState = new State(serializedPersistentState, 0);
                maxVersion = volatileState.GetCurrentCut().Max(e => e.Value);
            }
            else
            {
                volatileState = new State();
                serializedPersistentState = new byte[0];
                serializedPersistentStateSize = 0;
            }

            foreach (var (worker, version) in volatileState.GetCurrentCut())
                versionTable[worker] = version;
        }

        // Try to commit wvs starting from the given wv. Commits and updates the volatile state the entire dependency
        // set of wv if all of them are persistent. Committed versions are removed from the precedence graph.
        private bool TryCommitWorkerVersion(WorkerVersion wv)
        {
            if (wv.Version <= volatileState.GetCurrentCut().GetValueOrDefault(wv.Worker, 0))
            {
                // already committed. Remove but do not signal changes to the cut
                if (precedenceGraph.TryRemove(wv, out var list))
                    objectPool.Return(list);
                return false;
            }

            visited.Clear();
            frontier.Clear();
            frontier.Enqueue(wv);
            while (frontier.Count != 0)
            {
                var node = frontier.Dequeue();
                if (visited.Contains(node)) continue;
                if (volatileState.GetCurrentCut().GetValueOrDefault(node.Worker, 0) >= node.Version) continue;
                if (!precedenceGraph.TryGetValue(node, out var val)) return false;

                visited.Add(node);
                foreach (var dep in val)
                    frontier.Enqueue(dep);
            }

            foreach (var committed in visited)
            {
                var version = volatileState.GetCurrentCut().GetValueOrDefault(committed.Worker, 0);

                if (version < committed.Version)
                    volatileState.GetCurrentCut()[committed.Worker] = committed.Version;

                if (precedenceGraph.TryRemove(committed, out var list))
                    objectPool.Return(list);
            }

            return true;
        }


        private void ApplyRollback(long worldLine, Worker worker, long survivingVersion)
        {
            volatileState.GetCurrentWorldLines()[worker] = worldLine;
            // Remove any rolled back version to avoid accidentally including them in a cut.
            for (var v = survivingVersion + 1; v <= maxVersion; v++)
            {
                if (precedenceGraph.TryRemove(new WorkerVersion(worker, v), out var list))
                    objectPool.Return(list);
            }
        }
        
        public void TryFindDprCut()
        {
            if (versionTable.IsEmpty)
                return;
            
            lock (volatileState)
            {
                // Apply any recovery-related updates
                while (recoveryReports.TryDequeue(out var entry))
                {
                    ApplyRollback(entry.Item2, entry.Item1.Worker, entry.Item1.Version);
                    if (entry.Item3 != null)
                        callbacks.Add(entry.Item3);
                }
            
                // Check if all workers are on the same worldline. If not, there's an in-progress recovery. No
                // guarantees if recovery is in progress
                long worldLine = -1;
                foreach (var entry in volatileState.GetCurrentWorldLines())
                {
                    if (worldLine == -1)
                        worldLine = entry.Value;
                    if (worldLine != entry.Value) return;

                }
            }
            
            lock (volatileState)
            {
                // Use min to quickly prune outdated vertices
                var minVersion = versionTable.Min(pair => pair.Value);
                // Update entries in the cut to be at least the min. No need to prune the graph as later traversals will take
                // care of that
                foreach (var (worker, version) in volatileState.GetCurrentCut().ToList())
                {
                    if (version > minVersion) continue;
                    volatileState.hasUpdates = true;
                    volatileState.GetCurrentCut()[worker] = minVersion;
                }
            }

            // Go through the unprocessed wvs and traverse the graph, but give up after a while
            for (var i = 0; i < 50; i++)
            {
                if (!outstandingWvs.TryDequeue(out var wv)) return;
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

            persistentStorage.WriteReliably(serializationBuffer, 0, size);
            
            // Atomically updates the current stashed copy of persistent state and its size for client consumption.
            lock (serializedPersistentState)
            {
                Interlocked.Exchange(ref serializedPersistentState, serializationBuffer);
                serializedPersistentStateSize = size;
            }
            
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
            var list = objectPool.Checkout();
            list.Clear();
            var prev = versionTable[wv.Worker];
            list.Add(new WorkerVersion(wv.Worker, prev));
            Debug.Assert(deps.Select(d => d.Worker).All(versionTable.ContainsKey));
            list.AddRange(deps);
            maxVersion = Math.Max(wv.Version, maxVersion);
            precedenceGraph.TryAdd(wv, list);
            outstandingWvs.Enqueue(wv);
            versionTable.TryUpdate(wv.Worker, wv.Version, prev);
        }

        // TODO(Tianyu): Need to rewrite recovery trigger logic
        /// <summary>
        /// Report a new recovery of a worker version. If the given worker is the special value of CLUSTER_MANAGER,
        /// this is treated as an external signal to start a recovery process. 
        /// </summary>
        /// <param name="wv">worker version that is recovered, or (CLUSTER_MANAGER, 0) if triggering recovery for the cluster</param>
        /// <param name="worldLine"> the new worldline the worker is on</param>
        /// <param name="callback"> the function to trigger when the recovery update is persistent on disk. If null or not supplied, the function blocks until persistence.</param>
        public void ReportRecovery(WorkerVersion wv, long worldLine, Action callback)
        {
            recoveryReports.Enqueue(ValueTuple.Create(wv, worldLine, callback));
        }


        /// <summary>
        /// Adds a new worker to the system to be tracked. 
        /// </summary>
        /// <param name="worker">the worker to add</param>
        /// <param name="callback">callback to invoke when the add is persistent</param>
        /// <returns> (world-line, version) that the new worker should start at </returns>>
        public (long, long) AddWorker(Worker worker, Action callback)
        {
            var minVer = versionTable.IsEmpty ? 0 : versionTable.Select(e => e.Value).Min();
            versionTable.TryAdd(worker, minVer);
            lock (volatileState)
            {
                var currentWorldLines = volatileState.GetCurrentWorldLines();
                var latestWorldLine = currentWorldLines.Count == 0 ? 0 : currentWorldLines.Select(e => e.Value).Max();
                // First time we have seen this worker --- start them at the global min version and current cluster
                // world-line and add to tracking
                if (volatileState.GetCurrentCut().TryAdd(worker, 0))
                {
                    currentWorldLines.TryAdd(worker, latestWorldLine);
                    volatileState.hasUpdates = true;
                    if (callback != null)
                        callbacks.Add(callback);
                    return ValueTuple.Create(latestWorldLine, minVer);
                }
                // Otherwise, this worker thinks it's booting up for a second time, which means there was a restart.
                // We count this as a failure. Advance the cluster world-line
                var newWorldLine = currentWorldLines[worker] = latestWorldLine + 1;
                // Cannot assume anything later than the persisted cut survived the crash
                var survivingVersion = volatileState.GetCurrentCut()[worker];
                ApplyRollback(newWorldLine, worker, survivingVersion);
                return ValueTuple.Create(newWorldLine, survivingVersion);
                // TODO(Tianyu): Finish by adding appropriate calls to add worker on DprFinder bootup, and proper handling of return value.
                // Also, just convey this back to the client in Redis response message
            }
        }

        /// <summary>
        /// Remove a worker from the system for tracking. It is up to the caller to ensure that removal is safe (i.e.,
        /// all dependencies on the removed worker are resolved)
        /// </summary>
        /// <param name="worker">the worker to remove</param>
        /// <param name="callback">callback to invoke when the removal is persistent</param>
        public void DeleteWorker(Worker worker, Action callback)
        {
            lock (volatileState)
            {
                volatileState.GetCurrentCut().Remove(worker);
                volatileState.GetCurrentWorldLines().Remove(worker);
                volatileState.hasUpdates = true;
                versionTable.TryRemove(worker, out _);
                if (callback != null)
                    callbacks.Add(callback);
            }
        }

        public (byte[], int) GetPersistentState()
        {
            lock (serializedPersistentState)
            {
                return ValueTuple.Create(serializedPersistentState, serializedPersistentStateSize);
            }
        }

        public long MaxVersion() => maxVersion;

        public void Dispose()
        {
            persistentStorage?.Dispose();
        }
    }
}
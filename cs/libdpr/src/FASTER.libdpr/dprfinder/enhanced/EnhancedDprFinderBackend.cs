using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace FASTER.libdpr.enhanced
{
    public class PersistentState
    {
        public long currentWorldLine;
        public Dictionary<Worker, long> worldLineDivergencePoint;

        public PersistentState()
        {
            currentWorldLine = 1;
            worldLineDivergencePoint = new Dictionary<Worker, long>();
        }
    }

    public class EnhancedDprFinderBackend
    {
        private ConcurrentDictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph =
            new ConcurrentDictionary<WorkerVersion, List<WorkerVersion>>();

        private long maxVersion = 0;
        private Dictionary<Worker, long> currentCut;
        private PersistentState persistentState;
        private ReaderWriterLockSlim clusterChangeLatch;


        private List<Action> callbacks = new List<Action>();

        private PingPongDevice persistentStorage;

        private SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();
        private Queue<WorkerVersion> frontier = new Queue<WorkerVersion>();
        private ConcurrentQueue<WorkerVersion> outstandingWvs = new ConcurrentQueue<WorkerVersion>();
        private ConcurrentDictionary<Worker, long> versionTable = new ConcurrentDictionary<Worker, long>();

        private byte[] serializationBuffer;
        
        public EnhancedDprFinderBackend(PingPongDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
            // TODO(Tianyu): initialize state from disk and workers
        }

        private bool TryCommitWorkerVersion(WorkerVersion wv)
        {
            if (!precedenceGraph.ContainsKey(wv)) return true;

            try
            {
                clusterChangeLatch.EnterReadLock();

                lock (currentCut)
                {
                    if (wv.Version <= currentCut.GetValueOrDefault(wv.Worker, 0))
                    {
                        // already committed. Remove but do not signal changes to the cut
                        if (precedenceGraph.TryRemove(wv, out var list))
                            objectPool.Return(list);
                        return true;
                    }

                    visited.Clear();
                    frontier.Clear();
                    frontier.Enqueue(wv);


                    while (frontier.Count != 0)
                    {
                        var node = frontier.Dequeue();
                        if (visited.Contains(node)) continue;
                        if (currentCut.GetValueOrDefault(node.Worker, 0) >= node.Version) continue;
                        if (!precedenceGraph.TryGetValue(node, out var val)) return false;

                        visited.Add(node);
                        foreach (var dep in val)
                            frontier.Enqueue(dep);
                    }

                    // Lock to ensure readers of current cut do not see partial updates
                    foreach (var committed in visited)
                    {
                        var version = currentCut.GetValueOrDefault(committed.Worker, 0);
                        if (version < committed.Version)
                            currentCut[committed.Worker] = committed.Version;
                        if (precedenceGraph.TryRemove(committed, out var list))
                            objectPool.Return(list);
                    }

                    return true;
                }
            }
            finally
            {
                clusterChangeLatch.ExitReadLock();
            }
        }

        public void TryFindDprCut()
        {
            // Go through the unprocessed wvs and traverse the graph, but give up after a while
            for (var i = 0; i < 50; i++)
            {
                if (!outstandingWvs.TryDequeue(out var wv)) break;
                if (!TryCommitWorkerVersion(wv))
                    outstandingWvs.Enqueue(wv);
            }
        }

        public void NewCheckpoint(long worldLine, WorkerVersion wv, IEnumerable<WorkerVersion> deps)
        {
            // We need to protect against entering outdated information into the graph
            clusterChangeLatch.EnterReadLock();
            if (worldLine != persistentState.currentWorldLine) return;
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
            clusterChangeLatch.ExitReadLock();
        }

        private void FlushPersistentState()
        {
            
        }

        public (long, long) AddWorker(Worker worker, Action<(long, long)> callback)
        {
            clusterChangeLatch.EnterWriteLock();
            versionTable.TryAdd(worker, 0);
            (long, long) result;
            if (persistentState.worldLineDivergencePoint.TryAdd(worker, 0))
            {
                // First time we have seen this worker --- start them at current world-line
                result = (persistentState.currentWorldLine, 0);
            }
            else
            {
                // Otherwise, this worker thinks it's booting up for a second time, which means there was a restart.
                // We count this as a failure. Advance the cluster world-line
                persistentState.currentWorldLine++;
                // TODO(Tianyu): This is slightly more aggressive than needed, but no worse than original DPR. Can do
                // more precise rollback later.
                foreach (var (w, v) in currentCut)
                    persistentState.worldLineDivergencePoint[w] = v;
                // Anything in the precedence graph is rolled back and we can just remove them 
                foreach (var list in precedenceGraph.Values)
                    objectPool.Return(list);
                precedenceGraph.Clear();
                var survivingVersion = currentCut[worker];
                result = (persistentState.currentWorldLine, survivingVersion);
            }

            if (callback != null)
                callbacks.Add(() => callback(result));
            
            // TODO(Tianyu):
            // 1. Serialize this state to buffer
            // 2. Flush to disk
            // 3. Update responses to sync calls 
            // 4. Mark operation complete and invoke callback
            clusterChangeLatch.ExitWriteLock();
            return result;
        }

        public void DeleteWorker(Worker worker, Action callback)
        {
            clusterChangeLatch.EnterWriteLock();

            currentCut.Remove(worker);
            persistentState.worldLineDivergencePoint.Remove(worker);
            persistentState.worldLineDivergencePoint.Remove(worker);
            versionTable.TryRemove(worker, out _);
            if (callback != null)
                callbacks.Add(callback);
            clusterChangeLatch.ExitWriteLock();
            // TODO(Tianyu): Trigger write to storage
        }
        
        public long MaxVersion() => maxVersion;

        public void Dispose()
        {
            persistentStorage?.Dispose();
        }
    }
}
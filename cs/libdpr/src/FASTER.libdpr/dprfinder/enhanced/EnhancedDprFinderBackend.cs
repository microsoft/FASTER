using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace FASTER.libdpr.enhanced
{
    public class ClusterState
    {
        public long currentWorldLine;
        public Dictionary<Worker, long> worldLineDivergencePoint;

        public ClusterState()
        {
            currentWorldLine = 1;
            worldLineDivergencePoint = new Dictionary<Worker, long>();
        }
    }

    public class EnhancedDprFinderBackend
    {
        private long maxVersion = 0;
        private Dictionary<Worker, long> currentCut;

        private ClusterState volatileClusterState;
        private ReaderWriterLockSlim clusterChangeLatch;

        private ConcurrentDictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph =
            new ConcurrentDictionary<WorkerVersion, List<WorkerVersion>>();

        private ConcurrentDictionary<Worker, long> versionTable = new ConcurrentDictionary<Worker, long>();
        private ConcurrentQueue<WorkerVersion> outstandingWvs = new ConcurrentQueue<WorkerVersion>();

        private HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();
        private Queue<WorkerVersion> frontier = new Queue<WorkerVersion>();

        private SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private PingPongDevice persistentStorage;
        private PrecomputedSyncResponse syncResponse;

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
            
            clusterChangeLatch.EnterReadLock();
            // Compute a new syncResponse for consumption
            if (syncResponse.worldLine == volatileClusterState.currentWorldLine)
                syncResponse.UpdateCut(currentCut);
            clusterChangeLatch.ExitReadLock();
        }

        public void NewCheckpoint(long worldLine, WorkerVersion wv, IEnumerable<WorkerVersion> deps)
        {
            // We need to protect against entering outdated information into the graph
            clusterChangeLatch.EnterReadLock();
            
            // The DprFinder should be the most up-to-date w.r.t. worldlines
            Debug.Assert(worldLine <= volatileClusterState.currentWorldLine);
            // Unless the reported versions are in the current world-line (or belong to the common prefix), we should
            // not allow this write to go through
            if (worldLine != volatileClusterState.currentWorldLine
                && wv.Version > volatileClusterState.worldLineDivergencePoint[wv.Worker]) return;
            
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

        public (long, long) AddWorker(Worker worker, Action<(long, long)> callback)
        {
            clusterChangeLatch.EnterWriteLock();
            versionTable.TryAdd(worker, 0);
            (long, long) result;
            if (volatileClusterState.worldLineDivergencePoint.TryAdd(worker, 0))
            {
                // First time we have seen this worker --- start them at current world-line
                result = (volatileClusterState.currentWorldLine, 0);
            }
            else
            {
                // Otherwise, this worker thinks it's booting up for a second time, which means there was a restart.
                // We count this as a failure. Advance the cluster world-line
                volatileClusterState.currentWorldLine++;
                // TODO(Tianyu): This is slightly more aggressive than needed, but no worse than original DPR. Can
                // implement more precise rollback later.
                foreach (var (w, v) in currentCut)
                    volatileClusterState.worldLineDivergencePoint[w] = v;
                // Anything in the precedence graph is rolled back and we can just remove them 
                foreach (var list in precedenceGraph.Values)
                    objectPool.Return(list);
                precedenceGraph.Clear();
                var survivingVersion = currentCut[worker];
                result = (volatileClusterState.currentWorldLine, survivingVersion);
            }

            var newResponse = new PrecomputedSyncResponse(volatileClusterState);
            persistentStorage.WriteReliablyAsync(newResponse.serializedResponse, 0, newResponse.recoveryStateEnd, () =>
            {
                clusterChangeLatch.EnterWriteLock();
                newResponse.UpdateCut(currentCut);
                if (syncResponse.worldLine < newResponse.worldLine)
                    syncResponse = newResponse;
                clusterChangeLatch.ExitWriteLock();
                callback?.Invoke(result);
            });
            clusterChangeLatch.ExitWriteLock();
            return result;
        }

        public void DeleteWorker(Worker worker, Action callback = null)
        {
            clusterChangeLatch.EnterWriteLock();

            currentCut.Remove(worker);
            volatileClusterState.worldLineDivergencePoint.Remove(worker);
            volatileClusterState.worldLineDivergencePoint.Remove(worker);
            versionTable.TryRemove(worker, out _);

            var newResponse = new PrecomputedSyncResponse(volatileClusterState);
            persistentStorage.WriteReliablyAsync(newResponse.serializedResponse, 0, newResponse.recoveryStateEnd, () =>
            {
                clusterChangeLatch.EnterWriteLock();
                newResponse.UpdateCut(currentCut);
                if (syncResponse.worldLine < newResponse.worldLine)
                    syncResponse = newResponse;
                clusterChangeLatch.ExitWriteLock();
                callback?.Invoke();
            });
            clusterChangeLatch.ExitWriteLock();
        }

        public long MaxVersion() => maxVersion;

        public void Dispose()
        {
            persistentStorage?.Dispose();
        }
    }
}
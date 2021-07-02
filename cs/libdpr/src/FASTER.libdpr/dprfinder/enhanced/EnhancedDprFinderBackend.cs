using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace FASTER.libdpr
{
    public class PrecomputedSyncResponse
    {
        public ReaderWriterLockSlim rwLatch;
        public long worldLine;
        public byte[] serializedResponse;
        public int recoveryStateEnd, responseEnd;

        public PrecomputedSyncResponse(ClusterState clusterState)
        {
            rwLatch = new ReaderWriterLockSlim();
            worldLine = clusterState.currentWorldLine;
            var serializedSize = sizeof(long) + RespUtil.DictionarySerializedSize(clusterState.worldLineDivergencePoint);
            if (serializedSize > serializedResponse.Length)
                serializedResponse = new byte[Math.Max(2 * serializedResponse.Length, serializedSize)];

            BitConverter.TryWriteBytes(new Span<byte>(serializedResponse, 0, sizeof(long)),
                clusterState.currentWorldLine);
            recoveryStateEnd = RespUtil.SerializeDictionary(clusterState.worldLineDivergencePoint, serializedResponse, sizeof(long));
            responseEnd = recoveryStateEnd;
        }

        internal void UpdateCut(Dictionary<Worker, long> newCut)
        {
            rwLatch.EnterWriteLock();
            var serializedSize = RespUtil.DictionarySerializedSize(newCut);
            if (serializedSize > serializedResponse.Length - recoveryStateEnd)
            {
                var newBuffer = new byte[Math.Max(2 * serializedResponse.Length, recoveryStateEnd + serializedSize)];
                Array.Copy(serializedResponse, newBuffer, recoveryStateEnd);
                serializedResponse = newBuffer;
            }

            responseEnd = RespUtil.SerializeDictionary(newCut, serializedResponse, recoveryStateEnd);
            rwLatch.EnterReadLock();
        }
    }
    
    public class ClusterState
    {
        public long currentWorldLine;
        public Dictionary<Worker, long> worldLineDivergencePoint;

        public ClusterState()
        {
            currentWorldLine = 0;
            worldLineDivergencePoint = new Dictionary<Worker, long>();
        }

        public ClusterState(byte[] buf, int offset)
        {
            currentWorldLine = BitConverter.ToInt64(buf, offset);
            worldLineDivergencePoint = new Dictionary<Worker, long>();
            RespUtil.ReadDictionaryFromBytes(buf, offset + sizeof(long), worldLineDivergencePoint);
        }
    }
    

    public class EnhancedDprFinderBackend
    {
        private class RecoveryState
        {
            private EnhancedDprFinderBackend backend;
            private bool recoveryComplete;
            private CountdownEvent countdown;
            private ConcurrentDictionary<Worker, byte> workersUnaccontedFor = new ConcurrentDictionary<Worker, byte>();

            public RecoveryState(EnhancedDprFinderBackend backend, ClusterState recoveredState)
            {
                this.backend = backend;
                recoveryComplete = false;
                // Mark all previously known worker as unaccounted for --- we cannot make any statements about the
                // current state of the cluster until we are sure we have up-to-date information from all of them
                foreach (var w in recoveredState.worldLineDivergencePoint.Keys)
                    workersUnaccontedFor.TryAdd(w, 0);
                countdown = new CountdownEvent(workersUnaccontedFor.Count);
            }

            public bool RecoveryComplete() => recoveryComplete;

            public void MarkWorkerAccountedFor(Worker worker)
            {
                if (!workersUnaccontedFor.TryRemove(worker, out _)) return;
                if (!countdown.Signal()) return;
                // At this point, we have all information that is at least as up-to-date as when we crashed. We can
                // traverse the graph and be sure to reach a conclusion that's at least as up-to-date as the guarantees
                // we may have given out before we crashed.
                backend.TryFindDprCut(true);
                // Only mark recovery complete after we have reached that conclusion
                recoveryComplete = true;
            }
        }
        
        private long maxVersion = 0;
        private Dictionary<Worker, long> currentCut = new Dictionary<Worker, long>();

        private ClusterState volatileClusterState;
        private ReaderWriterLockSlim clusterChangeLatch = new ReaderWriterLockSlim();

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

        // Only used during DprFinder recovery
        private RecoveryState recoveryState;
        
        public EnhancedDprFinderBackend(PingPongDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
            if (persistentStorage.ReadLatestCompleteWrite(out var buf))
            {
                volatileClusterState = new ClusterState(buf, 0);
                // TODO(Tianyu): Set up recovery state

            }
            else
            {
                volatileClusterState = new ClusterState();
            }
            syncResponse = new PrecomputedSyncResponse(volatileClusterState);
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

        public void TryFindDprCut(bool tryCommitAll = false)
        {
            // Unable to make commit progress until we rebuild precedence graph from worker's persistent storage
            if (!recoveryState.RecoveryComplete()) return;
            
            // Go through the unprocessed wvs and traverse the graph, but give up after a while
            var threshold = tryCommitAll ? outstandingWvs.Count : 50;
            for (var i = 0; i < threshold; i++)
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
            try
            {
                // We need to protect against entering outdated information into the graph
                clusterChangeLatch.EnterReadLock();

                // The DprFinder should be the most up-to-date w.r.t. world-lines
                Debug.Assert(worldLine <= volatileClusterState.currentWorldLine);
                // Unless the reported versions are in the current world-line (or belong to the common prefix), we should
                // not allow this write to go through
                if (worldLine != volatileClusterState.currentWorldLine
                    && wv.Version > volatileClusterState.worldLineDivergencePoint[wv.Worker]) return;
                // do not add duplicate entries to the graph
                if (precedenceGraph.ContainsKey(wv))
                    return;
                var list = objectPool.Checkout();
                list.Clear();
                var prev = versionTable[wv.Worker];
                list.Add(new WorkerVersion(wv.Worker, prev));
                Debug.Assert(deps.Select(d => d.Worker).All(versionTable.ContainsKey));
                list.AddRange(deps);
                maxVersion = Math.Max(wv.Version, maxVersion);
                outstandingWvs.Enqueue(wv);
                versionTable.TryUpdate(wv.Worker, wv.Version, prev);
            }
            finally
            {
                clusterChangeLatch.ExitReadLock();
            }
        }

        public void MarkWorkerAccountedFor(Worker worker, long earliestUncommittedVersion)
        {
            // Lock here because can be accessed from multiple threads. No need to lock once all workers are accounted
            // for as then only the graph traversal thread will update current cut
            lock (currentCut)
                currentCut[worker] = earliestUncommittedVersion;
            recoveryState.MarkWorkerAccountedFor(worker);
        }

        public (long, long) AddWorker(Worker worker, Action<(long, long)> callback)
        {
            // Before adding a worker, make sure all workers have already reported all (if any) locally outstanding 
            // checkpoints. We require this to be able to process the request.
            if (!recoveryState.RecoveryComplete()) throw new InvalidOperationException();

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
            // Before adding a worker, make sure all workers have already reported all (if any) locally outstanding 
            // checkpoints. We require this to be able to process the request.
            if (!recoveryState.RecoveryComplete()) throw new InvalidOperationException();
            
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

        public PrecomputedSyncResponse GetPrecomputedResponse() => syncResponse;

        public long MaxVersion() => maxVersion;

        public void Dispose()
        {
            persistentStorage?.Dispose();
        }
    }
}
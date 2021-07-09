using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    public class PrecomputedSyncResponse
    {
        public ReaderWriterLockSlim rwLatch;
        public long worldLine;
        public byte[] serializedResponse = new byte[1 << 15];
        public int recoveryStateEnd, responseEnd;

        public PrecomputedSyncResponse(ClusterState clusterState)
        {
            rwLatch = new ReaderWriterLockSlim();
            worldLine = clusterState.currentWorldLine;
            var serializedSize = sizeof(long) + RespUtil.DictionarySerializedSize(clusterState.worldLinePrefix);
            if (serializedSize > serializedResponse.Length)
                serializedResponse = new byte[Math.Max(2 * serializedResponse.Length, serializedSize)];

            BitConverter.TryWriteBytes(new Span<byte>(serializedResponse, 0, sizeof(long)),
                clusterState.currentWorldLine);
            recoveryStateEnd =
                RespUtil.SerializeDictionary(clusterState.worldLinePrefix, serializedResponse, sizeof(long));
            // In the absence of a cut, set cut to a special unknown value.
            BitConverter.TryWriteBytes(new Span<byte>(serializedResponse, recoveryStateEnd, sizeof(long)), -1L);
            responseEnd = recoveryStateEnd + sizeof(long);
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
            rwLatch.ExitWriteLock();
        }
    }

    public class ClusterState
    {
        public long currentWorldLine;
        public Dictionary<Worker, long> worldLinePrefix;

        public ClusterState()
        {
            currentWorldLine = 1;
            worldLinePrefix = new Dictionary<Worker, long>();
        }

        public static ClusterState FromBuffer(byte[] buf, int offset, out int head)
        {
            var result = new ClusterState();
            result.currentWorldLine = BitConverter.ToInt64(buf, offset);
            result.worldLinePrefix = new Dictionary<Worker, long>();
            head = RespUtil.ReadDictionaryFromBytes(buf, offset + sizeof(long), result.worldLinePrefix);
            return result;
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

            public RecoveryState(EnhancedDprFinderBackend backend)
            {
                this.backend = backend;
                if (backend.volatileClusterState.worldLinePrefix.Count == 0)
                {
                    backend.GetPrecomputedResponse().UpdateCut(backend.currentCut);
                    recoveryComplete = true;
                    return;
                }

                recoveryComplete = false;
                // Mark all previously known worker as unaccounted for --- we cannot make any statements about the
                // current state of the cluster until we are sure we have up-to-date information from all of them
                foreach (var w in backend.volatileClusterState.worldLinePrefix.Keys)
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
                backend.cutChanged = true;
                backend.TryCommitVersions(true);
                
                // Only mark recovery complete after we have reached that conclusion
                recoveryComplete = true;
            }
        }

        private long maxVersion = 0;
        private Dictionary<Worker, long> currentCut = new Dictionary<Worker, long>();
        private bool cutChanged;

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

        // Used to send add/delete worker requests to processing thread
        private ConcurrentQueue<(Worker, Action<(long, long)>)> addQueue =
            new ConcurrentQueue<(Worker, Action<(long, long)>)>();

        private ConcurrentQueue<(Worker, Action)> deleteQueue = new ConcurrentQueue<(Worker, Action)>();

        // Only used during DprFinder recovery
        private RecoveryState recoveryState;

        public EnhancedDprFinderBackend(PingPongDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
            if (persistentStorage.ReadLatestCompleteWrite(out var buf))
                volatileClusterState = ClusterState.FromBuffer(buf, 0, out _);
            else
                volatileClusterState = new ClusterState();

            syncResponse = new PrecomputedSyncResponse(volatileClusterState);
            recoveryState = new RecoveryState(this);
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

                foreach (var committed in visited)
                {
                    cutChanged = true;
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

        public void Process()
        {
            // Unable to make commit progress until we rebuild precedence graph from worker's persistent storage
            if (!recoveryState.RecoveryComplete()) return;

            if (!addQueue.IsEmpty || !deleteQueue.IsEmpty)
            {
                // Because this code-path is rare, ok to allocate new data structures here
                var callbacks = new List<Action>();

                // Need to grab an exclusive lock to ensure that if a rollback is triggered, there are no concurrent
                // NewCheckpoint calls that can pollute the graph with rolled back versions
                clusterChangeLatch.EnterWriteLock();

                // Process cluster change requests
                while (addQueue.TryDequeue(out var entry))
                {
                    var result = ProcessAddWorker(entry.Item1);
                    callbacks.Add(() => entry.Item2(result));
                }

                while (deleteQueue.TryDequeue(out var entry))
                {
                    ProcessDeleteWorker(entry.Item1);
                    callbacks.Add(() => entry.Item2());
                }

                // serialize new cluster state and persist
                var newResponse = new PrecomputedSyncResponse(volatileClusterState);
                persistentStorage.WriteReliably(newResponse.serializedResponse, 0, newResponse.recoveryStateEnd);
                newResponse.UpdateCut(currentCut);
                syncResponse = newResponse;
                clusterChangeLatch.ExitWriteLock();

                foreach (var callback in callbacks)
                    callback();
            }

            TryCommitVersions();
        }

        private void TryCommitVersions(bool tryCommitAll = false)
        {
            // Go through the unprocessed wvs and traverse the graph, unless instructed otherwise, give up after a while
            // to return control to the calling thread 
            var threshold = tryCommitAll ? outstandingWvs.Count : 100;
            for (var i = 0; i < threshold; i++)
            {
                if (!outstandingWvs.TryDequeue(out var wv)) break;
                if (!TryCommitWorkerVersion(wv))
                    outstandingWvs.Enqueue(wv);
            }

            if (cutChanged)
            {
                clusterChangeLatch.EnterReadLock();
                // Compute a new syncResponse for consumption

                // We need to make sure we are not writing a newer guarantee to an older world-line, but I am pretty
                // convinced this will never happen because the graph can only continue to evolve when the new world-line
                // is published (as part of the new syncResponse)
                Debug.Assert(syncResponse.worldLine == volatileClusterState.currentWorldLine);
                syncResponse.UpdateCut(currentCut);
                cutChanged = false;
                clusterChangeLatch.ExitReadLock();
            }
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
                    && wv.Version > volatileClusterState.worldLinePrefix[wv.Worker]) return;
                // do not add duplicate entries to the graph
                if (precedenceGraph.ContainsKey(wv))
                    return;
                var list = objectPool.Checkout();
                list.Clear();
                Debug.Assert(deps.Select(d => d.Worker).All(versionTable.ContainsKey));
                list.AddRange(deps);
                maxVersion = Math.Max(wv.Version, maxVersion);
                outstandingWvs.Enqueue(wv);
                versionTable.AddOrUpdate(wv.Worker, wv.Version, (w, old) => Math.Max(old, wv.Version));
                precedenceGraph.TryAdd(wv, list);
            }
            finally
            {
                clusterChangeLatch.ExitReadLock();
            }
        }

        public void MarkWorkerAccountedFor(Worker worker, long earliestPresentVersion)
        {
            // Should not be invoked if recovery is underway
            Debug.Assert(!recoveryState.RecoveryComplete());
            // Lock here because can be accessed from multiple threads. No need to lock once all workers are accounted
            // for as then only the graph traversal thread will update current cut
            lock (currentCut)
            {
                Debug.Assert(earliestPresentVersion <= volatileClusterState.worldLinePrefix[worker]);
                // We don't know if a present version is committed or not, but all pruned versions must be committed.
                currentCut[worker] = Math.Max(0, earliestPresentVersion - 1);
            }

            recoveryState.MarkWorkerAccountedFor(worker);
        }

        public void AddWorker(Worker worker, Action<(long, long)> callback = null) =>
            addQueue.Enqueue(ValueTuple.Create(worker, callback));

        private (long, long) ProcessAddWorker(Worker worker)
        {
            // Before adding a worker, make sure all workers have already reported all (if any) locally outstanding 
            // checkpoints. We require this to be able to process the request.
            if (!recoveryState.RecoveryComplete()) throw new InvalidOperationException();

            versionTable.TryAdd(worker, 0);
            (long, long) result;
            if (volatileClusterState.worldLinePrefix.TryAdd(worker, 0))
            {
                // First time we have seen this worker --- start them at current world-line
                result = (volatileClusterState.currentWorldLine, 0);
                currentCut.Add(worker, 0);
                cutChanged = true;
            }
            else
            {
                // Otherwise, this worker thinks it's booting up for a second time, which means there was a restart.
                // We count this as a failure. Advance the cluster world-line
                volatileClusterState.currentWorldLine++;
                // TODO(Tianyu): This is slightly more aggressive than needed, but no worse than original DPR. Can
                // implement more precise rollback later.
                foreach (var (w, v) in currentCut)
                    volatileClusterState.worldLinePrefix[w] = v;
                // Anything in the precedence graph is rolled back and we can just remove them 
                foreach (var list in precedenceGraph.Values)
                    objectPool.Return(list);
                precedenceGraph.Clear();
                var survivingVersion = currentCut[worker];
                result = (volatileClusterState.currentWorldLine, survivingVersion);
            }

            return result;
        }

        public void DeleteWorker(Worker worker, Action callback = null) =>
            deleteQueue.Enqueue(ValueTuple.Create(worker, callback));

        private void ProcessDeleteWorker(Worker worker)
        {
            // Before adding a worker, make sure all workers have already reported all (if any) locally outstanding 
            // checkpoints. We require this to be able to process the request.
            if (!recoveryState.RecoveryComplete()) throw new InvalidOperationException();

            currentCut.Remove(worker);
            cutChanged = true;
            volatileClusterState.worldLinePrefix.Remove(worker);
            volatileClusterState.worldLinePrefix.Remove(worker);
            versionTable.TryRemove(worker, out _);
        }

        public PrecomputedSyncResponse GetPrecomputedResponse() => syncResponse;

        public long MaxVersion() => maxVersion;

        public void Dispose()
        {
            persistentStorage?.Dispose();
        }
    }
}
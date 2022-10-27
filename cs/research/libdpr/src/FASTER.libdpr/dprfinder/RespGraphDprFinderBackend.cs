using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FASTER.common;

namespace FASTER.libdpr
{
    /// <summary>
    ///     Precomputed response to sync calls into DprFinder. Holds both serialized cluster persistent state and the
    ///     current DPR cut.
    /// </summary>
    public class PrecomputedSyncResponse
    {
        /// <summary>
        ///     end offset of the serialized portion of cluster state on the response buffer
        /// </summary>
        public int recoveryStateEnd;

        /// <summary>
        ///     end offset of the entire response on the buffer
        /// </summary>
        public int responseEnd;

        /// <summary>
        ///     Reader/Writer latch that protects members --- if reading the serialized response, must do so under
        ///     read latch to prevent concurrent modification.
        /// </summary>
        public ReaderWriterLockSlim rwLatch;

        /// <summary>
        ///     buffer that holds the serialized state
        /// </summary>
        public byte[] serializedResponse = new byte[1 << 15];

        /// <summary>
        ///     Create a new PrecomputedSyncResponse from the given cluster state. Until UpdateCut is called, the
        ///     serialized response will have a special value for the cut to signal its absence.
        /// </summary>
        /// <param name="clusterState"> cluster state to serialize </param>
        public PrecomputedSyncResponse(ClusterState clusterState)
        {
            rwLatch = new ReaderWriterLockSlim();
            // Reserve space for world-line + prefix + size field of cut as a minimum
            var serializedSize = sizeof(long) + RespUtil.DictionarySerializedSize(clusterState.worldLinePrefix) +
                                 sizeof(int);
            // Resize response buffer to fit
            if (serializedSize > serializedResponse.Length)
                serializedResponse = new byte[Math.Max(2 * serializedResponse.Length, serializedSize)];

            Utility.TryWriteBytes(new Span<byte>(serializedResponse, 0, sizeof(long)),
                clusterState.currentWorldLine);
            recoveryStateEnd =
                RespUtil.SerializeDictionary(clusterState.worldLinePrefix, serializedResponse, sizeof(long));
            // In the absence of a cut, set cut to a special "unknown" value.
            Utility.TryWriteBytes(new Span<byte>(serializedResponse, recoveryStateEnd, sizeof(int)), -1);
            responseEnd = recoveryStateEnd + sizeof(int);
        }

        /// <summary>
        ///     Update the PrecomputedSyncResponse to hold the given cut
        /// </summary>
        /// <param name="newCut"> DPR cut to serialize </param>
        internal void UpdateCut(Dictionary<Worker, long> newCut)
        {
            // Update serialized under write latch so readers cannot see partial updates
            rwLatch.EnterWriteLock();
            var serializedSize = RespUtil.DictionarySerializedSize(newCut);

            // Resize response buffer to fit
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

    /// <summary>
    ///     Persistent state about the DPR cluster
    /// </summary>
    public class ClusterState
    {
        /// <summary>
        ///     Latest recorded world-line of the cluster
        /// </summary>
        public long currentWorldLine;

        /// <summary>
        ///     common prefix of the current world-line with previous ones. In other words, this is the cut workers had
        ///     to recover to before entering the current world-line. This also serves as point of truth of cluster
        ///     membership, and a worker is recognized as part of the cluster iff they have an entry in this dictionary.
        ///     Workers that did not participate in the previous world-lines have 0 in the cut.
        /// </summary>
        public Dictionary<Worker, long> worldLinePrefix;

        /// <summary>
        ///     Creates a new ClusterState object for an empty cluster
        /// </summary>
        public ClusterState()
        {
            currentWorldLine = 1;
            worldLinePrefix = new Dictionary<Worker, long>();
        }

        /// <summary>
        ///     Creates a ClusterState from serialized bytes
        /// </summary>
        /// <param name="buf"> byte buffer that holds serialized cluster state</param>
        /// <param name="offset"> offset to start scanning </param>
        /// <param name="head"> end of the serialized ClusterState on the buffer </param>
        /// <returns> new ClusterState from serialized bytes</returns>
        public static ClusterState FromBuffer(byte[] buf, int offset, out int head)
        {
            var result = new ClusterState
            {
                currentWorldLine = BitConverter.ToInt64(buf, offset),
                worldLinePrefix = new Dictionary<Worker, long>()
            };
            head = RespUtil.ReadDictionaryFromBytes(buf, offset + sizeof(long), result.worldLinePrefix);
            return result;
        }
    }


    /// <summary>
    ///     Backend logic for the RespGraphDprFinderServer.
    ///     The implementation relies on state objects to persist dependencies and avoids incurring additional storage
    ///     round-trips on the commit critical path.
    /// </summary>
    public class RespGraphDprFinderBackend
    {
        // Used to send add/delete worker requests to processing thread
        private readonly ConcurrentQueue<(Worker, Action<(long, long)>)> addQueue =
            new ConcurrentQueue<(Worker, Action<(long, long)>)>();

        private readonly ReaderWriterLockSlim clusterChangeLatch = new ReaderWriterLockSlim();
        private readonly Dictionary<Worker, long> currentCut = new Dictionary<Worker, long>();
        private bool cutChanged;
        private readonly ConcurrentQueue<(Worker, Action)> deleteQueue = new ConcurrentQueue<(Worker, Action)>();
        private readonly Queue<WorkerVersion> frontier = new Queue<WorkerVersion>();

        private long maxVersion;

        private readonly SimpleObjectPool<List<WorkerVersion>> objectPool =
            new SimpleObjectPool<List<WorkerVersion>>(() => new List<WorkerVersion>());

        private readonly ConcurrentQueue<WorkerVersion> outstandingWvs = new ConcurrentQueue<WorkerVersion>();

        private readonly PingPongDevice persistentStorage;

        private readonly ConcurrentDictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph =
            new ConcurrentDictionary<WorkerVersion, List<WorkerVersion>>();

        // Only used during DprFinder recovery
        private readonly RecoveryState recoveryState;
        private PrecomputedSyncResponse syncResponse;
        private readonly ConcurrentDictionary<Worker, long> versionTable = new ConcurrentDictionary<Worker, long>();
        private readonly HashSet<WorkerVersion> visited = new HashSet<WorkerVersion>();
        private readonly ClusterState volatileClusterState;

        /// <summary>
        ///     Create a new EnhancedDprFinderBackend backed by the given storage. If the storage holds a valid persisted
        ///     EnhancedDprFinderBackend state, the constructor will attempt to recover from it.
        /// </summary>
        /// <param name="persistentStorage"> persistent storage backing this dpr finder </param>
        public RespGraphDprFinderBackend(PingPongDevice persistentStorage)
        {
            this.persistentStorage = persistentStorage;
            // see if a previously persisted state is available
            var buf = persistentStorage.ReadLatestCompleteWrite();
            if (buf != null)
                volatileClusterState = ClusterState.FromBuffer(buf, 0, out _);
            else
                volatileClusterState = new ClusterState();

            syncResponse = new PrecomputedSyncResponse(volatileClusterState);
            recoveryState = new RecoveryState(this);
        }

        // Try to commit a single worker version by chasing through its dependencies
        // The worker version supplied in the argument must already be reported as persistent
        private bool TryCommitWorkerVersion(WorkerVersion wv)
        {
            // Because wv is already persistent, if it's not in the graph that means it was pruned as part of a commit.
            // Ok to return as committed, but no need to mark the cut as changed
            if (!precedenceGraph.ContainsKey(wv)) return true;

            // Perform graph traversal in mutual exclusion with cluster change, but it is ok for traversal to be 
            // concurrent with adding of new versions to the graph. 
            try
            {
                clusterChangeLatch.EnterReadLock();

                // If version is in the graph but somehow already committed, remove it and reclaim associated resources
                if (wv.Version <= currentCut.GetValueOrDefault(wv.Worker, 0))
                {
                    // already committed. Remove but do not signal changes to the cut
                    if (precedenceGraph.TryRemove(wv, out var list))
                        objectPool.Return(list);
                    return true;
                }

                // Prepare traversal data structures
                visited.Clear();
                frontier.Clear();
                frontier.Enqueue(wv);

                // Breadth first search to find all dependencies
                while (frontier.Count != 0)
                {
                    var node = frontier.Dequeue();
                    if (visited.Contains(node)) continue;
                    // If node is committed as determined by the cut, ok to continue
                    if (currentCut.GetValueOrDefault(node.Worker, 0) >= node.Version) continue;
                    // Otherwise, need to check if it is persistent (and therefore present in the graph)
                    if (!precedenceGraph.TryGetValue(node, out var val)) return false;

                    visited.Add(node);
                    foreach (var dep in val)
                        frontier.Enqueue(dep);
                }

                // If all dependencies are present, we should commit them all
                // This will appear atomic without special protection as we serialize out the cut for sync calls in
                // the same thread later on. Other calls reading the cut involve cluster changes and cannot
                // interleave with this code
                foreach (var committed in visited)
                {
                    // Mark cut as changed so we know to serialize the new cut later on
                    cutChanged = true;
                    var version = currentCut.GetValueOrDefault(committed.Worker, 0);
                    // Update cut if necessary
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

        /// <summary>
        ///     Performs work to evolve cluster state and find DPR cuts. Must be called repeatedly for the DprFinder to
        ///     make progress. Process() should only be invoked sequentially, but may be concurrent with other public methods.
        /// </summary>
        public void Process()
        {
            // Unable to make commit progress until we rebuild precedence graph from worker's persistent storage
            if (!recoveryState.RecoveryComplete()) return;

            // Process any cluster change requests
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

            // Traverse the graph to try and commit versions
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
                // Compute a new syncResponse for consumption
                // No need to protect against concurrent changes to the cluster because this method is either called
                // on the process thread or during recovery. No cluster change can interleave.
                syncResponse.UpdateCut(currentCut);
                cutChanged = false;
            }
        }

        /// <summary>
        ///     Adds a new checkpoint to the precedence graph with the given dependencies
        /// </summary>
        /// <param name="worldLine"> world-line of the checkpoint </param>
        /// <param name="wv"> worker version checkpointed </param>
        /// <param name="deps"> dependencies of the checkpoint </param>
        public void NewCheckpoint(long worldLine, WorkerVersion wv, IEnumerable<WorkerVersion> deps)
        {
            // The DprFinder should be the most up-to-date w.r.t. world-lines and we should not ever receive
            // a request from the future. 
            Debug.Assert(worldLine <= volatileClusterState.currentWorldLine);
            Debug.Assert(currentCut.ContainsKey(wv.Worker));
            try
            {
                // Cannot interleave NewCheckpoint calls with cluster changes --- cluster changes may change the 
                // current world-line and remove worker-versions. A concurrent NewCheckpoint may not see that
                // and enter a worker-version that should have been removed after cluster change has finished. 
                clusterChangeLatch.EnterReadLock();
                // Unless the reported versions are in the current world-line (or belong to the common prefix), we should
                // not allow this write to go through
                if (worldLine != volatileClusterState.currentWorldLine
                    && wv.Version > volatileClusterState.worldLinePrefix[wv.Worker]) return;

                var list = objectPool.Checkout();
                list.Clear();
                list.AddRange(deps);
                maxVersion = Math.Max(wv.Version, maxVersion);
                // wv may be duplicate as workers retry sending dependencies. Need to guard against this.
                versionTable.AddOrUpdate(wv.Worker, wv.Version, (w, old) => Math.Max(old, wv.Version));
                if (!precedenceGraph.TryAdd(wv, list))
                    objectPool.Return(list);
                else
                    outstandingWvs.Enqueue(wv);
            }
            finally
            {
                clusterChangeLatch.ExitReadLock();
            }
        }

        public void MarkWorkerAccountedFor(Worker worker, long earliestPresentVersion)
        {
            // Should only be invoked if recovery is underway. However, a new worker may send a blind graph resend. 
            if (!recoveryState.RecoveryComplete()) return;
            // Lock here because can be accessed from multiple threads. No need to lock once all workers are accounted
            // for as then only the graph traversal thread will update current cut
            lock (currentCut)
            {
                Debug.Assert(currentCut.ContainsKey(worker));
                // We don't know if a present version is committed or not, but all pruned versions must be committed.
                currentCut[worker] = Math.Max(currentCut[worker], earliestPresentVersion - 1);
            }

            recoveryState.MarkWorkerAccountedFor(worker);
        }

        /// <summary>
        ///     Add the worker to the cluster. If the worker is already part of the cluster the DprFinder considers that
        ///     a failure recovery and triggers necessary next steps. Given callback is invoked when the effect of this
        ///     call is recoverable on storage
        /// </summary>
        /// <param name="worker"> worker to add to the cluster </param>
        /// <param name="callback"> callback to invoke when worker addition is persistent </param>
        public void AddWorker(Worker worker, Action<(long, long)> callback = null)
        {
            addQueue.Enqueue(ValueTuple.Create(worker, callback));
        }

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
                foreach (var entry in currentCut)
                    volatileClusterState.worldLinePrefix[entry.Key] = entry.Value;
                // Anything in the precedence graph is rolled back and we can just remove them 
                foreach (var list in precedenceGraph.Values)
                    objectPool.Return(list);
                precedenceGraph.Clear();
                var survivingVersion = currentCut[worker];
                result = (volatileClusterState.currentWorldLine, survivingVersion);
            }

            return result;
        }

        /// <summary>
        ///     Delete a worker from the cluster. It is up to the caller to ensure that the worker is not participating in
        ///     any future operations or have outstanding unpersisted operations others may depend on. Until callback is
        ///     invoked, the worker is still considered part of the system and must be recovered if it crashes.
        /// </summary>
        /// <param name="worker"> the worker to delete from the cluster </param>
        /// <param name="callback"> callback to invoke when worker removal is persistent on storage </param>
        public void DeleteWorker(Worker worker, Action callback = null)
        {
            deleteQueue.Enqueue(ValueTuple.Create(worker, callback));
        }

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

        /// <summary></summary>
        /// <returns> PrecomputedResponse to sync calls </returns>
        public PrecomputedSyncResponse GetPrecomputedResponse()
        {
            return syncResponse;
        }

        /// <summary></summary>
        /// <returns> Largest version number seen by this DprFinder </returns>
        public long MaxVersion()
        {
            return maxVersion;
        }

        /// <inheritdoc cref="IDisposable" />
        public void Dispose()
        {
            persistentStorage?.Dispose();
        }

        // Recovery state is the information the backend needs to keep when restarting (presumably because
        // of failure) from previous on-disk state.
        private class RecoveryState
        {
            private readonly RespGraphDprFinderBackend backend;
            private readonly CountdownEvent countdown;
            private bool recoveryComplete;

            private readonly ConcurrentDictionary<Worker, byte> workersUnaccontedFor =
                new ConcurrentDictionary<Worker, byte>();

            internal RecoveryState(RespGraphDprFinderBackend backend)
            {
                this.backend = backend;
                // Check if the cluster is empty
                if (backend.volatileClusterState.worldLinePrefix.Count == 0)
                {
                    // If so, we do not need to recover anything, simply finish writing out the empty cut and mark
                    // this backend as fully recovered for future operations
                    backend.GetPrecomputedResponse().UpdateCut(backend.currentCut);
                    recoveryComplete = true;
                    return;
                }

                // Otherwise, we need to first rebuild an in-memory precedence graph from information persisted
                // at each state object. 
                recoveryComplete = false;
                // Mark all previously known worker as unaccounted for --- we cannot make any statements about the
                // current state of the cluster until we are sure we have up-to-date information from all of them
                foreach (var w in backend.volatileClusterState.worldLinePrefix.Keys)
                    workersUnaccontedFor.TryAdd(w, 0);
                countdown = new CountdownEvent(workersUnaccontedFor.Count);
            }

            internal bool RecoveryComplete()
            {
                return recoveryComplete;
            }

            // Called when the backend has received all precedence graph information from a worker
            internal void MarkWorkerAccountedFor(Worker worker)
            {
                // A worker may repeatedly check-in due to crashes or other reason, we need to make sure each
                // worker decrements the count exactly once
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
    }
}
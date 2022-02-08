using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    ///     Maintains per-worker state for DPR version-tracking.
    ///     Shared between the DprManager and DprWorkerCallback. This is separate from DprManager to avoid introducing
    ///     the IStateObject generic type argument to DprWorkerCallback.
    /// </summary>
    internal class DprWorkerState
    {
        internal readonly SimpleObjectPool<LightDependencySet> dependencySetPool;

        internal readonly IDprFinder dprFinder;
        internal readonly Worker me;
        internal readonly ConcurrentDictionary<long, LightDependencySet> versions;

        internal readonly SimpleVersionScheme worldlineTracker;
        internal long lastCheckpointMilli, lastRefreshMilli;
        internal ManualResetEventSlim rollbackProgress;

        internal Stopwatch sw = Stopwatch.StartNew();

        internal DprWorkerState(IDprFinder dprFinder, Worker me)
        {
            this.dprFinder = dprFinder;
            this.me = me;
            worldlineTracker = new SimpleVersionScheme();
            versions = new ConcurrentDictionary<long, LightDependencySet>();
            dependencySetPool = new SimpleObjectPool<LightDependencySet>(() => new LightDependencySet());
        }
    }

    /// <summary>
    ///     A DprServer corresponds to an individual stateful failure domain (e.g., a physical machine or VM) in
    ///     the system. 
    /// </summary>
    /// <typeparam name="TStateObject"></typeparam>
    public class DprServer<TStateObject>
        where TStateObject : IStateObject
    {
        private readonly DprWorkerState state;
        private readonly TStateObject stateObject;
        private readonly byte[] depSerializationArray;
        private readonly SimpleObjectPool<DprBatchVersionTracker> trackers;

        /// <summary>
        /// Creates a new DprServer.
        /// </summary>
        /// <param name="dprFinder"> interface to the cluster's DPR finder component </param>
        /// <param name="me"> unique id of the DPR server </param>
        /// <param name="stateObject"> underlying state object </param>
        public DprServer(IDprFinder dprFinder, Worker me, TStateObject stateObject)
        {
            state = new DprWorkerState(dprFinder, me);
            this.stateObject = stateObject;
            trackers = new SimpleObjectPool<DprBatchVersionTracker>(() => new DprBatchVersionTracker());
            stateObject.Register(new DprWorkerCallbacks(state));
            depSerializationArray = new byte[2 * LightDependencySet.MaxClusterSize * sizeof(long)];
        }

        /// <summary></summary>
        /// <returns> Worker ID of this DprServer instance </returns>
        public Worker Me() => state.me;

        /// <summary>
        /// At the start (restart) of processing, connect to the rest of the DPR cluster. If the worker restarted from
        /// an existing instance, the cluster will detect this and trigger rollback as appropriate across the cluster.
        /// Must be invoked exactly once before any other operations. 
        /// </summary>
        public void ConnectToCluster()
        {
            var v = state.dprFinder.NewWorker(state.me, stateObject);
            // This worker is recovering from some failure and we need to load said checkpoint
            state.worldlineTracker.TryAdvanceVersion((vOld, vNew) =>
            {
                if (v != 0)
                {
                    // If worker is recovering from failure, need to load a previous checkpoint
                    state.rollbackProgress = new ManualResetEventSlim();
                    stateObject.BeginRestore(state.dprFinder.SafeVersion(state.me));
                    // Wait for user to signal end of restore;
                    state.rollbackProgress.Wait();
                }
            }, state.dprFinder.SystemWorldLine());
            state.dprFinder.Refresh();
        }

        /// <summary></summary>
        /// <returns> The underlying state object </returns>
        public TStateObject StateObject()
        {
            return stateObject;
        }

        private ReadOnlySpan<byte> ComputeDependency(long version)
        {
            var deps = state.versions[version];
            var head = 0;
            foreach (var wv in deps)
            {
                Utility.TryWriteBytes(new Span<byte>(depSerializationArray, head, sizeof(long)), wv.Worker.guid);
                head += sizeof(long);
                Utility.TryWriteBytes(new Span<byte>(depSerializationArray, head, sizeof(long)), wv.Version);
                head += sizeof(long);
            }

            return new ReadOnlySpan<byte>(depSerializationArray, 0, head);
        }

        /// <summary>
        /// Check whether this DprServer is due to refresh its view of the cluster or need to perform a checkpoint ---
        /// if so, perform the relevant operation(s). Roughly ensures that both are only actually invoked as frequently
        /// as the given intervals for checkpoint and refresh. 
        /// </summary>
        /// <param name="checkpointPeriodMilli">The rough interval, in milliseconds, expected between two checkpoints</param>
        /// <param name="refreshPeriodMilli">The rough interval, in milliseconds, expected between two refreshes</param>
        public void TryRefreshAndCheckpoint(long checkpointPeriodMilli, long refreshPeriodMilli)
        {
            var currentTime = state.sw.ElapsedMilliseconds;

            var lastCommitted = state.dprFinder.SafeVersion(state.me);

            if (state.lastRefreshMilli + refreshPeriodMilli < currentTime)
            {
                if (!state.dprFinder.Refresh())
                    state.dprFinder.ResendGraph(state.me, stateObject);
                core.Utility.MonotonicUpdate(ref state.lastRefreshMilli, currentTime, out _);
                TryAdvanceWorldLineTo(state.dprFinder.SystemWorldLine());
            }

            if (state.lastCheckpointMilli + checkpointPeriodMilli <= currentTime)
            {
                stateObject.BeginCheckpoint(ComputeDependency,
                    Math.Max(stateObject.Version() + 1, state.dprFinder.GlobalMaxVersion()));
                core.Utility.MonotonicUpdate(ref state.lastCheckpointMilli, currentTime, out _);
            }

            // Can prune dependency information of committed versions
            var newCommitted = state.dprFinder.SafeVersion(state.me);
            for (var i = lastCommitted; i < newCommitted; i++)
                stateObject.PruneVersion(i);
        }

        private void TryAdvanceWorldLineTo(long targetWorldline)
        {
            state.worldlineTracker.TryAdvanceVersion((vOld, vNew) =>
            {
                state.rollbackProgress = new ManualResetEventSlim();
                stateObject.BeginRestore(state.dprFinder.SafeVersion(state.me));
                // Wait for user to signal end of restore;
                state.rollbackProgress.Wait();
            }, targetWorldline);
        }

        /// <summary>
        ///     When a remote request fails to begin (because of failures), compose an error DPR header to the
        ///     sending DPR
        /// component. 
        /// </summary>
        /// <param name="requestBytes">the original request that failed</param>
        /// <param name="response"> the response message to populate</param>
        /// <returns> size of the response message, or negative of the required size if it does not fit </returns>
        public int ComposeErrorResponse(ReadOnlySpan<byte> requestBytes, Span<byte> response)
        {
            var cast = MemoryMarshal.Cast<byte, DprBatchHeader>(requestBytes);
            ref readonly var request = ref MemoryMarshal.GetReference(cast);
            if (response.Length < DprBatchHeader.FixedLenSize) return -DprBatchHeader.FixedLenSize;

            ref var responseObj =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchHeader>(response));
            responseObj.srcWorkerId = Me();
            // Use negative to signal that there was a mismatch, which would prompt error handling on client side
            // Must be negative to distinguish from a normal response message in current version
            responseObj.worldLine = -state.worldlineTracker.Version();
            responseObj.batchId = request.batchId;
            return DprBatchHeader.FixedLenSize;
        }

        /// <summary>
        ///     Invoke before beginning processing of a batch received from a remote worker/client. If the function
        ///     returns false, the batch must not be executed to preserve DPR consistency (caller may respond with an
        ///     error message using ComposeErrorResponse). Otherwise, when the function returns, the batch is safe to
        ///     execute. In the true case, there must eventually be a matching SignalRemoteBatchFinish call on the same
        ///     thread for DPR to make progress. Only one batch is allowed to be active on a thread at a given time. 
        /// </summary>
        /// <param name="requestBytes">Dpr request message from user</param>
        /// <param name="tracker">Tracker to use for batch execution</param>
        /// <returns>Whether the batch can be executed</returns>
        public unsafe bool RequestRemoteBatchBegin(ReadOnlySpan<byte> requestBytes,
            out DprBatchVersionTracker tracker)
        {
            var cast = MemoryMarshal.Cast<byte, DprBatchHeader>(requestBytes);
            ref readonly var request = ref MemoryMarshal.GetReference(cast);
            
            // Wait for worker version to catch up to largest in batch (minimum version where all operations
            // can be safely executed), taking checkpoints if necessary.
            while (request.version > stateObject.Version())
            {
                stateObject.BeginCheckpoint(ComputeDependency, request.version);
                core.Utility.MonotonicUpdate(ref state.lastCheckpointMilli, state.sw.ElapsedMilliseconds, out _);
                Thread.Yield();
            }

            // Enter protected region for world-lines. Because we validate requests batch-at-a-time, the world-line
            // must not shift while a batch is being processed, otherwise a message from an older world-line may be
            // processed in a new one. 
            var wl = state.worldlineTracker.Enter();
            // If the worker world-line is behind, wait for worker to recover up to the same point as the client,
            // so client operation is not lost in a rollback that the client has already observed.
            while (request.worldLine > wl)
            {
                state.worldlineTracker.Leave();
                TryAdvanceWorldLineTo(request.worldLine);
                Thread.Yield();
                wl = state.worldlineTracker.Enter();
            }

            // If the worker world-line is newer, the request must be rejected. 
            if (request.worldLine < wl)
            {
                state.worldlineTracker.Leave();
                tracker = default;
                return false;
            }

            tracker = trackers.Checkout();
            // At this point, we are certain that the request world-line and worker world-line match, and worker
            // world-line will not advance until this thread refreshes the epoch. We can proceed to batch execution.
            Debug.Assert(request.worldLine == wl);

            // Update batch dependencies to the current worker-version. This is an over-approximation, as the batch
            // could get processed at a future version instead due to thread timing. However, this is not a correctness
            // issue, nor do we lose much precision as batch-level dependency tracking is already an approximation.
            var deps = state.versions[stateObject.Version()];
            if (!request.srcWorkerId.Equals(Worker.INVALID))
                deps.Update(request.srcWorkerId, request.version);
            fixed (byte* d = request.data)
            {
                var depsHead = d + request.AdditionalDepsOffset;
                for (var i = 0; i < request.numAdditionalDeps; i++)
                {
                    ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                    deps.Update(wv.Worker, wv.Version);
                    depsHead += sizeof(WorkerVersion);
                }
            }

            // Exit without releasing epoch, as protection is supposed to extend until end of batch.
            return true;
        }

        /// <summary>
        ///     Invoke after processing of a remote batch is complete, and DprBatchVersionTracker has been (optionally)
        ///     populated with a batch offset -> executed version mapping. This function must be invoked after beginning
        ///     a remote batch on the same thread for DPR to make progress.
        /// </summary>
        /// <param name="requestBytes">Dpr request message from user</param>
        /// <param name="response">Dpr response message to be populated</param>
        /// <param name="tracker">Tracker used in batch processing</param>
        /// <returns> size of header if successful, negative size required to hold response if supplied byte span is too small</returns>
        public int SignalRemoteBatchFinish(ReadOnlySpan<byte> requestBytes, Span<byte> response,
            DprBatchVersionTracker tracker)
        {
            var cast = MemoryMarshal.Cast<byte, DprBatchHeader>(requestBytes);
            ref readonly var request = ref MemoryMarshal.GetReference(cast);
            
            ref var dprResponse = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchHeader>(response));
            var responseSize = DprBatchHeader.FixedLenSize + tracker.EncodingSize();
            if (response.Length < responseSize) return -responseSize;

            // Signal batch finished so world-lines can advance. Need to make sure not double-invoked, therefore only
            // called after size validation.
            state.worldlineTracker.Leave();

            // Populate response
            dprResponse.srcWorkerId = Me();
            dprResponse.worldLine = request.worldLine;
            // If not tracking per-operation version within batch, use over-approximation of current version instead
            dprResponse.version = tracker.maxVersion == DprBatchVersionTracker.NotExecuted
                ? stateObject.Version()
                : tracker.maxVersion;
            dprResponse.batchId = request.batchId;
            dprResponse.numAdditionalDeps = 0;
            tracker.AppendToHeader(ref dprResponse);

            tracker.Reset();
            trackers.Return(tracker);
            return responseSize;
        }

        /// <summary>
        ///     Invoke before beginning processing of a locally started batch (e.g., serving a long running subscription
        ///     client or sending a server-to-server message, not in response to a client request) There must eventually
        ///     be a matching SignalLocalBatchFinish call on the same thread for DPR to make progress. Only one batch
        ///     is allowed to be active on a thread at a given time. 
        /// </summary>
        /// <returns>Tracker to use for batch execution</returns>
        public DprBatchVersionTracker RequestLocalBatchBegin()
        {
            state.worldlineTracker.Enter();
            return trackers.Checkout();
        }

        /// <summary>
        ///     Invoke after processing of a local batch is complete, and DprBatchVersionTracker has been (optionally)
        ///     populated with a batch offset -> executed version mapping. This function must be invoked after beginning
        ///     a local batch on the same thread for DPR to make progress.
        /// </summary>
        /// <param name="response">Dpr response message to be populated</param>
        /// <param name="tracker">Tracker used in batch processing</param>
        /// <returns> size of header if successful, negative size required to hold response if supplied byte span is too small</returns>
        public int SignalLocalBatchFinish(Span<byte> response, DprBatchVersionTracker tracker)
        {
            ref var dprResponse = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchHeader>(response));
            var responseSize = DprBatchHeader.FixedLenSize + tracker.EncodingSize();
            if (response.Length < responseSize) return -responseSize;

            var wl = state.worldlineTracker.Version();
            // Signal batch finished so world-lines can advance. Need to make sure not double-invoked, therefore only
            // called after size validation.
            state.worldlineTracker.Leave();

            // Populate response
            dprResponse.srcWorkerId = Me();
            dprResponse.worldLine = wl;
            dprResponse.version = tracker.maxVersion == DprBatchVersionTracker.NotExecuted
                ? stateObject.Version()
                : tracker.maxVersion;
            dprResponse.batchId = ClientBatchTracker.INVALID_BATCH_ID;
            tracker.AppendToHeader(ref dprResponse);

            tracker.Reset();
            trackers.Return(tracker);
            return responseSize;
        }

        /// <summary>
        ///     Force the execution of a checkpoint ahead of the schedule specified at creation time.
        ///     Resets the checkpoint schedule to happen checkpoint_milli after this invocation.
        /// </summary>
        /// <param name="targetVersion"> the version to jump to after the checkpoint, or -1 for the immediate next version</param>
        public void ForceCheckpoint(long targetVersion = -1)
        {
            stateObject.BeginCheckpoint(ComputeDependency, targetVersion);
            core.Utility.MonotonicUpdate(ref state.lastCheckpointMilli, state.sw.ElapsedMilliseconds, out _);
        }
    }
}
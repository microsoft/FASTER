using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{

    /// <summary>
    /// Maintains per-worker state for DPR version-tracking.
    ///
    /// Shared between the DprManager and DprWorkerCallback. This is separate from DprManager to avoid introducing
    /// the IStateObject generic type argument to DprWorkerCallback.
    /// </summary>
    internal class DprWorkerState
    {
        internal DprWorkerState(IDprFinder dprFinder, Worker me)
        {
            this.dprFinder = dprFinder;
            this.me = me;
            worldlineTracker = new SimpleVersionScheme();
            versions = new ConcurrentDictionary<long, LightDependencySet>();
            dependencySetPool = new SimpleObjectPool<LightDependencySet>(() => new LightDependencySet());
        }

        internal readonly IDprFinder dprFinder;

        internal readonly SimpleVersionScheme worldlineTracker;
        internal ManualResetEventSlim rollbackProgress;
        internal readonly ConcurrentDictionary<long, LightDependencySet> versions;
        internal readonly SimpleObjectPool<LightDependencySet> dependencySetPool;
        internal readonly Worker me;

        internal Stopwatch sw = Stopwatch.StartNew();
        internal long lastCheckpointMilli = 0, lastRefreshMilli = 0;
    }

    /// <summary>
    /// DprManager is the primary interface between an underlying state object and the libDPR layer. libDPR expects
    /// to be called before and after every batch execution.
    /// </summary>
    /// <typeparam name="TStateObject"></typeparam>
    /// <typeparam name="TToken"></typeparam>
    public class DprServer<TStateObject>
        where TStateObject : IStateObject
    {
        private readonly DprWorkerState state;
        private readonly TStateObject stateObject;
        private SimpleObjectPool<DprBatchVersionTracker> trackers;
        private byte[] depSerializationArray;

        public DprServer(IDprFinder dprFinder, Worker me, TStateObject stateObject)
        {
            state = new DprWorkerState(dprFinder, me);
            this.stateObject = stateObject;
            trackers = new SimpleObjectPool<DprBatchVersionTracker>(() => new DprBatchVersionTracker());
            stateObject.Register(new DprWorkerCallbacks(state));
            depSerializationArray = new byte[2 * LightDependencySet.MaxClusterSize * sizeof(long)];
        }

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
        }

        public TStateObject StateObject() => stateObject;

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

        public void TryRefreshAndCheckpoint(long checkpointPeriodMilli, long refreshPeriodMilli)
        {
            var currentTime = state.sw.ElapsedMilliseconds;

            var lastCommitted = state.dprFinder.SafeVersion(state.me);

            if (state.lastRefreshMilli + refreshPeriodMilli < currentTime)
            {
                if (!state.dprFinder.Refresh())
                    state.dprFinder.ResendGraph(state.me, stateObject);
                FASTER.core.Utility.MonotonicUpdate(ref state.lastRefreshMilli, currentTime, out _);
                TryAdvanceWorldLineTo(state.dprFinder.SystemWorldLine());
            }

            if (state.lastCheckpointMilli + checkpointPeriodMilli <= currentTime)
            {
                stateObject.BeginCheckpoint(ComputeDependency,
                    Math.Max(stateObject.Version() + 1, state.dprFinder.GlobalMaxVersion()));
                FASTER.core.Utility.MonotonicUpdate(ref state.lastCheckpointMilli, currentTime, out _);
            }

            // Can prune dependency information of committed versions
            var newCommitted = state.dprFinder.SafeVersion(state.me);
            for (var i = lastCommitted + 1; i < newCommitted; i++)
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
        /// Invoke before beginning processing of a batch. If the function returns false, the batch must not be
        /// executed to preserve DPR consistency, and the caller should return the error response (written in the
        /// response field in case of failure). Otherwise, when the function returns, the batch is safe to execute.
        /// In the true case, there must eventually be a matching SignalBatchFinish call for DPR to make
        /// progress.
        /// </summary>
        /// <param name="request">Dpr request message from user</param>
        /// <param name="response">Dpr response message that will be returned to user in case of failure</param>
        /// <param name="tracker">Tracker to use for batch execution</param>
        /// <returns>Whether the batch can be executed</returns>
        public unsafe bool RequestBatchBegin(ref DprBatchRequestHeader request, ref DprBatchResponseHeader response,
            out DprBatchVersionTracker tracker)
        {
            // Wait for worker version to catch up to largest in batch (minimum version where all operations
            // can be safely executed), taking checkpoints if necessary.
            while (request.versionLowerBound > stateObject.Version())
            {
                stateObject.BeginCheckpoint(ComputeDependency, request.versionLowerBound);
                FASTER.core.Utility.MonotonicUpdate(ref state.lastCheckpointMilli, state.sw.ElapsedMilliseconds, out _);
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
                wl = state.worldlineTracker.Enter();
            }

            // If the worker world-line is newer, the request must be rejected so the client can observe failure
            // and rollback. Populate response with this information and signal failure to upper layers so the batch
            // is not executed.
            if (request.worldLine < wl)
            {
                response.sessionId = request.sessionId;
                // Use negative to signal that there was a mismatch, which would prompt error handling on client side
                response.worldLine = -wl;
                response.batchId = request.batchId;
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
            fixed (byte* depValues = request.deps)
            {
                for (var i = 0; i < request.numDeps; i++)
                {
                    ref var wv = ref Unsafe.AsRef<WorkerVersion>(depValues + sizeof(WorkerVersion) * i);
                    deps.Update(wv.Worker, wv.Version);
                }
            }

            // Exit without releasing epoch, as protection is supposed to extend until end of batch.
            return true;
        }
        

        /// <summary>
        /// Invoke after processing of a batch is complete, and dprBatchHeader has been populated with a batch offset ->
        /// executed version mapping. This function must be invoked once for every processed batch for DPR to make
        /// progress.
        /// </summary>
        /// <param name="request">Dpr request message from user</param>
        /// <param name="response">Dpr response message that will be returned to user</param>
        /// <param name="tracker">Tracker used in batch processing</param>
        /// <returns> size of header if successful, negative size required to hold response if supplied byte span is too small</returns>
        public int SignalBatchFinish(ref DprBatchRequestHeader request, Span<byte> response,
            DprBatchVersionTracker tracker)
        {
            ref var dprResponse =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchResponseHeader>(response));
            var responseSize = DprBatchResponseHeader.HeaderSize + tracker.EncodingSize();
            if (response.Length < responseSize) return -responseSize;

            // Signal batch finished so world-lines can advance. Need to make sure not double-invoked, therefore only
            // called after size validation.
            state.worldlineTracker.Leave();

            // Populate response
            dprResponse.sessionId = request.sessionId;
            dprResponse.worldLine = request.worldLine;
            dprResponse.batchId = request.batchId;
            dprResponse.batchSize = responseSize;
            tracker.AppendOntoResponse(ref dprResponse);

            trackers.Return(tracker);
            return responseSize;
        }

        /// <summary>
        /// Force the execution of a checkpoint ahead of the schedule specified at creation time.
        ///
        /// Resets the checkpoint schedule to happen checkpoint_milli after this invocation. 
        /// </summary>
        /// <param name="targetVersion"> the version to jump to after the checkpoint</param>
        public void ForceCheckpoint(long targetVersion = -1)
        {
            stateObject.BeginCheckpoint(ComputeDependency, targetVersion);
            FASTER.core.Utility.MonotonicUpdate(ref state.lastCheckpointMilli, state.sw.ElapsedMilliseconds, out _);
        }
    }
}
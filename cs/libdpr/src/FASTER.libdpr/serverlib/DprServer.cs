using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// Per-version information needed by DPR
    /// </summary>
    /// <typeparam name="TToken">Type of token checkpoints generate</typeparam>
    internal class VersionHandle<TToken>
    {
        /// <summary>
        /// The token that uniquely identifies the checkpoint associated with this version.
        /// </summary>
        public TToken token;
        /// <summary>
        /// The set of dependencies of a version
        /// </summary>
        // TODO(Tianyu): Change to another data structure for performance?
        public HashSet<WorkerVersion> deps = new HashSet<WorkerVersion>();
    }

    /// <summary>
    /// Maintains per-worker state for DPR version-tracking.
    ///
    /// Shared between the DprManager and DprWorkerCallback. This is separate from DprManager to avoid introducing
    /// the IStateObject generic type argument to DprWorkerCallback.
    /// </summary>
    /// <typeparam name="TToken">Type of token checkpoints generate</typeparam>
    internal class DprWorkerState<TToken>
    {
        public DprWorkerState(IDprFinder dprFinder, Worker me)
        {
            this.dprFinder = dprFinder;
            this.me = me;
            // epoch = new LightEpoch();
            worldlineLatch = new ReaderWriterLockSlim();
            versions = new ConcurrentDictionary<long, VersionHandle<TToken>>();
            nextWorldLine = 0;
        }
        
        public readonly IDprFinder dprFinder;
        // TODO(Tianyu): Epoch is highly specialized for a system like FASTER. Maybe add back in later.
        // public readonly LightEpoch epoch;
        public ReaderWriterLockSlim worldlineLatch;
        public readonly ConcurrentDictionary<long, VersionHandle<TToken>> versions;
        public readonly Worker me;
        public long nextWorldLine;
    }
    
    /// <summary>
    /// DprManager is the primary interface between an underlying state object and the libDPR layer. libDPR expects
    /// to be called before and after every batch execution.
    /// </summary>
    /// <typeparam name="TStateObject"></typeparam>
    /// <typeparam name="TToken"></typeparam>
    public class DprServer<TStateObject, TToken>
        where TStateObject : IStateObject<TToken>
    {
        private DprWorkerState<TToken> state;
        private TStateObject stateObject;
        private SimpleObjectPool<DprBatchVersionTracker> trackers;
            
        private Thread refreshThread;
        private readonly long checkpointPeriodMilli;
        private ManualResetEventSlim termination;

        public DprServer(IDprFinder dprFinder, Worker me, TStateObject stateObject, long checkpointPeriodMilli)
        {
         
            state = new DprWorkerState<TToken>(dprFinder, me);
            this.stateObject = stateObject;
            trackers = new SimpleObjectPool<DprBatchVersionTracker>(() => new DprBatchVersionTracker());
            stateObject.Register(new DprWorkerCallbacks<TToken>(state));
            this.checkpointPeriodMilli = checkpointPeriodMilli;
        }

        public TStateObject StateObject() => stateObject;
        
        public void Start()
        {
            termination = new ManualResetEventSlim();
            refreshThread = new Thread(() =>
            {
                var sw = Stopwatch.StartNew();
                while (!termination.IsSet)
                {
                    var expectedVersion = sw.ElapsedMilliseconds / checkpointPeriodMilli + 1;
                    if (stateObject.Version() < expectedVersion)
                        stateObject.BeginCheckpoint(expectedVersion);
                    // TODO(Tianyu): Need to add async or otherwise frequency-limit this invocation?
                    state.dprFinder.Refresh();
                }
            });
            refreshThread.Start();
        }

        public void End()
        {
            // TODO(Tianyu): Implement leaving a DPR cluster
            termination.Set();
            refreshThread.Join();
        }

        private void TryAdvanceWorldLineTo(long targetWorldline)
        {
            // Advance world-line one-at-a-time, taking care to make sure that for each target worldline, restore
            // is called only once.
            while (Utility.MonotonicUpdate(ref state.nextWorldLine, Math.Min(state.nextWorldLine + 1, targetWorldline), out _))
                stateObject.BeginRestore(state.versions[state.dprFinder.SafeVersion(state.me)].token);
        }
        

        /// <summary>
        /// Invoke before beginning processing of a batch. If the function returns false, the batch must not be
        /// executed to preserve DPR consistency. Otherwise, when the function returns, the batch is safe to execute.
        /// In the true case, there must eventually be a matching SignalBatchFinish call for DPR to make
        /// progress.
        /// </summary>
        /// <param name="request">Dpr request message from user</param>
        /// <param name="response">Dpr response message that will be returned to user</param>
        /// <param name="tracker">Tracker to use for batch execution</param>
        /// <returns>Whether the batch can be executed</returns>
        public unsafe bool RequestBatchBegin(ReadOnlySpan<byte> request, Span<byte> response, out DprBatchVersionTracker tracker)
        {
            ref var dprRequest = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchRequestHeader>(request));
            // Wait for worker version to catch up to largest in batch (minimum version where all operations
            // can be safely executed), taking checkpoints if necessary.
            while (dprRequest.versionLowerBound > stateObject.Version())
                stateObject.BeginCheckpoint(dprRequest.versionLowerBound);

            // Enter protected region for world-lines. Because we validate requests batch-at-a-time, the world-line
            // must not shift while a batch is being processed, otherwise a message from an older world-line may be
            // processed in a new one. 
            // state.epoch.Resume();
            state.worldlineLatch.EnterReadLock();
            // If the worker world-line is behind, wait for worker to recover up to the same point as the client,
            // so client operation is not lost in a rollback that the client has already observed.
            while (dprRequest.worldLine >= state.dprFinder.SystemWorldLine())
            {
                state.worldlineLatch.ExitReadLock();
                TryAdvanceWorldLineTo(dprRequest.worldLine);
                // state.epoch.ProtectAndDrain();
                state.worldlineLatch.EnterReadLock();
            }
            
            // If the worker world-line is newer, the request must be rejected so the client can observe failure
            // and rollback. Populate response with this information and signal failure to upper layers so the batch
            // is not executed.
            if (dprRequest.worldLine < state.dprFinder.SystemWorldLine())
            {
                ref var dprResponse = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchResponseHeader>(response));
                dprResponse.sessionId = dprRequest.sessionId;
                // Use negative to signal that there was a mismatch, which would prompt error handling on client side
                dprResponse.worldLine = -state.dprFinder.SystemWorldLine();
                dprResponse.batchId = dprRequest.batchId;
                state.worldlineLatch.ExitReadLock();
                // state.epoch.Suspend();
                tracker = default;
                return false;
            }

            tracker = trackers.Checkout();
            // At this point, we are certain that the request world-line and worker world-line match, and worker
            // world-line will not advance until this thread refreshes the epoch. We can proceed to batch execution.
            Debug.Assert(dprRequest.worldLine == state.dprFinder.SystemWorldLine());
            
            // Update batch dependencies to the current worker-version. This is an over-approximation, as the batch
            // could get processed at a future version instead due to thread timing. However, this is not a correctness
            // issue, nor do we lose precision as batch-level dependency tracking is already an approximation.
            var versionHandle = state.versions.GetOrAdd(stateObject.Version(), version => new VersionHandle<TToken>());
            fixed (byte* depValues = dprRequest.deps)
            {
                for (var i = 0; i < dprRequest.numDeps; i++)
                    versionHandle.deps.Add(Unsafe.AsRef<WorkerVersion>(depValues + sizeof(Guid) * i));
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
        /// <returns> 0 if successful, size required to hold return if supplied byte span is too small</returns>
        public int SignalBatchFinish(ReadOnlySpan<byte> request, Span<byte> response, DprBatchVersionTracker tracker)
        {
            ref var dprRequest = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchRequestHeader>(request));
            ref var dprResponse = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchResponseHeader>(response));
            var responseSize = DprBatchResponseHeader.HeaderSize + tracker.EncodingSize();
            if (response.Length < responseSize) return responseSize;
            
            // Signal batch finished so world-lines can advance. Need to make sure not double-invoked, therefore only
            // called after size validation.
            // state.epoch.Suspend();
            state.worldlineLatch.ExitReadLock();
            
            // Populate response
            dprResponse.sessionId = dprRequest.sessionId;
            dprResponse.worldLine = dprRequest.worldLine;
            dprResponse.batchId = dprRequest.batchId;
            dprResponse.batchSize = responseSize;
            tracker.AppendOntoResponse(ref dprResponse);
            
            trackers.Return(tracker);
            return 0;
        }
    }
}
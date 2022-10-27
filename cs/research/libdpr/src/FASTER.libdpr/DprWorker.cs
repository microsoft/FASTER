using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// A DprWorker corresponds to an individual stateful failure domain (e.g., a physical machine or VM) in the system.
    /// DprWorkers have access to some external persistent storage and can commit and restore state through it using the
    /// StateObject implementation.
    /// </summary>
    /// <typeparam name="TStateObject"> type of state object</typeparam>
    public class DprWorker<TStateObject>
        where TStateObject : IStateObject
    {
        private readonly SimpleObjectPool<LightDependencySet> dependencySetPool;
        private readonly IDprFinder dprFinder;
        private readonly Worker me;
        private readonly ConcurrentDictionary<long, LightDependencySet> versions;
        private readonly EpochProtectedVersionScheme versionScheme;
        private long worldLine = 0;
        private long lastCheckpointMilli, lastRefreshMilli;
        private Stopwatch sw = Stopwatch.StartNew();
        private readonly TStateObject stateObject;
        private readonly byte[] depSerializationArray;
        private TaskCompletionSource<long> nextCommit;
        private List<IStateObjectAttachment> attachments = new List<IStateObjectAttachment>();
        private byte[] metadataBuffer = new byte[1 << 15];

        /// <summary>
        /// Creates a new DprServer.
        /// </summary>
        /// <param name="dprFinder"> interface to the cluster's DPR finder component </param>
        /// <param name="me"> unique id of the DPR server </param>
        /// <param name="stateObject"> underlying state object </param>
        public DprWorker(IDprFinder dprFinder, Worker me, TStateObject stateObject)
        {
            this.dprFinder = dprFinder;
            this.me = me;
            versionScheme = new EpochProtectedVersionScheme(new LightEpoch());
            versions = new ConcurrentDictionary<long, LightDependencySet>();
            dependencySetPool = new SimpleObjectPool<LightDependencySet>(() => new LightDependencySet());
            this.stateObject = stateObject;
            depSerializationArray = new byte[2 * LightDependencySet.MaxClusterSize * sizeof(long)];
            nextCommit = new TaskCompletionSource<long>();
        }

        public void AddAttachment(IStateObjectAttachment attachment)
        {
            attachments.Add(attachment);
        }

        /// <summary></summary>
        /// <returns> Worker ID of this DprServer instance </returns>
        public Worker Me() => me;

        private Task BeginRestore(long newWorldLine, long version)
        {
            var tcs = new TaskCompletionSource<object>();
            // Enforce that restore happens sequentially to prevent multiple restores to the same newWorldLine
            lock (this)
            {
                // Restoration to this particular worldline has already been completed
                if (worldLine >= newWorldLine) return Task.CompletedTask;
                
                versionScheme.TryAdvanceVersionWithCriticalSection((vOld, vNew) =>
                {
                    // Restore underlying state object state
                    stateObject.RestoreCheckpoint(version, out var metadata);
                    // Use the restored metadata to restore attachments state
                    unsafe
                    {
                        fixed (byte* src = metadata)
                        {
                            var head = src +
                                       SerializationUtil.DeserializeCheckpointMetadata(metadata, out _, out _, out _);
                            var numAttachments = *(int*)head;
                            head += sizeof(int);
                            Debug.Assert(numAttachments == attachments.Count,
                                "recovered checkpoint contains a different number of attachments!");
                            foreach (var attachment in attachments)
                            {
                                var size = *(int*)head;
                                head += sizeof(int);
                                attachment.RecoverFrom(new Span<byte>(head, size));
                                head += size;
                            }
                        }
                    }

                    // Clear any leftover state and signal complete
                    versions.Clear();
                    var deps = dependencySetPool.Checkout();
                    if (vOld != 0)
                        deps.Update(me, vOld);
                    var success = versions.TryAdd(vNew, deps);
                    Debug.Assert(success);
                    tcs.SetResult(null);

                    worldLine = newWorldLine;
                }, Math.Max(version, versionScheme.CurrentState().Version) + 1);
            }
            return tcs.Task;
        }

        private int MetadataSize(ReadOnlySpan<byte> deps)
        {
            var result = deps.Length + sizeof(int);
            foreach (var attachment in attachments)
                result += sizeof(int) + attachment.SerializedSize();
            return result;
        }

        private bool BeginCheckpoint(long targetVersion = -1)
        {
            return versionScheme.TryAdvanceVersionWithCriticalSection((vOld, vNew) =>
            {
                // Prepare checkpoint metadata
                int length;
                var deps = ComputeCheckpointMetadata(vOld);
                Debug.Assert(MetadataSize(deps) < metadataBuffer.Length);
                unsafe
                {
                    fixed (byte* dst = metadataBuffer)
                    {
                        var head = dst;
                        var end = dst + metadataBuffer.Length;
                        deps.CopyTo(new Span<byte>(head, (int)(end - head)));
                        head += deps.Length;

                        *(int*)head = attachments.Count;
                        head += sizeof(int);

                        foreach (var attachment in attachments)
                        {
                            var size = attachment.SerializedSize();
                            *(int*)head = size;
                            head += sizeof(int);
                            attachment.SerializeTo(new Span<byte>(head, (int)(end - head)));
                            head += size;
                        }

                        length = (int)(head - dst);
                    }
                }

                // Perform checkpoint with a callback to report persistence and clean-up leftover tracking state
                stateObject.PerformCheckpoint(vOld, new Span<byte>(metadataBuffer, 0, length), () =>
                {
                    versions.TryRemove(vOld, out var deps);
                    var workerVersion = new WorkerVersion(me, vOld);
                    dprFinder.ReportNewPersistentVersion(worldLine, workerVersion, deps);
                    dependencySetPool.Return(deps);
                });

                // Prepare new version before any operations can occur in it
                var newDeps = dependencySetPool.Checkout();
                if (vOld != 0) newDeps.Update(me, vOld);
                var success = versions.TryAdd(vNew, newDeps);
                Debug.Assert(success);
            }, targetVersion) == StateMachineExecutionStatus.OK;
        }

        /// <summary>
        /// At the start (restart) of processing, connect to the rest of the DPR cluster. If the worker restarted from
        /// an existing instance, the cluster will detect this and trigger rollback as appropriate across the cluster,
        /// and the worker will automatically load the correct checkpointed state for recovery. Must be invoked exactly
        /// once before any other operations. 
        /// </summary>
        public void ConnectToCluster()
        {
            var v = dprFinder.NewWorker(me, stateObject);
            // This worker is recovering from some failure and we need to load said checkpoint
            if (v != 0)
                BeginRestore(dprFinder.SystemWorldLine(), v).GetAwaiter().GetResult();
            dprFinder.Refresh();
        }

        /// <summary></summary>
        /// <returns> The underlying state object </returns>
        public TStateObject StateObject()
        {
            return stateObject;
        }

        private ReadOnlySpan<byte> ComputeCheckpointMetadata(long version)
        {
            var deps = versions[version];
            var size = SerializationUtil.SerializeCheckpointMetadata(depSerializationArray,
                worldLine, new WorkerVersion(me, version), deps);
            Debug.Assert(size > 0);
            return new ReadOnlySpan<byte>(depSerializationArray, 0, size);
        }

        /// <summary>
        /// Check whether this DprWorker is due to refresh its view of the cluster or need to perform a checkpoint ---
        /// if so, perform the relevant operation(s). This method must be called regularly to ensure cluster progress in
        /// the general case. This method automatically filters redundant calls and roughly ensures that both only
        /// happen as frequently as the given intervals for checkpoint and refresh. Thread-safe with other methods but
        /// not safe to be called concurrently from multiple threads.
        /// </summary>
        /// <param name="checkpointPeriodMilli">The rough interval, in milliseconds, expected between two checkpoints</param>
        /// <param name="refreshPeriodMilli">The rough interval, in milliseconds, expected between two refreshes</param>
        public void TryRefreshAndCheckpoint(long checkpointPeriodMilli, long refreshPeriodMilli)
        {
            var currentTime = sw.ElapsedMilliseconds;
            var lastCommitted = dprFinder.SafeVersion(me);

            if (dprFinder != null && lastRefreshMilli + refreshPeriodMilli < currentTime)
            {
                // A false return indicates that the DPR finder does not have a cut available, this is usually due to
                // restart from crash, at which point we should resend the graph 
                if (!dprFinder.Refresh())
                    dprFinder.ResendGraph(me, stateObject);
                core.Utility.MonotonicUpdate(ref lastRefreshMilli, currentTime, out _);
                if (worldLine != dprFinder.SystemWorldLine())
                    BeginRestore(dprFinder.SystemWorldLine(), dprFinder.SafeVersion(me)).GetAwaiter().GetResult(); 
            }

            if (lastCheckpointMilli + checkpointPeriodMilli <= currentTime)
            {
                BeginCheckpoint(
                    Math.Max(versionScheme.CurrentState().Version + 1, dprFinder.GlobalMaxVersion()));

                core.Utility.MonotonicUpdate(ref lastCheckpointMilli, currentTime, out _);
            }

            // Can prune dependency information of committed versions
            var newCommitted = dprFinder.SafeVersion(me);
            if (lastCommitted != newCommitted)
            {
                var oldTask = nextCommit;
                nextCommit = new TaskCompletionSource<long>();
                oldTask.SetResult(newCommitted);
            }

            for (var i = lastCommitted; i < newCommitted; i++)
                stateObject.PruneVersion(i);
        }

        /// <summary>
        /// When processing of messages fails to begin (because of DPR violations), compose an error DPR header
        /// to the sending DPR entity
        /// </summary>
        /// <param name="inputHeader">the original input DPR header that failed</param>
        /// <param name="outputHeaderBytes"> the response message to populate</param>
        public void ComposeErrorResponse(ReadOnlySpan<byte> inputHeader, Span<byte> outputHeaderBytes)
        {
            if (outputHeaderBytes.Length < DprBatchHeader.FixedLenSize)
                throw new FasterException(
                    "header given was too small to fit DPR response, need to be at least DprBatchHeader.FixedLenSize");
            var cast = MemoryMarshal.Cast<byte, DprBatchHeader>(inputHeader);
            ref readonly var request = ref MemoryMarshal.GetReference(cast);

            ref var responseObj =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchHeader>(outputHeaderBytes));
            responseObj.srcWorkerId = Me();
            // Use negative to signal that there was a mismatch, which would prompt error handling on client side
            // Must be negative to distinguish from a normal response message in current version
            responseObj.worldLine = -worldLine;
        }

        /// <summary>
        ///     Invoke before beginning processing of a locally started batch (e.g., serving a long running subscription
        ///     client or sending a server-to-server message, not in response to a client request) There must eventually
        ///     be a matching SignalLocalBatchFinish call on the same thread for DPR to make progress. Only one batch
        ///     is allowed to be active on a thread at a given time. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeginProcessing()
        {
            versionScheme.Enter();
        }
        
        /// <summary>
        /// Invoke before beginning processing of a batch received from a remote worker/client. If the function
        /// returns false, the batch must not be executed to preserve DPR consistency (caller may respond with an
        /// error message using ComposeErrorResponse). Otherwise, when the function returns, the batch is safe to
        /// execute. In the true case, there must eventually be a matching call to finish processing on the same
        /// thread for DPR to make progress. Only one batch is allowed to be active on a thread at a given time. 
        /// </summary>
        /// <param name="inputHeaderBytes">Dpr request message from user</param>
        /// <returns>Whether the batch can be executed</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool ReceiveAndBeginProcessing(ReadOnlySpan<byte> inputHeaderBytes)
        {
            var cast = MemoryMarshal.Cast<byte, DprBatchHeader>(inputHeaderBytes);
            ref readonly var inputHeader = ref MemoryMarshal.GetReference(cast);

            // Wait for worker version to catch up to largest in batch (minimum version where all operations
            // can be safely executed), taking checkpoints if necessary.
            while (inputHeader.version > versionScheme.CurrentState().Version)
            {
                if (BeginCheckpoint(inputHeader.version))
                    core.Utility.MonotonicUpdate(ref lastCheckpointMilli, sw.ElapsedMilliseconds, out _);
                Thread.Yield();
            }

            // Enter protected region. Because we validate requests batch-at-a-time, the world-line must not shift while
            // a batch is being processed, otherwise a message from an older world-line may be processed in a new one.
            versionScheme.Enter();
            // If the worker world-line is behind, wait for worker to recover up to the same point as the client,
            // so client operation is not lost in a rollback that the client has already observed.
            while (inputHeader.worldLine > worldLine)
            {
                versionScheme.Leave();
                BeginRestore(inputHeader.worldLine, dprFinder.SafeVersion(me)).GetAwaiter().GetResult();
                Thread.Yield();
                versionScheme.Enter();
            }

            // If the worker world-line is newer, the request must be rejected. 
            if (inputHeader.worldLine < worldLine)
            {
                versionScheme.Leave();
                return false;
            }
            
            // Update batch dependencies to the current worker-version. This is an over-approximation, as the batch
            // could get processed at a future version instead due to thread timing. However, this is not a correctness
            // issue, nor do we lose much precision as batch-level dependency tracking is already an approximation.
            var deps = versions[versionScheme.CurrentState().Version];
            if (!inputHeader.srcWorkerId.Equals(Worker.INVALID))
                deps.Update(inputHeader.srcWorkerId, inputHeader.version);
            fixed (byte* d = inputHeader.data)
            {
                var depsHead = d + inputHeader.ClientDepsOffset;
                for (var i = 0; i < inputHeader.numClientDeps; i++)
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
        /// Invoke after processing of a remote batch is complete. This function must be invoked after beginning
        /// a remote batch on the same thread for DPR to make progress.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FinishProcessing()
        {
            versionScheme.Leave();
        }

        /// <summary>
        /// Invoke after processing of a batch is complete and populate the given span with a DPR header for any
        /// outgoing messages processing mean need to send. This function must be invoked successfully after beginning
        /// a batch on the same thread for DPR to make progress.
        /// </summary>
        /// <param name="outputHeaderBytes">Dpr response message to be populated</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FinishProcessingAndSend(Span<byte> outputHeaderBytes)
        {
            if (outputHeaderBytes.Length < DprBatchHeader.FixedLenSize)
                throw new FasterException(
                    "header given was too small to fit DPR response, need to be at least DprBatchHeader.FixedLenSize");            
            ref var outputHeader = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchHeader>(outputHeaderBytes));

            var wl = worldLine;
            // Signal batch finished so world-lines can advance. Need to make sure not double-invoked, therefore only
            // called after size validation.
            versionScheme.Leave();

            // Populate response
            outputHeader.srcWorkerId = Me();
            outputHeader.worldLine = wl;
            outputHeader.version = versionScheme.CurrentState().Version;
            outputHeader.numClientDeps = 0;
        }

        /// <summary>
        ///     Force the execution of a checkpoint ahead of the schedule specified at creation time.
        ///     Resets the checkpoint schedule to happen checkpoint_milli after this invocation.
        /// </summary>
        /// <param name="targetVersion"> the version to jump to after the checkpoint, or -1 for the immediate next version</param>
        public void ForceCheckpoint(long targetVersion = -1)
        {
            if (BeginCheckpoint(targetVersion))
                core.Utility.MonotonicUpdate(ref lastCheckpointMilli, sw.ElapsedMilliseconds, out _);
        }
    }
}
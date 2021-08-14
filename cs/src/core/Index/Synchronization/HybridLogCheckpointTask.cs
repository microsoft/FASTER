using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// This task is the base class for a checkpoint "backend", which decides how a captured version is
    /// persisted on disk.
    /// </summary>
    internal abstract class HybridLogCheckpointOrchestrationTask : ISynchronizationTask
    {
        private long lastVersion;
        /// <inheritdoc />
        public virtual void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.PREPARE:
                    lastVersion = faster.systemState.version;
                    if (faster._hybridLogCheckpoint.IsDefault())
                    {
                        faster._hybridLogCheckpointToken = Guid.NewGuid();
                        faster.InitializeHybridLogCheckpoint(faster._hybridLogCheckpointToken, next.version);
                    }
                    faster._hybridLogCheckpoint.info.version = next.version;
                    faster.ObtainCurrentTailAddress(ref faster._hybridLogCheckpoint.info.startLogicalAddress);
                    break;
                case Phase.WAIT_FLUSH:
                    faster._hybridLogCheckpoint.info.headAddress = faster.hlog.HeadAddress;
                    faster._hybridLogCheckpoint.info.beginAddress = faster.hlog.BeginAddress;
                    faster._hybridLogCheckpoint.info.nextVersion = next.version;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    CollectMetadata(next, faster);
                    faster.WriteHybridLogMetaInfo();
                    faster.lastVersion = lastVersion;
                    break;
                case Phase.REST:
                    faster._hybridLogCheckpoint.Reset();
                    var nextTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    faster.checkpointTcs.SetResult(new LinkedCheckpointInfo { NextTask = nextTcs.Task });
                    faster.checkpointTcs = nextTcs;
                    break;
            }
        }

        protected void CollectMetadata<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            // Collect object log offsets only after flushes
            // are completed
            var seg = faster.hlog.GetSegmentOffsets();
            if (seg != null)
            {
                faster._hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                Array.Copy(seg, faster._hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
            }

            // Temporarily block new sessions from starting, which may add an entry to the table and resize the
            // dictionary. There should be minimal contention here.
            lock (faster._activeSessions)
                // write dormant sessions to checkpoint
                foreach (var kvp in faster._activeSessions)
                    kvp.Value.AtomicSwitch(next.version - 1);
        }

        /// <inheritdoc />
        public virtual void GlobalAfterEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {
        }

        /// <inheritdoc />
        public virtual void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession : IFasterSession
        {
            if (current.phase != Phase.PERSISTENCE_CALLBACK) return;

            if (ctx != null)
            {
                if (!ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                {
                    faster.IssueCompletionCallback(ctx, fasterSession);
                    ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                }

                faster.epoch.Mark(EpochPhaseIdx.CheckpointCompletionCallback, current.version);
            }

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, current.version))
                faster.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A FoldOver checkpoint persists a version by setting the read-only marker past the last entry of that
    /// version on the log and waiting until it is flushed to disk. It is simple and fast, but can result
    /// in garbage entries on the log, and a slower recovery of performance.
    /// </summary>
    internal sealed class FoldOverCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
        {
            base.GlobalBeforeEnteringState(next, faster);

            if (next.phase == Phase.PREPARE)
            {
                faster._lastSnapshotCheckpoint.deltaFileDevice?.Dispose();
                faster._lastSnapshotCheckpoint.deltaLog?.Dispose();
                faster._lastSnapshotCheckpoint = default;
            }
            if (next.phase != Phase.WAIT_FLUSH) return;

            faster.hlog.ShiftReadOnlyToTail(out var tailAddress,
                out faster._hybridLogCheckpoint.flushedSemaphore);
            faster._hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);

            if (current.phase != Phase.WAIT_FLUSH) return;

            if (ctx == null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = faster._hybridLogCheckpoint.flushedSemaphore;

                var notify = faster.hlog.FlushedUntilAddress >= faster._hybridLogCheckpoint.info.finalLogicalAddress;
                notify = notify || !faster.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx != null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            if (ctx != null)
                faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.version);

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.version))
                faster.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A Snapshot persists a version by making a copy for every entry of that version separate from the log. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class SnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        private long lastVersion;
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.PREPARE:
                    faster._lastSnapshotCheckpoint.deltaFileDevice?.Dispose();
                    faster._lastSnapshotCheckpoint.deltaLog?.Dispose();
                    faster._lastSnapshotCheckpoint = default;
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._hybridLogCheckpoint.info.startLogicalAddress = faster.hlog.FlushedUntilAddress;
                    faster._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, faster);
                    faster.ObtainCurrentTailAddress(ref faster._hybridLogCheckpoint.info.finalLogicalAddress);
                    faster._hybridLogCheckpoint.info.snapshotFinalLogicalAddress = faster._hybridLogCheckpoint.info.finalLogicalAddress;

                    faster._hybridLogCheckpoint.snapshotFileDevice =
                        faster.checkpointManager.GetSnapshotLogDevice(faster._hybridLogCheckpointToken);
                    faster._hybridLogCheckpoint.snapshotFileObjectLogDevice =
                        faster.checkpointManager.GetSnapshotObjectLogDevice(faster._hybridLogCheckpointToken);
                    faster._hybridLogCheckpoint.snapshotFileDevice.Initialize(faster.hlog.GetSegmentSize());
                    faster._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(-1);

                    long startPage = faster.hlog.GetPage(faster._hybridLogCheckpoint.info.startLogicalAddress);
                    long endPage = faster.hlog.GetPage(faster._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (faster._hybridLogCheckpoint.info.finalLogicalAddress >
                        faster.hlog.GetStartLogicalAddress(endPage))
                    {
                        endPage++;
                    }

                    // We are writing pages outside epoch protection, so callee should be able to
                    // handle corrupted or unexpected concurrent page changes during the flush, e.g., by
                    // resuming epoch protection if necessary. Correctness is not affected as we will
                    // only read safe pages during recovery.
                    faster.hlog.AsyncFlushPagesToDevice(
                        startPage,
                        endPage,
                        faster._hybridLogCheckpoint.info.finalLogicalAddress,
                        faster._hybridLogCheckpoint.snapshotFileDevice,
                        faster._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        out faster._hybridLogCheckpoint.flushedSemaphore);
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    faster.lastVersion = 
                    // update flushed-until address to the latest
                    faster._hybridLogCheckpoint.info.flushedLogicalAddress = faster.hlog.FlushedUntilAddress;
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._lastSnapshotCheckpoint = faster._hybridLogCheckpoint;
                    break;
                default:
                    base.GlobalBeforeEnteringState(next, faster);
                    break;
            }
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);

            if (current.phase != Phase.WAIT_FLUSH) return;

            if (ctx == null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = faster._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !faster.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx != null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            if (ctx != null)
                faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.version);

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.version))
                faster.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// A Incremental Snapshot makes a copy of only changes that have happened since the last full Snapshot. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal sealed class IncrementalSnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.PREPARE:
                    faster._hybridLogCheckpoint = faster._lastSnapshotCheckpoint;
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._hybridLogCheckpoint.info.startLogicalAddress = faster.hlog.FlushedUntilAddress;
                    faster._hybridLogCheckpoint.prevVersion = next.version;
                    break;
                case Phase.WAIT_FLUSH:
                    base.GlobalBeforeEnteringState(next, faster);
                    faster._hybridLogCheckpoint.info.finalLogicalAddress = 0;
                    faster.ObtainCurrentTailAddress(ref faster._hybridLogCheckpoint.info.finalLogicalAddress);

                    if (faster._hybridLogCheckpoint.deltaLog == null)
                    {
                        faster._hybridLogCheckpoint.deltaFileDevice = faster.checkpointManager.GetDeltaLogDevice(faster._hybridLogCheckpointToken);
                        faster._hybridLogCheckpoint.deltaFileDevice.Initialize(-1);
                        faster._hybridLogCheckpoint.deltaLog = new DeltaLog(faster._hybridLogCheckpoint.deltaFileDevice, faster.hlog.LogPageSizeBits, -1);
                        faster._hybridLogCheckpoint.deltaLog.InitializeForWrites(faster.hlog.bufferPool);
                    }

                    faster.hlog.AsyncFlushDeltaToDevice(
                        faster._hybridLogCheckpoint.info.startLogicalAddress,
                        faster._hybridLogCheckpoint.info.finalLogicalAddress,
                        faster._lastSnapshotCheckpoint.info.finalLogicalAddress,
                        faster._hybridLogCheckpoint.prevVersion,
                        faster._hybridLogCheckpoint.deltaLog);
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    faster._hybridLogCheckpoint.info.flushedLogicalAddress = faster.hlog.FlushedUntilAddress;
                    CollectMetadata(next, faster);
                    faster.WriteHybridLogIncrementalMetaInfo(faster._hybridLogCheckpoint.deltaLog);
                    faster._hybridLogCheckpoint.info.deltaTailAddress = faster._hybridLogCheckpoint.deltaLog.TailAddress;
                    faster._lastSnapshotCheckpoint = faster._hybridLogCheckpoint;
                    break;
            }
        }

        /// <inheritdoc />
        public override void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
        {
            base.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);

            if (current.phase != Phase.WAIT_FLUSH) return;

            if (ctx == null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var s = faster._hybridLogCheckpoint.flushedSemaphore;

                var notify = s != null && s.CurrentCount > 0;
                notify = notify || !faster.SameCycle(ctx, current) || s == null;

                if (valueTasks != null && !notify)
                {
                    Debug.Assert(s != null);
                    valueTasks.Add(new ValueTask(s.WaitAsync(token).ContinueWith(t => s.Release())));
                }

                if (!notify) return;

                if (ctx != null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            if (ctx != null)
                faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.version);

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.version))
                faster.GlobalStateMachineStep(current);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    internal class HybridLogCheckpointStateMachine : VersionChangeStateMachine
    {
        /// <summary>
        /// Construct a new HybridLogCheckpointStateMachine to use the given checkpoint backend (either fold-over or
        /// snapshot), drawing boundary at targetVersion.
        /// </summary>
        /// <param name="checkpointBackend">A task that encapsulates the logic to persist the checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public HybridLogCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1)
            : base(targetVersion, new VersionChangeTask(), checkpointBackend) { }

        /// <summary>
        /// Construct a new HybridLogCheckpointStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected HybridLogCheckpointStateMachine(long targetVersion, params ISynchronizationTask[] tasks)
            : base(targetVersion, tasks) { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.phase)
            {
                case Phase.WAIT_PENDING:
                    result.phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    result.phase = Phase.PERSISTENCE_CALLBACK;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    result.phase = Phase.REST;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}
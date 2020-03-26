using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32;

namespace FASTER.core
{
    public class HybridLogCheckpointOrchestrationTask : ISynchronizationTask
    {
        public void GlobalBeforeEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            // TODO(Tianyu): The cast makes this setup not as extensible as one would expect, where are templates
            // when you need them....
            if (!(stateMachine is HybridLogCheckpointStateMachine checkpointStateMachine))
                throw new ArgumentException();

            switch (next.phase)
            {
                case Phase.PREPARE:
                    checkpointStateMachine.checkpointToken = Guid.NewGuid();
                    checkpointStateMachine.checkpoint.Initialize(checkpointStateMachine.checkpointToken, next.version,
                        faster.checkpointManager);
                    faster.ObtainCurrentTailAddress(ref checkpointStateMachine.checkpoint.info.startLogicalAddress);
                    break;
                case Phase.WAIT_FLUSH:
                    checkpointStateMachine.checkpoint.info.headAddress = faster.hlog.HeadAddress;
                    checkpointStateMachine.checkpoint.info.beginAddress = faster.hlog.BeginAddress;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    // Collect object log offsets only after flushes
                    // are completed
                    var seg = faster.hlog.GetSegmentOffsets();
                    if (seg != null)
                    {
                        checkpointStateMachine.checkpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                        Array.Copy(seg, checkpointStateMachine.checkpoint.info.objectLogSegmentOffsets, seg.Length);
                    }

                    if (faster._activeSessions != null)
                    {
                        // write dormant sessions to checkpoint
                        foreach (var kvp in faster._activeSessions)
                        {
                            faster.AtomicSwitch(kvp.Value.ctx, kvp.Value.ctx.prevCtx, next.version - 1);
                        }
                    }

                    faster.checkpointManager.CommitLogCheckpoint(checkpointStateMachine.checkpointToken,
                        checkpointStateMachine.checkpoint.info.ToByteArray());
                    break;
                case Phase.REST:
                    var nextTcs =
                        new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions
                            .RunContinuationsAsynchronously);
                    faster.checkpointTcs.SetResult(new LinkedCheckpointInfo {NextTask = nextTcs.Task});
                    faster.checkpointTcs = nextTcs;
                    break;
            }
        }

        public void GlobalAfterEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        public ValueTask OnThreadEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState entering,
            SystemState prev, FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (entering.phase != Phase.PERSISTENCE_CALLBACK) return default;

            if (ctx != null)
            {
                if (!ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                {
                    if (ctx.prevCtx.serialNum != -1)
                    {
                        var commitPoint = new CommitPoint
                        {
                            UntilSerialNo = ctx.prevCtx.serialNum,
                            ExcludedSerialNos = ctx.prevCtx.excludedSerialNos
                        };

                        // Thread local action
                        faster.functions.CheckpointCompletionCallback(ctx.guid, commitPoint);
                        if (clientSession != null) clientSession.LatestCommitPoint = commitPoint;
                    }

                    ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                }

                faster.epoch.Mark(EpochPhaseIdx.CheckpointCompletionCallback, entering.version);
            }

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, entering.version))
                faster.GlobalStateMachineStep(entering);
            return default;
        }
    }

    public class FoldOverCheckpointTask : ISynchronizationTask
    {
        public void GlobalBeforeEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (!(stateMachine is HybridLogCheckpointStateMachine checkpointStateMachine))
                throw new ArgumentException();
            if (next.phase != Phase.WAIT_FLUSH) return;

            faster.hlog.ShiftReadOnlyToTail(out var tailAddress,
                out checkpointStateMachine.checkpoint.flushedSemaphore);
            checkpointStateMachine.checkpoint.info.finalLogicalAddress = tailAddress;
        }

        public void GlobalAfterEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        public async ValueTask OnThreadEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState entering,
            SystemState prev, FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (!(stateMachine is HybridLogCheckpointStateMachine checkpointStateMachine))
                throw new ArgumentException();
            if (entering.phase != Phase.WAIT_FLUSH) return;

            if (ctx == null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                bool notify;

                notify = (faster.hlog.FlushedUntilAddress >=
                          checkpointStateMachine.checkpoint.info.finalLogicalAddress);

                if (async && !notify)
                {
                    Debug.Assert(checkpointStateMachine.checkpoint.flushedSemaphore != null);
                    clientSession?.UnsafeSuspendThread();
                    await checkpointStateMachine.checkpoint.flushedSemaphore.WaitAsync(token);
                    clientSession?.UnsafeResumeThread();
                    checkpointStateMachine.checkpoint.flushedSemaphore.Release();
                    notify = true;
                }


                if (notify)
                {
                    if (ctx != null)
                        ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
                }
            }

            if (ctx != null)
                faster.epoch.Mark(EpochPhaseIdx.WaitFlush, entering.version);

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, entering.version))
                faster.GlobalStateMachineStep(entering);
        }
    }

    public class HybridLogCheckpointStateMachine : VersionChangeStateMachine
    {
        internal Guid checkpointToken = default;
        internal HybridLogCheckpointInfo checkpoint = default;

        public HybridLogCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1) : base(
            targetVersion, new VersionChangeTask(), new HybridLogCheckpointOrchestrationTask(), checkpointBackend)
        {
        }

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
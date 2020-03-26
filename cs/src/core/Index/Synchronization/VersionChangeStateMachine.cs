using System;
using System.ComponentModel;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class VersionChangeTask : ISynchronizationTask
    {
        public void GlobalBeforeEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        public void GlobalAfterEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState start,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        public ValueTask OnThreadEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState entering, SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            bool async = true,
            CancellationToken token = default)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            switch (entering.phase)
            {
                case Phase.PREPARE:
                    if (ctx != null)
                    {
                        if (!ctx.markers[EpochPhaseIdx.Prepare])
                        {
                            if (!faster.RelaxedCPR)
                                faster.AcquireSharedLatchesForAllPendingRequests(ctx);
                            ctx.markers[EpochPhaseIdx.Prepare] = true;
                        }

                        faster.epoch.Mark(EpochPhaseIdx.Prepare, entering.version);
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.Prepare, entering.version))
                        faster.GlobalStateMachineStep(entering);
                    break;
                case Phase.IN_PROGRESS:
                    if (ctx != null)
                    {
                        // Need to be very careful here as threadCtx is changing
                        var _ctx = prev.phase == Phase.IN_PROGRESS ? ctx.prevCtx : ctx;

                        if (!_ctx.markers[EpochPhaseIdx.InProgress])
                        {
                            faster.AtomicSwitch(ctx, ctx.prevCtx, _ctx.version);
                            faster.InitContext(ctx, ctx.prevCtx.guid, ctx.prevCtx.serialNum);

                            // Has to be prevCtx, not ctx
                            ctx.prevCtx.markers[EpochPhaseIdx.InProgress] = true;
                        }

                        faster.epoch.Mark(EpochPhaseIdx.InProgress, entering.version);
                    }

                    // Has to be prevCtx, not ctx
                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.InProgress, entering.version))
                        faster.GlobalStateMachineStep(entering);
                    break;
                case Phase.WAIT_PENDING:
                    if (ctx != null)
                    {
                        if (!ctx.prevCtx.markers[EpochPhaseIdx.WaitPending])
                        {
                            if (ctx.prevCtx.HasNoPendingRequests)
                                ctx.prevCtx.markers[EpochPhaseIdx.WaitPending] = true;
                            else
                                break;
                        }

                        faster.epoch.Mark(EpochPhaseIdx.WaitPending, entering.version);
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitPending, entering.version))
                        faster.GlobalStateMachineStep(entering);
                    break;
            }

            return default;
        }
    }

    public class FoldOverTask : ISynchronizationTask
    {
        public void GlobalBeforeEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (next.phase == Phase.REST)
                // Before leaving the checkpoint, make sure all previous versions are read-only.
                faster.hlog.ShiftReadOnlyToTail(out _, out _);
        }

        public void GlobalAfterEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context> { }

        public ValueTask OnThreadEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState entering,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            return default;
        }
    }

    public class VersionChangeStateMachine : SynchronizationStateMachineBase
    {
        private readonly long targetVersion;

        protected VersionChangeStateMachine(long targetVersion = -1, params ISynchronizationTask[] tasks) : base(tasks)
        {
            this.targetVersion = targetVersion;
        }

        public VersionChangeStateMachine(long targetVersion = -1) : this(targetVersion, new VersionChangeTask(), new FoldOverTask()) { }

        public override SystemState NextState(SystemState start)
        {
            var nextState = SystemState.Copy(ref start);
            switch (start.phase)
            {
                case Phase.REST:
                    nextState.phase = Phase.PREPARE;
                    break;
                case Phase.PREPARE:
                    nextState.phase = Phase.IN_PROGRESS;
                    // TODO(Tianyu): Move to long for system state as well. 
                    nextState.version = (int) (targetVersion == -1 ? start.version + 1 : targetVersion + 1);
                    break;
                case Phase.IN_PROGRESS:
                    // This phase has no effect if using relaxed CPR model
                    nextState.phase = Phase.WAIT_PENDING;
                    break;
                case Phase.WAIT_PENDING:
                    nextState.phase = Phase.REST;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return nextState;
        }
    }
}
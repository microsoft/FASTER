using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// A Version change captures a version on the log by forcing all threads to coordinate a move to the next
    /// version. It is used as the basis of many other tasks, which decides what they do with the captured
    /// version.
    /// </summary>
    internal sealed class VersionChangeTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState start,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        /// <inheritdoc />
        public ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(
            SystemState current, SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            switch (current.phase)
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

                        faster.epoch.Mark(EpochPhaseIdx.Prepare, current.version);
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.Prepare, current.version))
                        faster.GlobalStateMachineStep(current);
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

                        faster.epoch.Mark(EpochPhaseIdx.InProgress, current.version);
                    }

                    // Has to be prevCtx, not ctx
                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.InProgress, current.version))
                        faster.GlobalStateMachineStep(current);
                    break;
                case Phase.WAIT_PENDING:
                    if (ctx != null)
                    {
                        if (!faster.RelaxedCPR &&!ctx.prevCtx.markers[EpochPhaseIdx.WaitPending])
                        {
                            if (ctx.prevCtx.HasNoPendingRequests)
                                ctx.prevCtx.markers[EpochPhaseIdx.WaitPending] = true;
                            else
                                break;
                        }

                        faster.epoch.Mark(EpochPhaseIdx.WaitPending, current.version);
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitPending, current.version))
                        faster.GlobalStateMachineStep(current);
                    break;
                case Phase.REST:
                    break;
            }

            return default;
        }
    }

    /// <summary>
    /// The FoldOver task simply sets the read only offset to the current end of the log, so a captured version
    /// is immutable and will eventually be flushed to disk.
    /// </summary>
    internal sealed class FoldOverTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (next.phase == Phase.REST)
                // Before leaving the checkpoint, make sure all previous versions are read-only.
                faster.hlog.ShiftReadOnlyToTail(out _, out _);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context> { }

        /// <inheritdoc />
        public ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            return default;
        }
    }

    /// <summary>
    /// A VersionChangeStateMachine orchestrates to capture a version, but does not flush to disk.
    /// </summary>
    internal class VersionChangeStateMachine : SynchronizationStateMachineBase
    {
        private readonly long targetVersion;
        
        /// <summary>
        /// Construct a new VersionChangeStateMachine with the given tasks. Does not load any tasks by default.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <param name="tasks">The tasks to load onto the state machine</param>
        protected VersionChangeStateMachine(long targetVersion = -1, params ISynchronizationTask[] tasks) : base(tasks)
        {
            this.targetVersion = targetVersion;
        }

        /// <summary>
        /// Construct a new VersionChangeStateMachine that folds over the log at the end without waiting for flush. 
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public VersionChangeStateMachine(long targetVersion = -1) : this(targetVersion, new VersionChangeTask(), new FoldOverTask()) { }

        /// <inheritdoc />
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
                    // TODO: Move to long for system state as well. 
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
                    throw new FasterException("Invalid Enum Argument");
            }

            return nextState;
        }
    }
}
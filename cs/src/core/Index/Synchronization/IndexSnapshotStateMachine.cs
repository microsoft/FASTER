using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// This task performs an index checkpoint.
    /// </summary>
    internal class IndexSnapshotTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            switch (next.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    if (faster._indexCheckpointToken == default)
                    {
                        faster._indexCheckpointToken = Guid.NewGuid();
                        faster.InitializeIndexCheckpoint(faster._indexCheckpointToken);
                    }

                    faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.startLogicalAddress);
                    break;
                case Phase.INDEX_CHECKPOINT:
                    if (faster.UseReadCache && faster.ReadCache.BeginAddress != faster.ReadCache.TailAddress)
                        throw new FasterException("Index checkpoint with read cache is not supported");
                    faster.TakeIndexFuzzyCheckpoint();
                    break;
                    
                case Phase.REST:
                    // If the tail address has already been obtained, because another task on the state machine
                    // has done so earlier (e.g. FullCheckpoint captures log tail at WAIT_FLUSH), don't update
                    // the tail address.
                    if (faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.finalLogicalAddress))
                        faster._indexCheckpoint.info.num_buckets = faster.overflowBucketsAllocator.GetMaxValidAddress();
                    if (faster._indexCheckpointToken != default)
                    {
                        faster.WriteIndexMetaInfo();
                        faster._indexCheckpointToken = default;
                        var nextTcs = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                        faster.checkpointTcs.SetResult(new LinkedCheckpointInfo { NextTask = nextTcs.Task });
                        faster.checkpointTcs = nextTcs;
                    }
                    faster._indexCheckpoint.Reset();

                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        /// <inheritdoc />
        public async ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            switch (current.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    if (ctx != null)
                    {
                        if (!ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt])
                            ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = true;
                        faster.epoch.Mark(EpochPhaseIdx.PrepareForIndexCheckpt, current.version);
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.PrepareForIndexCheckpt, current.version))
                        faster.GlobalStateMachineStep(current);
                    break;
                case Phase.INDEX_CHECKPOINT:
                    if (ctx != null)
                    {
                        // Resetting the marker for a potential FULL or INDEX_ONLY checkpoint in the future
                        ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = false;
                    }

                    if (async && !faster.IsIndexFuzzyCheckpointCompleted())
                    {
                        clientSession?.UnsafeSuspendThread();
                        await faster.IsIndexFuzzyCheckpointCompletedAsync(token);
                        clientSession?.UnsafeResumeThread();
                    }

                    faster.GlobalStateMachineStep(current);
                    break;
            }
        }
    }

    /// <summary>
    /// This state machine performs an index checkpoint
    /// </summary>
    internal class IndexSnapshotStateMachine : SynchronizationStateMachineBase
    {
        /// <summary>
        /// Create a new IndexSnapshotStateMachine
        /// </summary>
        public IndexSnapshotStateMachine() : base(new IndexSnapshotTask())
        {
        }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.phase)
            {
                case Phase.REST:
                    result.phase = Phase.PREP_INDEX_CHECKPOINT;
                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    result.phase = Phase.INDEX_CHECKPOINT;
                    break;
                case Phase.INDEX_CHECKPOINT:
                    result.phase = Phase.REST;
                    result.version++;
                    break;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }

            return result;
        }
    }
}
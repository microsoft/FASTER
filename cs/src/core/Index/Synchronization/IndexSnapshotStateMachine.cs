using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class IndexSnapshotTask : ISynchronizationTask
    {
        public void GlobalBeforeEnteringState<T, Key, Value, Input, Output, Context, Functions>(T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            switch (next.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    faster._indexCheckpointToken = Guid.NewGuid();
                    faster.InitializeIndexCheckpoint(faster._indexCheckpointToken);
                    faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.startLogicalAddress);
                    break;
                case Phase.INDEX_CHECKPOINT:
                    if (faster.UseReadCache && faster.ReadCache.BeginAddress != faster.ReadCache.TailAddress)
                        throw new FasterException("Index checkpoint with read cache is not supported");
                    faster.TakeIndexFuzzyCheckpoint();
                    break;
                case Phase.REST:
                    faster._indexCheckpoint.info.num_buckets = faster.overflowBucketsAllocator.GetMaxValidAddress();
                    faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.finalLogicalAddress);
                    faster.WriteIndexMetaInfo();
                    faster._indexCheckpoint.Reset();
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
            switch (entering.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    if (ctx != null)
                    {
                        if (!ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt])
                            ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = true;
                        faster.epoch.Mark(EpochPhaseIdx.PrepareForIndexCheckpt, entering.version);
                    }

                    if (faster.epoch.CheckIsComplete(EpochPhaseIdx.PrepareForIndexCheckpt, entering.version))
                        faster.GlobalStateMachineStep(entering);
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

                    faster.GlobalStateMachineStep(entering);
                    break;
            }
        }
    }

    public class IndexSnapshotStateMachine : SynchronizationStateMachineBase
    {
        public IndexSnapshotStateMachine() : base(new IndexSnapshotTask()) {}
        
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
                    // TODO(Tianyu): What's the logic behind increasing the version here?
                    result.version++;
                    break;
                default:
                    throw new InvalidEnumArgumentException();
            }

            return result;
        }
    }
}
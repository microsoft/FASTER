using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class FullCheckpointOrchestrationTask : ISynchronizationTask
    {
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
                    Debug.Assert(faster._indexCheckpointToken == default &&
                                 faster._hybridLogCheckpointToken == default);
                    var fullCheckpointToken = Guid.NewGuid();
                    faster._indexCheckpointToken = fullCheckpointToken;
                    faster._hybridLogCheckpointToken = fullCheckpointToken;
                    faster.InitializeIndexCheckpoint(faster._indexCheckpointToken);
                    faster.InitializeHybridLogCheckpoint(faster._hybridLogCheckpointToken, next.version);
                    break;
                case Phase.PREPARE:
                    if (faster.UseReadCache && faster.ReadCache.BeginAddress != faster.ReadCache.TailAddress)
                        throw new FasterException("Index checkpoint with read cache is not supported");
                    faster.TakeIndexFuzzyCheckpoint();
                    break;
                case Phase.WAIT_FLUSH:
                    faster._indexCheckpoint.info.num_buckets = faster.overflowBucketsAllocator.GetMaxValidAddress();
                    faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.finalLogicalAddress);
                    break;
            }
        }

        public void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
        }

        public async ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(SystemState current,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default) where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (current.phase != Phase.WAIT_INDEX_CHECKPOINT) return;

            if (async && !faster.IsIndexFuzzyCheckpointCompleted())
            {
                clientSession?.UnsafeSuspendThread();
                await faster.IsIndexFuzzyCheckpointCompletedAsync(token);
                clientSession?.UnsafeResumeThread();
            }

            faster.GlobalStateMachineStep(current);
        }
    }

    public class FullCheckpointStateMachine : HybridLogCheckpointStateMachine
    {
        public FullCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1) : base(
            targetVersion, new VersionChangeTask(), new FullCheckpointOrchestrationTask(), checkpointBackend,
            new IndexSnapshotTask()) {}

        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.phase)
            {
                case Phase.REST:
                    result.phase = Phase.PREP_INDEX_CHECKPOINT;
                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    result.phase = Phase.PREPARE;
                    break;
                case Phase.WAIT_FLUSH:
                    result.phase = Phase.WAIT_INDEX_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                    result.phase = Phase.PERSISTENCE_CALLBACK;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}
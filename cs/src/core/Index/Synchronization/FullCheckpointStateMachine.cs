using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// This task contains logic to orchestrate the index and hybrid log checkpoint in parallel
    /// </summary>
    internal sealed class FullCheckpointOrchestrationTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value, Input, Output, Context>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context> faster)
            where Key : new()
            where Value : new()
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
                case Phase.PERSISTENCE_CALLBACK:
                    faster.WriteIndexMetaInfo();
                    faster._indexCheckpointToken = default;
                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value, Input, Output, Context>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context> faster)
            where Key : new()
            where Value : new()
        {
        }

        /// <inheritdoc />
        public async ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(SystemState current,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context> faster,
            FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext ctx,
            Functions functions,
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

        /// <inheritdoc />
        public async ValueTask OnThreadState<Key, Value, Input, Output, Context>(SystemState current,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context> faster,
            CancellationToken token = default) where Key : new()
            where Value : new()
        {
            if (current.phase != Phase.WAIT_INDEX_CHECKPOINT) return;

            if (!faster.IsIndexFuzzyCheckpointCompleted())
            {
                await faster.IsIndexFuzzyCheckpointCompletedAsync(token);
            }

            faster.GlobalStateMachineStep(current);
        }
    }
    
    /// <summary>
    /// The state machine orchestrates a full checkpoint
    /// </summary>
    internal sealed class FullCheckpointStateMachine : HybridLogCheckpointStateMachine
    {
        /// <summary>
        /// Construct a new FullCheckpointStateMachine to use the given checkpoint backend (either fold-over or snapshot),
        /// drawing boundary at targetVersion.
        /// </summary>
        /// <param name="checkpointBackend">A task that encapsulates the logic to persist the checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public FullCheckpointStateMachine(ISynchronizationTask checkpointBackend, long targetVersion = -1) : base(
            targetVersion, new VersionChangeTask(), new FullCheckpointOrchestrationTask(), checkpointBackend,
            new IndexSnapshotTask()) {}

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
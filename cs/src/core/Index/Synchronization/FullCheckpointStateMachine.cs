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
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
            where Key : new()
            where Value : new()
        {
            switch (next.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    Debug.Assert(faster._indexCheckpoint.IsDefault() &&
                                 faster._hybridLogCheckpoint.IsDefault());
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
                    faster._indexCheckpoint.Reset();
                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
            where Key : new()
            where Value : new()
        {
        }

        /// <inheritdoc />
        public async ValueTask OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            bool async = true,
            CancellationToken token = default) where Key : new()
            where Value : new()
            where FasterSession : IFasterSession
        {
            if (current.phase != Phase.WAIT_INDEX_CHECKPOINT) return;

            if (async && !faster.IsIndexFuzzyCheckpointCompleted())
            {
                fasterSession?.UnsafeSuspendThread();
                await faster.IsIndexFuzzyCheckpointCompletedAsync(token);
                fasterSession?.UnsafeResumeThread();
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
            new IndexSnapshotTask())
        { }

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
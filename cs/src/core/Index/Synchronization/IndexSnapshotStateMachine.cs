using System;
using System.Collections.Generic;
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
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    if (faster._indexCheckpoint.IsDefault())
                    {
                        faster._indexCheckpointToken = Guid.NewGuid();
                        faster.InitializeIndexCheckpoint(faster._indexCheckpointToken);
                    }

                    faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.startLogicalAddress);
                    if (faster.UseReadCache && faster.ReadCache.BeginAddress != faster.ReadCache.TailAddress)
                        throw new FasterException("Index checkpoint with read cache is not supported");
                    faster.TakeIndexFuzzyCheckpoint();
                    break;

                case Phase.WAIT_INDEX_CHECKPOINT:
                    break;
                    
                case Phase.REST:
                    // If the tail address has already been obtained, because another task on the state machine
                    // has done so earlier (e.g. FullCheckpoint captures log tail at WAIT_FLUSH), don't update
                    // the tail address.
                    if (faster.ObtainCurrentTailAddress(ref faster._indexCheckpoint.info.finalLogicalAddress))
                        faster._indexCheckpoint.info.num_buckets = faster.overflowBucketsAllocator.GetMaxValidAddress();
                    if (!faster._indexCheckpoint.IsDefault())
                    {
                        faster.WriteIndexMetaInfo();
                        faster._indexCheckpoint.Reset();
                    }

                    break;
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
        }

        /// <inheritdoc />
        public void OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev, 
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession : IFasterSession
        {
            switch (current.phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    faster.GlobalStateMachineStep(current);
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                    var notify = faster.IsIndexFuzzyCheckpointCompleted();
                    notify = notify || !faster.SameCycle(current);

                    if (valueTasks != null && !notify)
                    {
                        var t = faster.IsIndexFuzzyCheckpointCompletedAsync(token);
                        if (!faster.SameCycle(current))
                            notify = true;
                        else
                            valueTasks.Add(t);
                    }

                    if (!notify) return;
                    faster.GlobalStateMachineStep(current);
                    break;
            }
        }
    }

    /// <summary>
    /// This state machine performs an index checkpoint
    /// </summary>
    internal sealed class IndexSnapshotStateMachine : SynchronizationStateMachineBase
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
                    result.phase = Phase.WAIT_INDEX_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                    result.phase = Phase.REST;
                    break;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }

            return result;
        }
    }
}
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Resizes an index
    /// </summary>
    internal sealed class IndexResizeTask : ISynchronizationTask
    {
        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.PREPARE_GROW:
                    // nothing to do
                    break;
                case Phase.IN_PROGRESS_GROW:
                    // Set up the transition to new version of HT
                    var numChunks = (int) (faster.state[faster.resizeInfo.version].size / Constants.kSizeofChunk);
                    if (numChunks == 0) numChunks = 1; // at least one chunk

                    faster.numPendingChunksToBeSplit = numChunks;
                    faster.splitStatus = new long[numChunks];
                    faster.overflowBucketsAllocatorResize = faster.overflowBucketsAllocator;
                    faster.overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>(false);
                    faster.Initialize(1 - faster.resizeInfo.version, faster.state[faster.resizeInfo.version].size * 2, faster.sectorSize);

                    faster.resizeInfo.version = 1 - faster.resizeInfo.version;
                    break;
                case Phase.REST:
                    // nothing to do
                    break;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
        {
            switch (next.phase)
            {
                case Phase.PREPARE_GROW:
                    faster.epoch.BumpCurrentEpoch(() => faster.GlobalStateMachineStep(next));
                    break;
                case Phase.IN_PROGRESS_GROW:
                case Phase.REST:
                    // nothing to do
                    break;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }
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
                case Phase.PREPARE_GROW:
                case Phase.IN_PROGRESS_GROW:
                case Phase.REST:
                    return;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }
        }
    }

    /// <summary>
    /// Resizes the index
    /// </summary>
    internal sealed class IndexResizeStateMachine : SynchronizationStateMachineBase
    {
        /// <summary>
        /// Constructs a new IndexResizeStateMachine
        /// </summary>
        public IndexResizeStateMachine() : base(new IndexResizeTask()) {}

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var nextState = SystemState.Copy(ref start);
            switch (start.phase)
            {
                case Phase.REST:
                    nextState.phase = Phase.PREPARE_GROW;
                    break;
                case Phase.PREPARE_GROW:
                    nextState.phase = Phase.IN_PROGRESS_GROW;
                    break;
                case Phase.IN_PROGRESS_GROW:
                    nextState.phase = Phase.REST;
                    break;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }

            return nextState;
        }
    }
}
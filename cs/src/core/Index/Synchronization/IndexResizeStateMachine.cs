using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class IndexResizeTask : ISynchronizationTask
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

                    faster.Initialize(1 - faster.resizeInfo.version, faster.state[faster.resizeInfo.version].size * 2,
                        faster.sectorSize);

                    faster.resizeInfo.version = 1 - faster.resizeInfo.version;
                    break;
                case Phase.REST:
                    // nothing to do
                    break;
                default:
                    throw new InvalidEnumArgumentException();
            }
        }

        public void GlobalAfterEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where T : ISynchronizationStateMachine
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
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
                    throw new InvalidEnumArgumentException();
            }
        }

        public ValueTask OnThreadEnteringState<T, Key, Value, Input, Output, Context, Functions>(
            T stateMachine,
            SystemState entering,
            SystemState prev,
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
                case Phase.PREPARE_GROW:
                case Phase.IN_PROGRESS_GROW:
                case Phase.REST:
                    return default;
                default:
                    throw new InvalidEnumArgumentException();
            }
        }
    }

    public class IndexResizeStateMachine : SynchronizationStateMachineBase
    {
        public IndexResizeStateMachine() : base(new IndexResizeTask()) {}
        
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
                    throw new InvalidEnumArgumentException();
            }

            return nextState;
        }
    }
}
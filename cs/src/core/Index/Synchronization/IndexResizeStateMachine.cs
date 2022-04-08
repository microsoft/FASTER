// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
        public void GlobalBeforeEnteringState<Key, Value, StoreFunctions>(
            SystemState next,
            FasterKV<Key, Value, StoreFunctions> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
        {
            switch (next.Phase)
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
                    faster.overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>();
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
        public void GlobalAfterEnteringState<Key, Value, StoreFunctions>(
            SystemState next,
            FasterKV<Key, Value, StoreFunctions> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
        {
            switch (next.Phase)
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
        public void OnThreadState<Key, Value, Input, Output, Context, FasterSession, StoreFunctions>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value, StoreFunctions> faster,
            FasterKV<Key, Value, StoreFunctions>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession : IFasterSession
            where StoreFunctions : IStoreFunctions<Key, Value>
        {
            switch (current.Phase)
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
            switch (start.Phase)
            {
                case Phase.REST:
                    nextState.Phase = Phase.PREPARE_GROW;
                    break;
                case Phase.PREPARE_GROW:
                    nextState.Phase = Phase.IN_PROGRESS_GROW;
                    break;
                case Phase.IN_PROGRESS_GROW:
                    nextState.Phase = Phase.REST;
                    break;
                default:
                    throw new FasterException("Invalid Enum Argument");
            }

            return nextState;
        }
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
        {
            switch (next.Phase)
            {
                case Phase.PREP_INDEX_CHECKPOINT:
                    Debug.Assert(faster._indexCheckpoint.IsDefault() &&
                                 faster._hybridLogCheckpoint.IsDefault());
                    var fullCheckpointToken = Guid.NewGuid();
                    faster._indexCheckpointToken = fullCheckpointToken;
                    faster._hybridLogCheckpointToken = fullCheckpointToken;
                    faster.InitializeIndexCheckpoint(faster._indexCheckpointToken);
                    faster.InitializeHybridLogCheckpoint(faster._hybridLogCheckpointToken, next.Version);
                    break;
                case Phase.WAIT_FLUSH:
                    faster._indexCheckpoint.info.num_buckets = faster.overflowBucketsAllocator.GetMaxValidAddress();
                    faster._indexCheckpoint.info.finalLogicalAddress = faster.hlog.GetTailAddress();
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
            targetVersion, new VersionChangeTask(), new FullCheckpointOrchestrationTask(), 
            new IndexSnapshotTask(), checkpointBackend)
        { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    result.Phase = Phase.PREP_INDEX_CHECKPOINT;
                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    result.Phase = Phase.PREPARE;
                    break;
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_INDEX_CHECKPOINT;
                    break;
                case Phase.WAIT_INDEX_CHECKPOINT:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}
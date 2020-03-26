// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

//#define WAIT_FOR_INDEX_CHECKPOINT

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Linked list (chain) of checkpoint info
    /// </summary>
    public struct LinkedCheckpointInfo
    {
        /// <summary>
        /// Next task in checkpoint chain
        /// </summary>
        public Task<LinkedCheckpointInfo> NextTask;
    }
    
    internal class EpochPhaseIdx
    {
        public const int PrepareForIndexCheckpt = 0;

        public const int Prepare = 1;

        public const int InProgress = 2;

        public const int WaitPending = 3;

        public const int WaitFlush = 4;

        public const int CheckpointCompletionCallback = 5;
    }


    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase,
        IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal TaskCompletionSource<LinkedCheckpointInfo> checkpointTcs
            = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task<LinkedCheckpointInfo> CheckpointTask => checkpointTcs.Task;


        #region Starting points

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SynchronizeThreads(CheckpointType type)
        {
            if (_systemState.phase == Phase.REST)
            {
                var context = (long) type;
                var currentState = SystemState.Make(Phase.REST, _systemState.version);
                var nextState = GetNextState(currentState, type);
                return GlobalMoveToNextState(currentState, nextState, ref context);
            }
            else
            {
                return false;
            }
        }
        #endregion

        /// <summary>
        /// Global transition function that coordinates various state machines. 
        /// A few characteristics about the state machine:
        /// <list type="bullet">
        /// <item>
        /// <description>
        /// Transitions happen atomically using a compare-and-swap operation. So, multiple threads can try to do the same transition. Only one will succeed.
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// Transition from state A to B happens via an intermediate state (INTERMEDIATE). This serves as a lock by a thread to perform the transition. 
        /// Some transitions are accompanied by actions that must be performed before the transitions such as initializing contexts, etc. 
        /// </description>
        /// </item>
        /// <item>
        /// <description>
        /// States can be part of multiple state machines. For example: PREP_INDEX_CHECKPOINT is part of both index-only and full checkpoints. 
        /// </description>
        /// </item>
        /// </list>
        /// 
        /// We currently support 4 different state machines:
        /// <list type="number">
        /// <item>
        /// <term> Index-Only Checkpoint </term>
        /// <description> REST -> PREP_INDEX_CHECKPOINT -> INDEX_CHECKPOINT -> REST </description>
        /// </item>
        /// <item>
        /// <term>HybridLog-Only Checkpoint</term>
        /// <description>REST -> PREPARE -> IN_PROGRESS -> WAIT_PENDING -> WAIT_FLUSH -> PERSISTENCE_CALLBACK -> REST</description>
        /// </item>
        /// <item>
        /// <term>Full Checkpoint</term>
        /// <description>REST -> PREP_INDEX_CHECKPOINT -> PREPARE -> IN_PROGRESS -> WAIT_PENDING -> WAIT_FLUSH -> PERSISTENCE_CALLBACK -> REST</description>
        /// </item>
        /// <item>
        /// <term>Grow</term>
        /// <description></description>
        /// </item>
        /// </list>
        /// </summary>
        /// <param name="currentState">from state of the transition.</param>
        /// <param name="nextState">to state of the transition.</param>
        /// <param name="context">optional additioanl parameter for transition.</param>
        /// <returns>true if transition succeeds.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GlobalMoveToNextState(SystemState currentState, SystemState nextState, ref long context)
        {
            var intermediateState = SystemState.Make(Phase.INTERMEDIATE, currentState.version);

            // Move from S1 to I
            if (MakeTransition(currentState, intermediateState))
            {
                // Acquired ownership to make the transition from S1 to S2
                switch (nextState.phase)
                {
                    case Phase.PREP_INDEX_CHECKPOINT:
                    {
                        _checkpointType = (CheckpointType) context;

                        switch (_checkpointType)
                        {
                            case CheckpointType.INDEX_ONLY:
                            {
                                _indexCheckpointToken = Guid.NewGuid();
                                InitializeIndexCheckpoint(_indexCheckpointToken);
                                break;
                            }
                            case CheckpointType.FULL:
                            {
                                var fullCheckpointToken = Guid.NewGuid();
                                _indexCheckpointToken = fullCheckpointToken;
                                _hybridLogCheckpointToken = fullCheckpointToken;
                                InitializeIndexCheckpoint(_indexCheckpointToken);
                                InitializeHybridLogCheckpoint(_hybridLogCheckpointToken, currentState.version);
                                break;
                            }
                            default:
                                throw new FasterException();
                        }

                        ObtainCurrentTailAddress(ref _indexCheckpoint.info.startLogicalAddress);

                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.INDEX_CHECKPOINT:
                    {
                        if (UseReadCache && this.ReadCache.BeginAddress != this.ReadCache.TailAddress)
                        {
                            throw new FasterException("Index checkpoint with read cache is not supported");
                        }

                        TakeIndexFuzzyCheckpoint();

                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.PREPARE:
                    {
                        switch (currentState.phase)
                        {
                            case Phase.REST:
                            {
                                _checkpointType = (CheckpointType) context;

                                Debug.Assert(_checkpointType == CheckpointType.HYBRID_LOG_ONLY);
                                _hybridLogCheckpointToken = Guid.NewGuid();
                                InitializeHybridLogCheckpoint(_hybridLogCheckpointToken, currentState.version);
                                break;
                            }
                            case Phase.PREP_INDEX_CHECKPOINT:
                            {
                                if (UseReadCache && this.ReadCache.BeginAddress != this.ReadCache.TailAddress)
                                {
                                    throw new FasterException("Index checkpoint with read cache is not supported");
                                }

                                TakeIndexFuzzyCheckpoint();
                                break;
                            }
                            default:
                                throw new FasterException();
                        }

                        ObtainCurrentTailAddress(ref _hybridLogCheckpoint.info.startLogicalAddress);

                        if (!FoldOverSnapshot)
                        {
                            _hybridLogCheckpoint.info.flushedLogicalAddress = hlog.FlushedUntilAddress;
                            _hybridLogCheckpoint.info.useSnapshotFile = 1;
                        }

                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.IN_PROGRESS:
                    {
                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.WAIT_PENDING:
                    {
                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.WAIT_FLUSH:
                    {
                        if (_checkpointType == CheckpointType.FULL)
                        {
                            _indexCheckpoint.info.num_buckets = overflowBucketsAllocator.GetMaxValidAddress();
                            ObtainCurrentTailAddress(ref _indexCheckpoint.info.finalLogicalAddress);
                        }

                        _hybridLogCheckpoint.info.headAddress = hlog.HeadAddress;
                        _hybridLogCheckpoint.info.beginAddress = hlog.BeginAddress;

                        if (FoldOverSnapshot)
                        {
                            hlog.ShiftReadOnlyToTail(out long tailAddress, out _hybridLogCheckpoint.flushedSemaphore);
                            _hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
                        }
                        else
                        {
                            ObtainCurrentTailAddress(ref _hybridLogCheckpoint.info.finalLogicalAddress);

                            _hybridLogCheckpoint.snapshotFileDevice =
                                checkpointManager.GetSnapshotLogDevice(_hybridLogCheckpointToken);
                            _hybridLogCheckpoint.snapshotFileObjectLogDevice =
                                checkpointManager.GetSnapshotObjectLogDevice(_hybridLogCheckpointToken);
                            _hybridLogCheckpoint.snapshotFileDevice.Initialize(hlog.GetSegmentSize());
                            _hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(-1);

                            long startPage = hlog.GetPage(_hybridLogCheckpoint.info.flushedLogicalAddress);
                            long endPage = hlog.GetPage(_hybridLogCheckpoint.info.finalLogicalAddress);
                            if (_hybridLogCheckpoint.info.finalLogicalAddress > hlog.GetStartLogicalAddress(endPage))
                            {
                                endPage++;
                            }

                            // This can be run on a new thread if we want to immediately parallelize 
                            // the rest of the log flush
                            hlog.AsyncFlushPagesToDevice(
                                startPage,
                                endPage,
                                _hybridLogCheckpoint.info.finalLogicalAddress,
                                _hybridLogCheckpoint.snapshotFileDevice,
                                _hybridLogCheckpoint.snapshotFileObjectLogDevice,
                                out _hybridLogCheckpoint.flushedSemaphore);
                        }
                        
                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.PERSISTENCE_CALLBACK:
                    {
                        // Collect object log offsets only after flushes
                        // are completed
                        var seg = hlog.GetSegmentOffsets();
                        if (seg != null)
                        {
                            _hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                            Array.Copy(seg, _hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
                        }

                        if (_activeSessions != null)
                        {
                            // write dormant sessions to checkpoint
                            foreach (var kvp in _activeSessions)
                            {
                                AtomicSwitch(kvp.Value.ctx, kvp.Value.ctx.prevCtx, currentState.version - 1);
                            }
                        }

                        WriteHybridLogMetaInfo();

                        if (_checkpointType == CheckpointType.FULL)
                            WriteIndexMetaInfo();

                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.PREPARE_GROW:
                    {
                        // Note that the transition must be done before bumping epoch here!
                        MakeTransition(intermediateState, nextState);
                        epoch.BumpCurrentEpoch(() =>
                        {
                            long _context = 0;
                            GlobalMoveToNextState(nextState,
                                SystemState.Make(Phase.IN_PROGRESS_GROW, nextState.version), ref _context);
                        });
                        break;
                    }
                    case Phase.IN_PROGRESS_GROW:
                    {
                        // Set up the transition to new version of HT
                        int numChunks = (int) (state[resizeInfo.version].size / Constants.kSizeofChunk);
                        if (numChunks == 0) numChunks = 1; // at least one chunk

                        numPendingChunksToBeSplit = numChunks;
                        splitStatus = new long[numChunks];

                        Initialize(1 - resizeInfo.version, state[resizeInfo.version].size * 2, sectorSize);

                        resizeInfo.version = 1 - resizeInfo.version;

                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                    case Phase.REST:
                    {
                        switch (_checkpointType)
                        {
                            case CheckpointType.INDEX_ONLY:
                            {
                                _indexCheckpoint.info.num_buckets = overflowBucketsAllocator.GetMaxValidAddress();
                                ObtainCurrentTailAddress(ref _indexCheckpoint.info.finalLogicalAddress);
                                WriteIndexMetaInfo();
                                _indexCheckpoint.Reset();
                                break;
                            }
                            case CheckpointType.FULL:
                            {
                                _indexCheckpoint.Reset();
                                _hybridLogCheckpoint.Reset();
                                break;
                            }
                            case CheckpointType.HYBRID_LOG_ONLY:
                            {
                                _hybridLogCheckpoint.Reset();
                                break;
                            }
                            default:
                                throw new FasterException();
                        }

                        var nextTcs =
                            new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions
                                .RunContinuationsAsynchronously);
                        checkpointTcs.SetResult(new LinkedCheckpointInfo {NextTask = nextTcs.Task});
                        checkpointTcs = nextTcs;

                        MakeTransition(intermediateState, nextState);
                        break;
                    }
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Corresponding thread-local actions that must be performed when any state machine is active
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void HandleCheckpointingPhases(FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            var _ = HandleCheckpointingPhasesAsync(ctx, clientSession, false);
            return;
        }

        #region Helper functions

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GlobalMoveToNextCheckpointState(SystemState currentState)
        {
            long context = 0;
            return GlobalMoveToNextState(currentState, GetNextState(currentState, _checkpointType), ref context);
        }

        internal void AcquireSharedLatchesForAllPendingRequests(FasterExecutionContext ctx)
        {
            foreach (var _ctx in ctx.retryRequests)
            {
                AcquireSharedLatch(_ctx.key.Get());
            }

            foreach (var _ctx in ctx.ioPendingRequests.Values)
            {
                AcquireSharedLatch(_ctx.key.Get());
            }
        }

        /*
         * We have several state machines supported by this function.
         * Full Checkpoint:
         * REST -> PREP_INDEX_CHECKPOINT -> PREPARE -> IN_PROGRESS 
         *      -> WAIT_PENDING -> WAIT_FLUSH -> PERSISTENCE_CALLBACK -> REST
         * 
         * Index Checkpoint:
         * REST -> PREP_INDEX_CHECKPOINT -> INDEX_CHECKPOINT -> REST
         * 
         * Hybrid Log Checkpoint:
         * REST -> PREPARE -> IN_PROGRESS -> WAIT_PENDING -> WAIT_FLUSH ->
         *      -> PERSISTENCE_CALLBACK -> REST
         *      
         * Grow :
         * REST -> PREPARE_GROW -> IN_PROGRESS_GROW -> REST
         * 
         * GC: 
         * REST -> GC -> REST
         */
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SystemState GetNextState(SystemState start, CheckpointType type = CheckpointType.FULL)
        {
            var nextState = default(SystemState);
            nextState.word = start.word;
            switch (start.phase)
            {
                case Phase.REST:
                    switch (type)
                    {
                        case CheckpointType.HYBRID_LOG_ONLY:
                        case CheckpointType.SYNC_ONLY:
                            nextState.phase = Phase.PREPARE;
                            break;
                        case CheckpointType.FULL:
                        case CheckpointType.INDEX_ONLY:
                            nextState.phase = Phase.PREP_INDEX_CHECKPOINT;
                            break;
                    }

                    break;
                case Phase.PREP_INDEX_CHECKPOINT:
                    switch (type)
                    {
                        case CheckpointType.INDEX_ONLY:
                            nextState.phase = Phase.INDEX_CHECKPOINT;
                            break;
                        case CheckpointType.FULL:
                            nextState.phase = Phase.PREPARE;
                            break;
                    }

                    break;
                case Phase.INDEX_CHECKPOINT:
                    switch (type)
                    {
                        case CheckpointType.FULL:
                            nextState.phase = Phase.PREPARE;
                            break;
                        default:
                            nextState.phase = Phase.REST;
                            nextState.version = start.version + 1;
                            break;
                    }

                    break;
                case Phase.PREPARE:
                    nextState.phase = Phase.IN_PROGRESS;
                    nextState.version = start.version + 1;
                    break;
                case Phase.IN_PROGRESS:
                    if (type == CheckpointType.SYNC_ONLY)
                        nextState.phase = RelaxedCPR ? Phase.REST : Phase.WAIT_PENDING;
                    else
                        nextState.phase = RelaxedCPR ? Phase.WAIT_FLUSH : Phase.WAIT_PENDING;
                    break;
                case Phase.WAIT_PENDING:
                    if (type == CheckpointType.SYNC_ONLY)
                        nextState.phase = Phase.REST;
                    else
                        nextState.phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    nextState.phase = Phase.PERSISTENCE_CALLBACK;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    nextState.phase = Phase.REST;
                    break;
                case Phase.PREPARE_GROW:
                    nextState.phase = Phase.IN_PROGRESS_GROW;
                    break;
                case Phase.IN_PROGRESS_GROW:
                    nextState.phase = Phase.REST;
                    break;
            }

            return nextState;
        }

        private void WriteHybridLogMetaInfo()
        {
            checkpointManager.CommitLogCheckpoint(_hybridLogCheckpointToken, _hybridLogCheckpoint.info.ToByteArray());
        }

        internal void WriteIndexMetaInfo()
        {
            checkpointManager.CommitIndexCheckpoint(_indexCheckpointToken, _indexCheckpoint.info.ToByteArray());
        }

        internal bool ObtainCurrentTailAddress(ref long location)
        {
            var tailAddress = hlog.GetTailAddress();
            return Interlocked.CompareExchange(ref location, tailAddress, 0) == 0;
        }

        internal void InitializeIndexCheckpoint(Guid indexToken)
        {
            _indexCheckpoint.Initialize(indexToken, state[resizeInfo.version].size, checkpointManager);
        }

        internal void InitializeHybridLogCheckpoint(Guid hybridLogToken, int version)
        {
            _hybridLogCheckpoint.Initialize(hybridLogToken, version, checkpointManager);
        }

        #endregion
    }
}
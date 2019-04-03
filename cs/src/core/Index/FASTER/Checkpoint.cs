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

namespace FASTER.core
{

    /// <summary>
    /// Checkpoint related function of FASTER
    /// </summary>
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private class EpochPhaseIdx
        {
            public const int PrepareForIndexCheckpt = 0;

            public const int Prepare = 1;

            public const int InProgress = 2;

            public const int WaitPending = 3;

            public const int WaitFlush = 4;

            public const int CheckpointCompletionCallback = 5;
        }

        #region Starting points
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool InternalTakeCheckpoint(CheckpointType type)
        {
            if (_systemState.phase == Phase.GC)
            {
                Debug.WriteLine("Forcing completion of GC");
                GarbageCollectBuckets(0, true);
            }

            if (_systemState.phase == Phase.REST)
            {
                var context = (long)type;
                var currentState = SystemState.Make(Phase.REST, _systemState.version);
                var nextState = GetNextState(currentState, type);
                return GlobalMoveToNextState(currentState, nextState, ref context);
            }
            else
            {
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool InternalGrowIndex()
        {
            if (_systemState.phase == Phase.GC)
            {
                Debug.WriteLine("Forcing completion of GC");
                GarbageCollectBuckets(0, true);
            }

            if (_systemState.phase == Phase.REST)
            {
                var version = _systemState.version;
                long context = 0;
                SystemState nextState = SystemState.Make(Phase.PREPARE_GROW, version);
                if (GlobalMoveToNextState(SystemState.Make(Phase.REST, version), nextState, ref context))
                {
                    return true;
                }
            }

            return false;
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
        /// We currently support 5 different state machines:
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
        /// <term>GC</term>
        /// <description></description>
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
                            _checkpointType = (CheckpointType)context;
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
                                    throw new Exception();
                            }

                            ObtainCurrentTailAddress(ref _indexCheckpoint.info.startLogicalAddress);

                            MakeTransition(intermediateState, nextState);
                            break;
                        }
                    case Phase.INDEX_CHECKPOINT:
                        {
                            if (UseReadCache)
                            {
                                throw new Exception("Index checkpoint with read cache is not supported");
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
                                        _checkpointType = (CheckpointType)context;
                                        Debug.Assert(_checkpointType == CheckpointType.HYBRID_LOG_ONLY);
                                        _hybridLogCheckpointToken = Guid.NewGuid();
                                        InitializeHybridLogCheckpoint(_hybridLogCheckpointToken, currentState.version);
                                        break;
                                    }
                                case Phase.PREP_INDEX_CHECKPOINT:
                                    {
                                        if (UseReadCache)
                                        {
                                            throw new Exception("Index checkpoint with read cache is not supported");
                                        }
                                        TakeIndexFuzzyCheckpoint();
                                        break;
                                    }
                                default:
                                    throw new Exception();
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
                            var seg = hlog.GetSegmentOffsets();
                            if (seg != null)
                            {
                                _hybridLogCheckpoint.info.objectLogSegmentOffsets = new long[seg.Length];
                                Array.Copy(seg, _hybridLogCheckpoint.info.objectLogSegmentOffsets, seg.Length);
                            }
                            MakeTransition(intermediateState, nextState);
                            break;
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (_checkpointType == CheckpointType.FULL)
                            {
                                _indexCheckpoint.info.num_buckets = overflowBucketsAllocator.GetMaxValidAddress();
                                ObtainCurrentTailAddress(ref _indexCheckpoint.info.finalLogicalAddress);
                                WriteIndexMetaFile();
                                WriteIndexCheckpointCompleteFile();
                            }

                            if (FoldOverSnapshot)
                            {
                                hlog.ShiftReadOnlyToTail(out long tailAddress);

                                _hybridLogCheckpoint.info.finalLogicalAddress = tailAddress;
                            }
                            else
                            {
                                ObtainCurrentTailAddress(ref _hybridLogCheckpoint.info.finalLogicalAddress);

                                _hybridLogCheckpoint.snapshotFileDevice = Devices.CreateLogDevice
                                    (directoryConfiguration.GetHybridLogCheckpointFileName(_hybridLogCheckpointToken), false);
                                _hybridLogCheckpoint.snapshotFileObjectLogDevice = Devices.CreateLogDevice
                                    (directoryConfiguration.GetHybridLogObjectCheckpointFileName(_hybridLogCheckpointToken), false);
                                _hybridLogCheckpoint.snapshotFileDevice.Initialize(hlog.GetSegmentSize());
                                _hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(hlog.GetSegmentSize());

                                long startPage = hlog.GetPage(_hybridLogCheckpoint.info.flushedLogicalAddress);
                                long endPage = hlog.GetPage(_hybridLogCheckpoint.info.finalLogicalAddress);
                                if (_hybridLogCheckpoint.info.finalLogicalAddress > hlog.GetStartLogicalAddress(endPage))
                                {
                                    endPage++;
                                }

                                // This can be run on a new thread if we want to immediately parallelize 
                                // the rest of the log flush
                                hlog.AsyncFlushPagesToDevice(startPage,
                                                                endPage,
                                                                _hybridLogCheckpoint.info.finalLogicalAddress,
                                                                _hybridLogCheckpoint.snapshotFileDevice,
                                                                _hybridLogCheckpoint.snapshotFileObjectLogDevice,
                                                                out _hybridLogCheckpoint.flushed);
                            }

                            
                            MakeTransition(intermediateState, nextState);
                            break;
                        }
                    case Phase.PERSISTENCE_CALLBACK:
                        {
                            WriteHybridLogMetaInfo();
                            WriteHybridLogCheckpointCompleteFile();
                            MakeTransition(intermediateState, nextState);
                            break;
                        }
                    case Phase.GC:
                        {
                            hlog.ShiftBeginAddress(context);

                            int numChunks = (int)(state[resizeInfo.version].size / Constants.kSizeofChunk);
                            if (numChunks == 0) numChunks = 1; // at least one chunk

                            numPendingChunksToBeGCed = numChunks;
                            gcStatus = new long[numChunks];

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
                                GlobalMoveToNextState(nextState, SystemState.Make(Phase.IN_PROGRESS_GROW, nextState.version), ref _context);
                            });
                            break;
                        }
                    case Phase.IN_PROGRESS_GROW:
                        {
                            // Set up the transition to new version of HT
                            int numChunks = (int)(state[resizeInfo.version].size / Constants.kSizeofChunk);
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
                                        WriteIndexMetaFile();
                                        WriteIndexCheckpointCompleteFile();
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
                                case CheckpointType.NONE:
                                    break;
                                default:
                                    throw new Exception();
                            }

                            _checkpointType = CheckpointType.NONE;

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
        private void HandleCheckpointingPhases()
        {
            var previousState = SystemState.Make(threadCtx.Value.phase, threadCtx.Value.version);
            var finalState = SystemState.Copy(ref _systemState);

            // Don't play around when system state is being changed
            if (finalState.phase == Phase.INTERMEDIATE)
            {
                return;
            }

            // We need to move from previousState to finalState one step at a time
            do
            {
                var currentState = default(SystemState);
                if (previousState.word == finalState.word)
                {
                    currentState.word = previousState.word;
                }
                else
                {
                    currentState = GetNextState(previousState, _checkpointType);
                }

                switch (currentState.phase)
                {
                    case Phase.PREP_INDEX_CHECKPOINT:
                        {
                            if (!threadCtx.Value.markers[EpochPhaseIdx.PrepareForIndexCheckpt])
                            {
                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.PrepareForIndexCheckpt, threadCtx.Value.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
                                }
                                threadCtx.Value.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = true;
                            }
                            break;
                        }
                    case Phase.INDEX_CHECKPOINT:
                        {
                            if (_checkpointType == CheckpointType.INDEX_ONLY)
                            {
                                // Reseting the marker for a potential FULL or INDEX_ONLY checkpoint in the future
                                threadCtx.Value.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = false;
                            }

                            if (IsIndexFuzzyCheckpointCompleted())
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }
                            break;
                        }
                    case Phase.PREPARE:
                        {
                            if (!threadCtx.Value.markers[EpochPhaseIdx.Prepare])
                            {
                                // Thread local action
                                AcquireSharedLatchesForAllPendingRequests();

                                var idx = Interlocked.Increment(ref _hybridLogCheckpoint.info.numThreads);
                                idx -= 1;

                                _hybridLogCheckpoint.info.guids[idx] = threadCtx.Value.guid;

                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.Prepare, threadCtx.Value.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
                                }

                                threadCtx.Value.markers[EpochPhaseIdx.Prepare] = true;
                            }
                            break;
                        }
                    case Phase.IN_PROGRESS:
                        {
                            // Need to be very careful here as threadCtx is changing
                            FasterExecutionContext ctx;
                            if (previousState.phase == Phase.PREPARE)
                            {
                                ctx = threadCtx.Value;
                            }
                            else
                            {
                                ctx = prevThreadCtx.Value;
                            }

                            if (!ctx.markers[EpochPhaseIdx.InProgress])
                            {
                                prevThreadCtx = threadCtx;

                                InitLocalContext(prevThreadCtx.Value.guid);

                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.InProgress, ctx.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
                                }
                                prevThreadCtx.Value.markers[EpochPhaseIdx.InProgress] = true;
                            }
                            break;
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (!prevThreadCtx.Value.markers[EpochPhaseIdx.WaitPending])
                            {
                                var notify = (prevThreadCtx.Value.ioPendingRequests.Count == 0);
                                notify = notify && (prevThreadCtx.Value.retryRequests.Count == 0);

                                if (notify)
                                {
                                    if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitPending, threadCtx.Value.version))
                                    {
                                        GlobalMoveToNextCheckpointState(currentState);
                                    }
                                    prevThreadCtx.Value.markers[EpochPhaseIdx.WaitPending] = true;
                                }

                            }
                            break;
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (!prevThreadCtx.Value.markers[EpochPhaseIdx.WaitFlush])
                            {
                                var notify = false;
                                if (FoldOverSnapshot)
                                {
                                    notify = (hlog.FlushedUntilAddress >= _hybridLogCheckpoint.info.finalLogicalAddress);
                                }
                                else
                                {
                                    notify = (_hybridLogCheckpoint.flushed != null) && _hybridLogCheckpoint.flushed.IsSet;
                                }

                                if (_checkpointType == CheckpointType.FULL)
                                {
                                    notify = notify && IsIndexFuzzyCheckpointCompleted();
                                }

                                if (notify)
                                {
                                    WriteHybridLogContextInfo();

                                    if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitFlush, prevThreadCtx.Value.version))
                                    {
                                        GlobalMoveToNextCheckpointState(currentState);
                                    }

                                    prevThreadCtx.Value.markers[EpochPhaseIdx.WaitFlush] = true;
                                }
                            }
                            break;
                        }

                    case Phase.PERSISTENCE_CALLBACK:
                        {
                            if (!prevThreadCtx.Value.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                            {
                                // Thread local action
                                functions.CheckpointCompletionCallback(threadCtx.Value.guid, prevThreadCtx.Value.serialNum);

                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, prevThreadCtx.Value.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
                                }

                                prevThreadCtx.Value.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                            }
                            break;
                        }
                    case Phase.REST:
                        {
                            break;
                        }
                    default:
                        Debug.WriteLine("Error!");
                        break;
                }

                // update thread local variables
                threadCtx.Value.phase = currentState.phase;
                threadCtx.Value.version = currentState.version;

                previousState.word = currentState.word;
            } while (previousState.word != finalState.word);
        }

        #region Helper functions 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GlobalMoveToNextCheckpointState(SystemState currentState)
        {
            long context = 0;
            return GlobalMoveToNextState(currentState, GetNextState(currentState, _checkpointType), ref context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(SystemState currentState, SystemState nextState)
        {
            // Move from I to P2
            if (Interlocked.CompareExchange(ref _systemState.word, nextState.word, currentState.word) == currentState.word)
            {
                Debug.WriteLine("Moved to {0}, {1}", nextState.phase, nextState.version);
                return true;
            }
            else
            {
                return false;
            }
        }

        private void AcquireSharedLatchesForAllPendingRequests()
        {
            foreach (var ctx in threadCtx.Value.retryRequests)
            {
                AcquireSharedLatch(ctx.key);
            }
            foreach (var ctx in threadCtx.Value.ioPendingRequests.Values)
            {
                AcquireSharedLatch(ctx.key);
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
                    switch(type)
                    {
                        case CheckpointType.INDEX_ONLY:
                            nextState.phase = Phase.REST;
                            break;
                        case CheckpointType.FULL:
                            nextState.phase = Phase.PREPARE;
                            break;
                    }
                    break;
                case Phase.PREPARE:
                    nextState.phase = Phase.IN_PROGRESS;
                    nextState.version = start.version + 1;
                    break;
                case Phase.IN_PROGRESS:
                    nextState.phase = Phase.WAIT_PENDING;
                    break;
                case Phase.WAIT_PENDING:
                    nextState.phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    nextState.phase = Phase.PERSISTENCE_CALLBACK;
                    break;
                case Phase.PERSISTENCE_CALLBACK:
                    nextState.phase = Phase.REST;
                    break;

                case Phase.GC:
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
            string filename = directoryConfiguration.GetHybridLogCheckpointMetaFileName(_hybridLogCheckpointToken);
            using (var file = new StreamWriter(filename, false))
            {
                _hybridLogCheckpoint.info.Write(file);
                file.Flush();
            }
        }

        private void WriteHybridLogCheckpointCompleteFile()
        {
            string completed_filename = directoryConfiguration.GetHybridLogCheckpointFolder(_hybridLogCheckpointToken);
            completed_filename += Path.DirectorySeparatorChar + "completed.dat";
            using (var file = new StreamWriter(completed_filename, false))
            {
                file.WriteLine();
                file.Flush();
            }
        }

        private void WriteHybridLogContextInfo()
        {
            string filename = directoryConfiguration.GetHybridLogCheckpointContextFileName(_hybridLogCheckpointToken, prevThreadCtx.Value.guid);
            using (var file = new StreamWriter(filename, false))
            {
                prevThreadCtx.Value.Write(file);
                file.Flush();
            }

        }

        private void WriteIndexMetaFile()
        {
            string filename = directoryConfiguration.GetIndexCheckpointMetaFileName(_indexCheckpointToken);
            using (var file = new StreamWriter(filename, false))
            {
                _indexCheckpoint.info.Write(file);
                file.Flush();
            }
        }

        private void WriteIndexCheckpointCompleteFile()
        {
            string completed_filename = directoryConfiguration.GetIndexCheckpointFolder(_indexCheckpointToken);
            completed_filename += Path.DirectorySeparatorChar + "completed.dat";
            using (var file = new StreamWriter(completed_filename, false))
            {
                file.WriteLine();
                file.Flush();
            }
        }
        private bool ObtainCurrentTailAddress(ref long location)
        {
            var tailAddress = hlog.GetTailAddress();
            return Interlocked.CompareExchange(ref location, tailAddress, 0) == 0;
        }

        private void InitializeIndexCheckpoint(Guid indexToken)
        {
            directoryConfiguration.CreateIndexCheckpointFolder(indexToken);
            _indexCheckpoint.Initialize(indexToken, state[resizeInfo.version].size, directoryConfiguration);
        }

        private void InitializeHybridLogCheckpoint(Guid hybridLogToken, int version)
        {
            directoryConfiguration.CreateHybridLogCheckpointFolder(hybridLogToken);
            _hybridLogCheckpoint.Initialize(hybridLogToken, version);
        }

        #endregion
    }
}

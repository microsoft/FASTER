// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync()
        {
            do
            {
                bool done = true;

                #region Previous pending requests
                if (threadCtx.Value.phase == Phase.IN_PROGRESS
                    ||
                    threadCtx.Value.phase == Phase.WAIT_PENDING)
                {
                    await CompleteIOPendingRequestsAsync(prevThreadCtx.Value);
                    Debug.Assert(prevThreadCtx.Value.ioPendingRequests.Count == 0);

                    await InternalRefreshAsync();
                    CompleteRetryRequests(prevThreadCtx.Value);

                    done &= (prevThreadCtx.Value.ioPendingRequests.Count == 0);
                    done &= (prevThreadCtx.Value.retryRequests.Count == 0);
                }
                #endregion

                if (!(threadCtx.Value.phase == Phase.IN_PROGRESS
                      ||
                      threadCtx.Value.phase == Phase.WAIT_PENDING))
                {
                    await CompleteIOPendingRequestsAsync(threadCtx.Value);
                    Debug.Assert(threadCtx.Value.ioPendingRequests.Count == 0);
                }
                await InternalRefreshAsync();
                CompleteRetryRequests(threadCtx.Value);

                done &= (threadCtx.Value.ioPendingRequests.Count == 0);
                done &= (threadCtx.Value.retryRequests.Count == 0);

                if (done)
                {
                    return;
                }
            } while (true);
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompleteCheckpointAsync()
        {
            // Thread has an active session.
            // So we need to constantly complete pending 
            // and refresh (done inside CompletePending)
            // for the checkpoint to be proceed
            do
            {
                await CompletePendingAsync();
                if (_systemState.phase == Phase.REST)
                {
                    await CompletePendingAsync();
                    return;
                }
            } while (true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask InternalRefreshAsync()
        {
            epoch.ProtectAndDrain();

            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref _systemState);
            if (threadCtx.Value.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && threadCtx.Value.version == newPhaseInfo.version)
            {
                return;
            }

            // Moving to non-checkpointing phases
            if (newPhaseInfo.phase == Phase.GC || newPhaseInfo.phase == Phase.PREPARE_GROW || newPhaseInfo.phase == Phase.IN_PROGRESS_GROW)
            {
                threadCtx.Value.phase = newPhaseInfo.phase;
                return;
            }

            await HandleCheckpointingPhasesAsync();
        }

        private async ValueTask HandleCheckpointingPhasesAsync()
        {
            var previousState = SystemState.Make(threadCtx.Value.phase, threadCtx.Value.version);
            var finalState = SystemState.Copy(ref _systemState);

            // We need to move from previousState to finalState one step at a time
            do
            {
                // Coming back - need to complete older checkpoint first
                if ((finalState.version > previousState.version + 1) ||
                    (finalState.version > previousState.version && finalState.phase == Phase.REST))
                {
                    if (lastFullCheckpointVersion > previousState.version)
                    {
                        functions.CheckpointCompletionCallback(threadCtx.Value.guid, threadCtx.Value.serialNum);
                    }

                    // Get system out of intermediate phase
                    while (finalState.phase == Phase.INTERMEDIATE)
                    {
                        finalState = SystemState.Copy(ref _systemState);
                    }

                    // Fast forward to current global state
                    previousState.version = finalState.version;
                    previousState.phase = finalState.phase;
                }

                // Don't play around when system state is being changed
                if (finalState.phase == Phase.INTERMEDIATE)
                {
                    return;
                }


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

                            if (!IsIndexFuzzyCheckpointCompleted())
                            {
                                // Suspend
                                var prevThreadCtxCopy = prevThreadCtx.Value;
                                var threadCtxCopy = threadCtx.Value;
                                SuspendSession();

                                await IsIndexFuzzyCheckpointCompletedAsync();

                                // Resume session
                                ResumeSession(prevThreadCtxCopy, threadCtxCopy);
                            }
                            GlobalMoveToNextCheckpointState(currentState);

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
                                CopyContext(threadCtx.Value, prevThreadCtx.Value);
                                InitContext(threadCtx.Value, prevThreadCtx.Value.guid);

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
                                bool notify;

                                if (FoldOverSnapshot)
                                {
                                    notify = (hlog.FlushedUntilAddress >= _hybridLogCheckpoint.info.finalLogicalAddress);
                                }
                                else
                                {
                                    notify = (_hybridLogCheckpoint.flushed != null) && _hybridLogCheckpoint.flushed.IsSet;
                                }

                                if (!notify)
                                {
                                    Debug.Assert(_hybridLogCheckpoint.flushedSemaphore != null);

                                    // Suspend
                                    var prevThreadCtxCopy = prevThreadCtx.Value;
                                    var threadCtxCopy = threadCtx.Value;
                                    SuspendSession();

                                    await _hybridLogCheckpoint.flushedSemaphore.WaitAsync();

                                    // Resume session
                                    ResumeSession(prevThreadCtxCopy, threadCtxCopy);

                                    _hybridLogCheckpoint.flushedSemaphore.Release();

                                    notify = true;
                                }

                                if (_checkpointType == CheckpointType.FULL)
                                {
                                    notify = notify && IsIndexFuzzyCheckpointCompleted();

                                    if (!notify)
                                    {
                                        // Suspend
                                        var prevThreadCtxCopy = prevThreadCtx.Value;
                                        var threadCtxCopy = threadCtx.Value;
                                        SuspendSession();

                                        await IsIndexFuzzyCheckpointCompletedAsync();

                                        // Resume session
                                        ResumeSession(prevThreadCtxCopy, threadCtxCopy);

                                        notify = true;
                                    }
                                }

                                if (notify)
                                {
                                    _hybridLogCheckpoint.info.checkpointTokens.TryAdd(prevThreadCtx.Value.guid, prevThreadCtx.Value.serialNum);

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
                                    lastFullCheckpointVersion = currentState.version;
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
    }
}

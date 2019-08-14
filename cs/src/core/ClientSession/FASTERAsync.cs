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
        internal async ValueTask CompletePendingAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            do
            {
                bool done = true;

                #region Previous pending requests
                if (threadCtx.Value.phase == Phase.IN_PROGRESS
                    ||
                    threadCtx.Value.phase == Phase.WAIT_PENDING)
                {
                    await CompleteIOPendingRequestsAsync(prevThreadCtx.Value, clientSession);
                    clientSession.ResumeThread();

                    Debug.Assert(prevThreadCtx.Value.ioPendingRequests.Count == 0);
                    
                    await InternalRefreshAsync(clientSession);
                    clientSession.ResumeThread();

                    CompleteRetryRequests(prevThreadCtx.Value);

                    done &= (prevThreadCtx.Value.ioPendingRequests.Count == 0);
                    done &= (prevThreadCtx.Value.retryRequests.Count == 0);
                }
                #endregion

                if (!(threadCtx.Value.phase == Phase.IN_PROGRESS
                      ||
                      threadCtx.Value.phase == Phase.WAIT_PENDING))
                {
                    await CompleteIOPendingRequestsAsync(threadCtx.Value, clientSession);
                    clientSession.ResumeThread();

                    Debug.Assert(threadCtx.Value.ioPendingRequests.Count == 0);
                }
                await InternalRefreshAsync(clientSession);
                clientSession.ResumeThread();

                CompleteRetryRequests(threadCtx.Value);

                done &= (threadCtx.Value.ioPendingRequests.Count == 0);
                done &= (threadCtx.Value.retryRequests.Count == 0);

                if (done)
                {
                    return;
                }
                else
                {
                    throw new Exception("CompletePending loops");
                }
            } while (true);
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompleteCheckpointAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            // Thread has an active session.
            // So we need to constantly complete pending 
            // and refresh (done inside CompletePending)
            // for the checkpoint to be proceed
            int count = 0;
            do
            {
                await InternalRefreshAsync(clientSession);
                clientSession.ResumeThread();

                await CompletePendingAsync(clientSession);
                clientSession.ResumeThread();

                if (_systemState.phase == Phase.REST)
                {
                    await CompletePendingAsync(clientSession);
                    clientSession.ResumeThread();
                    return;
                }

                if (count++ == 10000) throw new Exception("CompleteCheckpointAsync loop too long " + threadCtx.Value.phase + threadCtx.Value.version + ":" + _systemState.phase + _systemState.version);
            } while (true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask InternalRefreshAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
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

            await HandleCheckpointingPhasesAsync(clientSession);
            clientSession.ResumeThread();
        }


        private bool AtomicSwitch(FasterExecutionContext fromCtx, FasterExecutionContext toCtx, int version)
        {
            lock (toCtx)
            {
                if (toCtx.version < version)
                {
                    CopyContext(fromCtx, toCtx);
                    _hybridLogCheckpoint.info.checkpointTokens.TryAdd(toCtx.guid, toCtx.serialNum);
                    return true;
                }
            }
            return false;
        }

        private async ValueTask HandleCheckpointingPhasesAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true)
        {
            var previousState = SystemState.Make(threadCtx.Value.phase, threadCtx.Value.version);
            var finalState = SystemState.Copy(ref _systemState);

            int count = 0;
            while (finalState.phase == Phase.INTERMEDIATE)
            {
                finalState = SystemState.Copy(ref _systemState);
                if (count++ == 10000) throw new Exception("Intermediate too long");
            }

            count = 0;

            // We need to move from previousState to finalState one step at a time
            do
            {
                var currentState = default(SystemState);
                if (previousState.word == finalState.word)
                {
                    currentState.word = previousState.word;
                }
                else if (previousState.version < finalState.version)
                {
                    if (finalState.phase <= Phase.PREPARE)
                        previousState.version = finalState.version;
                    else
                        previousState.version = finalState.version - 1;
                    currentState = GetNextState(previousState, CheckpointType.FULL);
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

                            if (async && !IsIndexFuzzyCheckpointCompleted())
                            {
                                clientSession.SuspendThread();
                                await IsIndexFuzzyCheckpointCompletedAsync();
                                clientSession.ResumeThread();
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
                            if (previousState.phase == Phase.IN_PROGRESS)
                            {
                                ctx = prevThreadCtx.Value;
                            }
                            else
                            {
                                ctx = threadCtx.Value;
                            }

                            if (!ctx.markers[EpochPhaseIdx.InProgress])
                            {
                                AtomicSwitch(threadCtx.Value, prevThreadCtx.Value, ctx.version);
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
                                    notify = (_hybridLogCheckpoint.flushedSemaphore != null) && _hybridLogCheckpoint.flushedSemaphore.CurrentCount > 0;
                                }

                                if (async && !notify)
                                {
                                    Debug.Assert(_hybridLogCheckpoint.flushedSemaphore != null);
                                    clientSession.SuspendThread();
                                    await _hybridLogCheckpoint.flushedSemaphore.WaitAsync();
                                    clientSession.ResumeThread();

                                    _hybridLogCheckpoint.flushedSemaphore.Release();

                                    notify = true;
                                }

                                if (_checkpointType == CheckpointType.FULL)
                                {
                                    notify = notify && IsIndexFuzzyCheckpointCompleted();

                                    if (async && !notify)
                                    {
                                        clientSession.SuspendThread();
                                        await IsIndexFuzzyCheckpointCompletedAsync();
                                        clientSession.ResumeThread();

                                        notify = true;
                                    }
                                }

                                if (notify)
                                {
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
                if (count++ == 10000) throw new Exception("HandleCheckpointingPhases do loop too long " + previousState + ":" + finalState);
            } while (previousState.word != finalState.word);
        }
    }
}

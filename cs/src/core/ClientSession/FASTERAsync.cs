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
        /// Whether we are in relaxed CPR mode, where IO pending ops are not
        /// part of the CPR checkpoint
        /// </summary>
        public bool RelaxedCPR = false;

        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            bool done = true;

            if (!RelaxedCPR)
            {
                #region Previous pending requests
                if (clientSession.ctx.phase == Phase.IN_PROGRESS
                    ||
                    clientSession.ctx.phase == Phase.WAIT_PENDING)
                {

                    await CompleteIOPendingRequestsAsync(clientSession.prevCtx, clientSession);
                    Debug.Assert(clientSession.prevCtx.ioPendingRequests.Count == 0);

                    if (clientSession.prevCtx.retryRequests.Count > 0)
                    {
                        clientSession.ResumeThread();
                        CompleteRetryRequests(clientSession.prevCtx, clientSession);
                        clientSession.SuspendThread();
                    }

                    done &= (clientSession.prevCtx.ioPendingRequests.Count == 0);
                    done &= (clientSession.prevCtx.retryRequests.Count == 0);
                }
                #endregion
            }

            if (RelaxedCPR || (!(clientSession.ctx.phase == Phase.IN_PROGRESS
                  ||
                  clientSession.ctx.phase == Phase.WAIT_PENDING)))
            {
                await CompleteIOPendingRequestsAsync(clientSession.ctx, clientSession);
                Debug.Assert(clientSession.ctx.ioPendingRequests.Count == 0);
            }

            CompleteRetryRequests(clientSession.ctx, clientSession);

            done &= (clientSession.ctx.ioPendingRequests.Count == 0);
            done &= (clientSession.ctx.retryRequests.Count == 0);

            if (!done)
            {
                throw new Exception("CompletePendingAsync did not complete");
            }
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompleteCheckpointAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            // Called outside active session
            while (true)
            {
                var systemState = _systemState;
                await InternalRefreshAsync(clientSession);
                await CompletePendingAsync(clientSession);

                if (systemState.phase == Phase.REST)
                    return;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask InternalRefreshAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            // We check if we are in normal mode
            var newPhaseInfo = SystemState.Copy(ref _systemState);
            if (clientSession.ctx.phase == Phase.REST && newPhaseInfo.phase == Phase.REST && clientSession.ctx.version == newPhaseInfo.version)
            {
                return;
            }

            // In non-checkpointing phases
            if (newPhaseInfo.phase == Phase.GC || newPhaseInfo.phase == Phase.PREPARE_GROW || newPhaseInfo.phase == Phase.IN_PROGRESS_GROW)
            {
                return;
            }

            await HandleCheckpointingPhasesAsync(clientSession);
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
            if (async)
                clientSession.ResumeThread();

            var previousState = SystemState.Make(threadCtx.Value.phase, threadCtx.Value.version);
            var finalState = SystemState.Copy(ref _systemState);

            while (finalState.phase == Phase.INTERMEDIATE)
            {
                finalState = SystemState.Copy(ref _systemState);
            }

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
                                threadCtx.Value.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = true;
                            }
                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.PrepareForIndexCheckpt, threadCtx.Value.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
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
                                if (!RelaxedCPR)
                                {
                                    AcquireSharedLatchesForAllPendingRequests();
                                }

                                var idx = Interlocked.Increment(ref _hybridLogCheckpoint.info.numThreads);
                                idx -= 1;

                                _hybridLogCheckpoint.info.guids[idx] = threadCtx.Value.guid;

                                threadCtx.Value.markers[EpochPhaseIdx.Prepare] = true;
                            }

                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.Prepare, threadCtx.Value.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
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

                                // Has to be prevThreadCtx, not ctx
                                prevThreadCtx.Value.markers[EpochPhaseIdx.InProgress] = true;
                            }

                            // Has to be prevThreadCtx, not ctx
                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.InProgress, prevThreadCtx.Value.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
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
                            else
                            {
                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitPending, threadCtx.Value.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
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
                            else
                            {
                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitFlush, prevThreadCtx.Value.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
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
                                prevThreadCtx.Value.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                            }
                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, prevThreadCtx.Value.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
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

            if (async)
                clientSession.SuspendThread();
        }
    }
}

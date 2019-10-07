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
            bool done = true;

            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (clientSession.ctx.phase == Phase.IN_PROGRESS
                    ||
                    clientSession.ctx.phase == Phase.WAIT_PENDING)
                {

                    await CompleteIOPendingRequestsAsync(clientSession.ctx.prevCtx, clientSession.ctx, clientSession);
                    Debug.Assert(clientSession.ctx.prevCtx.ioPendingRequests.Count == 0);

                    if (clientSession.ctx.prevCtx.retryRequests.Count > 0)
                    {
                        CompleteRetryRequests(clientSession.ctx.prevCtx, clientSession.ctx, clientSession);
                    }

                    done &= (clientSession.ctx.prevCtx.ioPendingRequests.Count == 0);
                    done &= (clientSession.ctx.prevCtx.retryRequests.Count == 0);
                }
            }
            #endregion

            await CompleteIOPendingRequestsAsync(clientSession.ctx, clientSession.ctx, clientSession);
            CompleteRetryRequests(clientSession.ctx, clientSession.ctx, clientSession);

            Debug.Assert(clientSession.ctx.ioPendingRequests.Count == 0);

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
        internal async ValueTask CompleteCheckpointAsync(FasterExecutionContext ctx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            // Called outside active session
            while (true)
            {
                var systemState = _systemState;
                await InternalRefreshAsync(ctx, clientSession);
                await CompletePendingAsync(clientSession);

                if (systemState.phase == Phase.REST)
                    return;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask InternalRefreshAsync(FasterExecutionContext ctx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
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

            await HandleCheckpointingPhasesAsync(ctx, clientSession);
        }


        private bool AtomicSwitch(FasterExecutionContext fromCtx, FasterExecutionContext toCtx, int version)
        {
            lock (toCtx)
            {
                if (toCtx.version < version)
                {
                    CopyContext(fromCtx, toCtx);
                    _hybridLogCheckpoint.info.checkpointTokens.TryAdd(toCtx.guid,
                        new CommitPoint
                        {
                            UntilSerialNo = toCtx.serialNum,
                            ExcludedSerialNos = toCtx.excludedSerialNos
                        });
                    return true;
                }
            }
            return false;
        }

        private async ValueTask HandleCheckpointingPhasesAsync(FasterExecutionContext ctx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true)
        {
            if (async)
                clientSession.UnsafeResumeThread();

            var previousState = SystemState.Make(ctx.phase, ctx.version);
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
                            if (!ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt])
                            {
                                ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = true;
                            }
                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.PrepareForIndexCheckpt, ctx.version))
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
                                ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = false;
                            }

                            if (async && !IsIndexFuzzyCheckpointCompleted())
                            {
                                clientSession.UnsafeSuspendThread();
                                await IsIndexFuzzyCheckpointCompletedAsync();
                                clientSession.UnsafeResumeThread();
                            }
                            GlobalMoveToNextCheckpointState(currentState);

                            break;
                        }
                    case Phase.PREPARE:
                        {
                            if (!ctx.markers[EpochPhaseIdx.Prepare])
                            {
                                if (!RelaxedCPR)
                                {
                                    AcquireSharedLatchesForAllPendingRequests(ctx);
                                }

                                var idx = Interlocked.Increment(ref _hybridLogCheckpoint.info.numThreads);
                                idx -= 1;

                                _hybridLogCheckpoint.info.guids[idx] = ctx.guid;

                                ctx.markers[EpochPhaseIdx.Prepare] = true;
                            }

                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.Prepare, ctx.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }

                            break;
                        }
                    case Phase.IN_PROGRESS:
                        {
                            // Need to be very careful here as threadCtx is changing
                            FasterExecutionContext _ctx;
                            if (previousState.phase == Phase.IN_PROGRESS)
                            {
                                _ctx = ctx.prevCtx;
                            }
                            else
                            {
                                _ctx = ctx;
                            }

                            if (!_ctx.markers[EpochPhaseIdx.InProgress])
                            {
                                AtomicSwitch(ctx, ctx.prevCtx, _ctx.version);
                                InitContext(ctx, ctx.prevCtx.guid);

                                // Has to be prevCtx, not ctx
                                ctx.prevCtx.markers[EpochPhaseIdx.InProgress] = true;
                            }

                            // Has to be prevCtx, not ctx
                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.InProgress, ctx.prevCtx.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }
                            break;
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (!ctx.prevCtx.markers[EpochPhaseIdx.WaitPending])
                            {
                                var notify = (ctx.prevCtx.ioPendingRequests.Count == 0);
                                notify = notify && (ctx.prevCtx.retryRequests.Count == 0);

                                if (notify)
                                {
                                    if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitPending, ctx.version))
                                    {
                                        GlobalMoveToNextCheckpointState(currentState);
                                    }
                                    ctx.prevCtx.markers[EpochPhaseIdx.WaitPending] = true;
                                }
                            }
                            else
                            {
                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitPending, ctx.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
                                }
                            }
                            break;
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (!ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
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
                                    clientSession.UnsafeSuspendThread();
                                    await _hybridLogCheckpoint.flushedSemaphore.WaitAsync();
                                    clientSession.UnsafeResumeThread();

                                    _hybridLogCheckpoint.flushedSemaphore.Release();

                                    notify = true;
                                }

                                if (_checkpointType == CheckpointType.FULL)
                                {
                                    notify = notify && IsIndexFuzzyCheckpointCompleted();

                                    if (async && !notify)
                                    {
                                        clientSession.UnsafeSuspendThread();
                                        await IsIndexFuzzyCheckpointCompletedAsync();
                                        clientSession.UnsafeResumeThread();

                                        notify = true;
                                    }
                                }

                                if (notify)
                                {
                                    if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitFlush, ctx.prevCtx.version))
                                    {
                                        GlobalMoveToNextCheckpointState(currentState);
                                    }

                                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
                                }
                            }
                            else
                            {
                                if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.WaitFlush, ctx.prevCtx.version))
                                {
                                    GlobalMoveToNextCheckpointState(currentState);
                                }
                            }
                            break;
                        }

                    case Phase.PERSISTENCE_CALLBACK:
                        {
                            if (!ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                            {
                                // Thread local action
                                functions.CheckpointCompletionCallback(ctx.guid,
                                    new CommitPoint
                                    {
                                        UntilSerialNo = ctx.prevCtx.serialNum,
                                        ExcludedSerialNos = ctx.prevCtx.excludedSerialNos
                                    });
                                ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                            }
                            if (epoch.MarkAndCheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, ctx.prevCtx.version))
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
                ctx.phase = currentState.phase;
                ctx.version = currentState.version;

                previousState.word = currentState.word;
            } while (previousState.word != finalState.word);

            if (async)
                clientSession.UnsafeSuspendThread();
        }
    }
}

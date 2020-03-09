// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// The FASTER key-value store
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    /// <typeparam name="Context">Context</typeparam>
    /// <typeparam name="Functions">Functions</typeparam>
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
        {
            bool done = true;

            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (clientSession.ctx.phase == Phase.IN_PROGRESS
                    ||
                    clientSession.ctx.phase == Phase.WAIT_PENDING)
                {

                    await clientSession.ctx.prevCtx.pendingReads.WaitEmptyAsync();

                    await CompleteIOPendingRequestsAsync(clientSession.ctx.prevCtx, clientSession.ctx, clientSession, token);
                    Debug.Assert(clientSession.ctx.prevCtx.ioPendingRequests.Count == 0);

                    if (clientSession.ctx.prevCtx.retryRequests.Count > 0)
                    {
                        CompleteRetryRequests(clientSession.ctx.prevCtx, clientSession.ctx, clientSession);
                    }

                    done &= (clientSession.ctx.prevCtx.HasNoPendingRequests);
                }
            }
            #endregion

            await clientSession.ctx.pendingReads.WaitEmptyAsync();

            await CompleteIOPendingRequestsAsync(clientSession.ctx, clientSession.ctx, clientSession, token);
            CompleteRetryRequests(clientSession.ctx, clientSession.ctx, clientSession);

            Debug.Assert(clientSession.ctx.HasNoPendingRequests);

            done &= (clientSession.ctx.HasNoPendingRequests);

            if (!done)
            {
                throw new Exception("CompletePendingAsync did not complete");
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
            if (newPhaseInfo.phase == Phase.PREPARE_GROW || newPhaseInfo.phase == Phase.IN_PROGRESS_GROW)
            {
                return;
            }

            await HandleCheckpointingPhasesAsync(ctx, clientSession);
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;

            
            (Status status, Output output) _result;
            FasterKV<Key, Value, Input, Output, Context, Functions> _fasterKV;
            ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;
            PendingContext _pendingContext;
            AsyncIOContext<Key, Value> _diskRequest;
            

            internal ReadAsyncResult(
                FasterKV<Key, Value, Input, Output, Context, Functions> fasterKV, 
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, 
                PendingContext pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {                 
                _exception = default;
                _result = default;
                _fasterKV = fasterKV;
                _clientSession = clientSession;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                _diskRequest.asyncOperation.CompletionComputeStatus = Pending;
            }

            internal ReadAsyncResult(Status status, Output output)
            {
                _exception = default;
                _result = (status, output);
                _fasterKV = default;
                _clientSession = default;
                _pendingContext = default;
                _diskRequest = default;
            }
            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result, or throws an exception if error encountered.</returns>
            public (Status, Output) CompleteRead()
            {
                if (_diskRequest.asyncOperation != null
                    && _diskRequest.asyncOperation.CompletionComputeStatus != Completed 
                    && Interlocked.CompareExchange(ref _diskRequest.asyncOperation.CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {

                        if (_clientSession.SupportAsync) _clientSession.UnsafeResumeThread();
                        try
                        {
                            Debug.Assert(_fasterKV.RelaxedCPR);

                            _result = _fasterKV.InternalCompleteIOPendingReadRequestsAsync(
                                _clientSession.ctx, _clientSession.ctx, _diskRequest, _pendingContext);
                        }
                        finally
                        {
                            if (_clientSession.SupportAsync) _clientSession.UnsafeSuspendThread();
                        }

                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _clientSession.ctx.pendingReads.Remove(_pendingContext.id);
                    }
                }

                if (_exception != default)
                    _exception.Throw();
                return _result;
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult> ReadAsync(
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Input input, Context context = default, CancellationToken token = default)
        {
            var pcontext = default(PendingContext);
            Output output = default;

            var nextSerialNum = clientSession.ctx.serialNum + 1;

            OperationStatus internalStatus;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {

            TryReadAgain:

                internalStatus = InternalRead(ref key, ref input, ref output,
                ref context, ref pcontext, clientSession.ctx, nextSerialNum);

                if (internalStatus == OperationStatus.CPR_SHIFT_DETECTED)
                {
                    SynchronizeEpoch(clientSession.ctx, clientSession.ctx, ref pcontext);
                    goto TryReadAgain;
                }

            }
            finally
            {
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            switch (internalStatus)
            {
                case OperationStatus.SUCCESS:
                case OperationStatus.NOTFOUND:

                    clientSession.ctx.serialNum = nextSerialNum;

                    return new ValueTask<ReadAsyncResult>(new ReadAsyncResult((Status)internalStatus, output));

                case OperationStatus.RECORD_ON_DISK:
                    return SlowReadAsync(this, clientSession, pcontext, nextSerialNum, token);


                default:
                    throw new Exception($"Unexpected {nameof(OperationStatus)} while reading => {internalStatus}");

            }

            static async ValueTask<ReadAsyncResult> SlowReadAsync(
                FasterKV<Key, Value, Input, Output, Context, Functions> @this,
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                PendingContext pendingContext, long nextSerialNum, CancellationToken token = default)
            {

                var diskRequest = @this.ScheduleGetFromDisk(clientSession.ctx, ref pendingContext);

                clientSession.ctx.serialNum = nextSerialNum;

                clientSession.ctx.pendingReads.Add(pendingContext.id, pendingContext);

                try
                {

                    token.ThrowIfCancellationRequested();

                    if (@this.epoch.ThisInstanceProtected())
                        throw new NotSupportedException("Async operations not supported over protected epoch");

                    diskRequest = await diskRequest.asyncOperation.ValueTaskOfT;

                }
                catch
                {
                    clientSession.ctx.pendingReads.Remove(pendingContext.id);
                    throw;
                }

                return new ReadAsyncResult(@this, clientSession, pendingContext, diskRequest);

            }
        }

        private bool AtomicSwitch(FasterExecutionContext fromCtx, FasterExecutionContext toCtx, int version)
        {
            lock (toCtx)
            {
                if (toCtx.version < version)
                {
                    CopyContext(fromCtx, toCtx);
                    if (toCtx.serialNum != -1)
                    {
                        _hybridLogCheckpoint.info.checkpointTokens.TryAdd(toCtx.guid,
                            new CommitPoint
                            {
                                UntilSerialNo = toCtx.serialNum,
                                ExcludedSerialNos = toCtx.excludedSerialNos
                            });
                    }
                    return true;
                }
            }
            return false;
        }

        private SystemState GetStartState(SystemState state)
        {
            if (state.phase <= Phase.REST)
                return SystemState.Make(Phase.REST, state.version - 1);
            else
                return SystemState.Make(Phase.REST, state.version);
        }

        private async ValueTask HandleCheckpointingPhasesAsync(FasterExecutionContext ctx, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true, CancellationToken token = default)
        {
            if (async)
                clientSession?.UnsafeResumeThread();

            var finalState = SystemState.Copy(ref _systemState);
            while (finalState.phase == Phase.INTERMEDIATE)
                finalState = SystemState.Copy(ref _systemState);

            var previousState = ctx != null ? SystemState.Make(ctx.phase, ctx.version) : finalState;

            // We need to move from previousState to finalState one step at a time
            var currentState = previousState;

            SystemState startState = GetStartState(finalState);

            if ((currentState.version < startState.version) ||
                (currentState.version == startState.version && currentState.phase < startState.phase))
            {
                // Fast-forward to beginning of current checkpoint cycle
                currentState = startState;
            }

            do
            {
                switch (currentState.phase)
                {
                    case Phase.PREP_INDEX_CHECKPOINT:
                        {
                            if (ctx != null)
                            {
                                if (!ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt])
                                {
                                    ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = true;
                                }
                                epoch.Mark(EpochPhaseIdx.PrepareForIndexCheckpt, currentState.version);
                            }

                            if (epoch.CheckIsComplete(EpochPhaseIdx.PrepareForIndexCheckpt, currentState.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }
                            break;
                        }
                    case Phase.INDEX_CHECKPOINT:
                        {
                            if (_checkpointType == CheckpointType.INDEX_ONLY && ctx != null)
                            {
                                // Reseting the marker for a potential FULL or INDEX_ONLY checkpoint in the future
                                ctx.markers[EpochPhaseIdx.PrepareForIndexCheckpt] = false;
                            }

                            if (async && !IsIndexFuzzyCheckpointCompleted())
                            {
                                clientSession?.UnsafeSuspendThread();
                                await IsIndexFuzzyCheckpointCompletedAsync(token);
                                clientSession?.UnsafeResumeThread();
                            }
                            GlobalMoveToNextCheckpointState(currentState);

                            break;
                        }
                    case Phase.PREPARE:
                        {
                            if (ctx != null)
                            {
                                if (!ctx.markers[EpochPhaseIdx.Prepare])
                                {
                                    if (!RelaxedCPR)
                                    {
                                        AcquireSharedLatchesForAllPendingRequests(ctx);
                                    }
                                    ctx.markers[EpochPhaseIdx.Prepare] = true;
                                }
                                epoch.Mark(EpochPhaseIdx.Prepare, currentState.version);
                            }

                            if (epoch.CheckIsComplete(EpochPhaseIdx.Prepare, currentState.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }

                            break;
                        }
                    case Phase.IN_PROGRESS:
                        {
                            if (ctx != null)
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
                                    InitContext(ctx, ctx.prevCtx.guid, ctx.prevCtx.serialNum);

                                    // Has to be prevCtx, not ctx
                                    ctx.prevCtx.markers[EpochPhaseIdx.InProgress] = true;
                                }

                                epoch.Mark(EpochPhaseIdx.InProgress, currentState.version);
                            }

                            // Has to be prevCtx, not ctx
                            if (epoch.CheckIsComplete(EpochPhaseIdx.InProgress, currentState.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }
                            break;
                        }
                    case Phase.WAIT_PENDING:
                        {
                            if (ctx != null)
                            {
                                if (!ctx.prevCtx.markers[EpochPhaseIdx.WaitPending])
                                {
                                    var notify = (ctx.prevCtx.HasNoPendingRequests);

                                    if (notify)
                                    {
                                        ctx.prevCtx.markers[EpochPhaseIdx.WaitPending] = true;
                                    }
                                    else
                                        break;
                                }
                                epoch.Mark(EpochPhaseIdx.WaitPending, currentState.version);
                            }

                            if (epoch.CheckIsComplete(EpochPhaseIdx.WaitPending, currentState.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }
                            break;
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            if (ctx == null  || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
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
                                    clientSession?.UnsafeSuspendThread();
                                    await _hybridLogCheckpoint.flushedSemaphore.WaitAsync(token);
                                    clientSession?.UnsafeResumeThread();

                                    _hybridLogCheckpoint.flushedSemaphore.Release();

                                    notify = true;
                                }

                                if (_checkpointType == CheckpointType.FULL)
                                {
                                    notify = notify && IsIndexFuzzyCheckpointCompleted();

                                    if (async && !notify)
                                    {
                                        clientSession?.UnsafeSuspendThread();
                                        await IsIndexFuzzyCheckpointCompletedAsync(token);
                                        clientSession?.UnsafeResumeThread();

                                        notify = true;
                                    }
                                }

                                if (notify)
                                {
                                    if (ctx != null)
                                        ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
                                }
                                else
                                    break;
                            }

                            if (ctx != null)
                                epoch.Mark(EpochPhaseIdx.WaitFlush, currentState.version);

                            if (epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, currentState.version))
                            {
                                GlobalMoveToNextCheckpointState(currentState);
                            }
                            break;
                        }

                    case Phase.PERSISTENCE_CALLBACK:
                        {
                            if (ctx != null)
                            {
                                if (!ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback])
                                {
                                    if (ctx.prevCtx.serialNum != -1)
                                    {
                                        var commitPoint = new CommitPoint
                                        {
                                            UntilSerialNo = ctx.prevCtx.serialNum,
                                            ExcludedSerialNos = ctx.prevCtx.excludedSerialNos
                                        };

                                        // Thread local action
                                        functions.CheckpointCompletionCallback(ctx.guid, commitPoint);
                                        if (clientSession != null)
                                            clientSession.LatestCommitPoint = commitPoint;
                                    }
                                    ctx.prevCtx.markers[EpochPhaseIdx.CheckpointCompletionCallback] = true;
                                }
                                epoch.Mark(EpochPhaseIdx.CheckpointCompletionCallback, currentState.version);
                            }
                            if (epoch.CheckIsComplete(EpochPhaseIdx.CheckpointCompletionCallback, currentState.version))
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
                        throw new FasterException("Invalid state found during checkpointing");
                }

                if (ctx != null)
                {
                    // update thread local variables
                    ctx.phase = currentState.phase;
                    ctx.version = currentState.version;
                }
                previousState.word = currentState.word;
                currentState = GetNextState(currentState, _checkpointType);
            } while (previousState.word != finalState.word);

            if (async)
                clientSession?.UnsafeSuspendThread();
        }
    }
}

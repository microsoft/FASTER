// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// The FASTER key-value store
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
        where Key : new()
        where Value : new()
    {

        /// <summary>
        /// Check if at least one (sync) request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="clientSession"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal async ValueTask ReadyToCompletePendingAsync<Input, Output, Context, Functions>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (clientSession.ctx.phase == Phase.IN_PROGRESS || clientSession.ctx.phase == Phase.WAIT_PENDING)
                {
                    if (clientSession.ctx.prevCtx.SyncIoPendingCount != 0)
                        await clientSession.ctx.prevCtx.readyResponses.WaitForEntryAsync(token);
                }
            }
            #endregion

            if (clientSession.ctx.SyncIoPendingCount != 0)
                await clientSession.ctx.readyResponses.WaitForEntryAsync(token);
        }

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<Input, Output, Context, Functions>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
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

                    await InternalCompletePendingRequestsAsync(clientSession.ctx.prevCtx, clientSession.ctx, clientSession.FasterSession, token);
                    Debug.Assert(clientSession.ctx.prevCtx.SyncIoPendingCount == 0);

                    if (clientSession.ctx.prevCtx.retryRequests.Count > 0)
                    {
                        InternalCompleteRetryRequests(clientSession.ctx.prevCtx, clientSession.ctx, clientSession.FasterSession);
                    }

                    done &= (clientSession.ctx.prevCtx.HasNoPendingRequests);
                }
            }
            #endregion

            await InternalCompletePendingRequestsAsync(clientSession.ctx, clientSession.ctx, clientSession.FasterSession, token);
            InternalCompleteRetryRequests(clientSession.ctx, clientSession.ctx, clientSession.FasterSession);

            Debug.Assert(clientSession.ctx.HasNoPendingRequests);

            done &= (clientSession.ctx.HasNoPendingRequests);

            if (!done)
            {
                throw new Exception("CompletePendingAsync did not complete");
            }
        }

        internal sealed class ReadAsyncInternal<Input, Output, Context, Functions>
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;
            PendingContext<Input, Output, Context> _pendingContext;
            AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;

            internal ReadAsyncInternal(FasterKV<Key, Value> fasterKV, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                _exception = default;
                _fasterKV = fasterKV;
                _clientSession = clientSession;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
            }

            internal (Status, Output) CompleteRead()
            {
                (Status, Output) _result = default;
                if (_diskRequest.asyncOperation != null
                    && CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_clientSession.SupportAsync) _clientSession.UnsafeResumeThread();
                        try
                        {
                            Debug.Assert(_fasterKV.RelaxedCPR);

                            _result = _fasterKV.InternalCompletePendingReadRequest(
                                _clientSession.ctx, _clientSession.ctx, _clientSession.FasterSession, _diskRequest, _pendingContext);
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
                        _clientSession.ctx.ioPendingRequests.Remove(_pendingContext.id);
                        _clientSession.ctx.asyncPendingCount--;
                    }
                }

                if (_exception != default)
                    _exception.Throw();
                return _result;
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, Output, Context, Functions>
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            internal readonly Status status;
            internal readonly Output output;

            internal readonly ReadAsyncInternal<Input, Output, Context, Functions> readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output)
            {
                this.status = status;
                this.output = output;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value> fasterKV,
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                status = Status.PENDING;
                output = default;
                readAsyncInternal = new ReadAsyncInternal<Input, Output, Context, Functions>(fasterKV, clientSession, pendingContext, diskRequest);
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result, or throws an exception if error encountered.</returns>
            public (Status, Output) CompleteRead()
            {
                if (status != Status.PENDING)
                    return (status, output);

                return readAsyncInternal.CompleteRead();
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync<Input, Output, Context, Functions>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Input input, Context context = default, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            Output output = default;
            OperationStatus internalStatus;
            var nextSerialNum = clientSession.ctx.serialNum + 1;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
            TryReadAgain:

                internalStatus = InternalRead(ref key, ref input, ref output, ref context, ref pcontext, clientSession.FasterSession, clientSession.ctx, nextSerialNum);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<Input, Output, Context, Functions>>(new ReadAsyncResult<Input, Output, Context, Functions>((Status)internalStatus, output));
                }

                if (internalStatus == OperationStatus.CPR_SHIFT_DETECTED)
                {
                    SynchronizeEpoch(clientSession.ctx, clientSession.ctx, ref pcontext, clientSession.FasterSession);
                    goto TryReadAgain;
                }
            }
            finally
            {
                clientSession.ctx.serialNum = nextSerialNum;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, clientSession, pcontext, token);
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> SlowReadAsync<Input, Output, Context, Functions>(
            FasterKV<Key, Value> @this,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            PendingContext<Input, Output, Context> pendingContext, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var diskRequest = @this.ScheduleGetFromDisk(clientSession.ctx, ref pendingContext);
            clientSession.ctx.ioPendingRequests.Add(pendingContext.id, pendingContext);
            clientSession.ctx.asyncPendingCount++;
            clientSession.ctx.pendingReads.Add();

            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                diskRequest = await diskRequest.asyncOperation.ValueTaskOfT;
            }
            catch
            {
                clientSession.ctx.ioPendingRequests.Remove(pendingContext.id);
                clientSession.ctx.asyncPendingCount--;
                throw;
            }
            finally
            {
                clientSession.ctx.pendingReads.Remove();
            }

            return new ReadAsyncResult<Input, Output, Context, Functions>(@this, clientSession, pendingContext, diskRequest);
        }

        internal bool AtomicSwitch<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> fromCtx, FasterExecutionContext<Input, Output, Context> toCtx, int version)
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
    }
}

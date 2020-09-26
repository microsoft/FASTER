// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
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
                        clientSession.FasterSession.UnsafeResumeThread();
                        InternalCompleteRetryRequests(clientSession.ctx.prevCtx, clientSession.ctx, clientSession.FasterSession);
                        clientSession.FasterSession.UnsafeSuspendThread();
                    }

                    done &= (clientSession.ctx.prevCtx.HasNoPendingRequests);
                }
            }
            #endregion

            await InternalCompletePendingRequestsAsync(clientSession.ctx, clientSession.ctx, clientSession.FasterSession, token);

            clientSession.FasterSession.UnsafeResumeThread();
            InternalCompleteRetryRequests(clientSession.ctx, clientSession.ctx, clientSession.FasterSession);
            clientSession.FasterSession.UnsafeSuspendThread();

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

            internal (Status, Output) Complete()
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

                            var status = _fasterKV.InternalCompletePendingRequestFromContext(_clientSession.ctx, _clientSession.ctx, _clientSession.FasterSession, _diskRequest, ref _pendingContext, true, out _);
                            Debug.Assert(status != Status.PENDING);
                            _result = (status, _pendingContext.output);
                            _pendingContext.Dispose();
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
            public (Status, Output) Complete()
            {
                if (status != Status.PENDING)
                    return (status, output);

                return readAsyncInternal.Complete();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync<Input, Output, Context, Functions>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Input input, Context context, long serialNo, CancellationToken token)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var diskRequest = default(AsyncIOContext<Key, Value>);
            Output output = default;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus = InternalRead(ref key, ref input, ref output, ref context, ref pcontext, clientSession.FasterSession, clientSession.ctx, serialNo);
                Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);
                Debug.Assert(internalStatus != OperationStatus.RETRY_LATER);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<Input, Output, Context, Functions>>(new ReadAsyncResult<Input, Output, Context, Functions>((Status)internalStatus, output));
                }
                else
                {
                    var status = HandleOperationStatus(clientSession.ctx, clientSession.ctx, ref pcontext, clientSession.FasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<ReadAsyncResult<Input, Output, Context, Functions>>(new ReadAsyncResult<Input, Output, Context, Functions>(status, output));
                }
            }
            finally
            {
                clientSession.ctx.serialNum = serialNo;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, clientSession, pcontext, diskRequest, token);
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> SlowReadAsync<Input, Output, Context, Functions>(
            FasterKV<Key, Value> @this,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            clientSession.ctx.asyncPendingCount++;
            clientSession.ctx.pendingReads.Add();

            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                diskRequest = await diskRequest.asyncOperation.Task;
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

        internal sealed class RmwAsyncInternal<Input, Output, Context, Functions>
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

            internal RmwAsyncInternal(FasterKV<Key, Value> fasterKV, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                _exception = default;
                _fasterKV = fasterKV;
                _clientSession = clientSession;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
            }

            internal ValueTask<RmwAsyncResult<Input, Output, Context, Functions>> CompleteAsync(CancellationToken token = default)
            {
                Debug.Assert(_fasterKV.RelaxedCPR);

                AsyncIOContext<Key, Value> newDiskRequest = default;

                if (_diskRequest.asyncOperation != null
                    && CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_clientSession.SupportAsync) _clientSession.UnsafeResumeThread();
                        try
                        {
                            var status = _fasterKV.InternalCompletePendingRequestFromContext(_clientSession.ctx, _clientSession.ctx, _clientSession.FasterSession, _diskRequest, ref _pendingContext, true, out newDiskRequest);
                            _pendingContext.Dispose();
                            if (status != Status.PENDING)
                                return new ValueTask<RmwAsyncResult<Input, Output, Context, Functions>>(new RmwAsyncResult<Input, Output, Context, Functions>(status, default));
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

                return SlowRmwAsync(_fasterKV, _clientSession, _pendingContext, newDiskRequest, token);
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct RmwAsyncResult<Input, Output, Context, Functions>
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            internal readonly Status status;
            internal readonly Output output;

            internal readonly RmwAsyncInternal<Input, Output, Context, Functions> rmwAsyncInternal;

            internal RmwAsyncResult(Status status, Output output)
            {
                this.status = status;
                this.output = output;
                this.rmwAsyncInternal = default;
            }

            internal RmwAsyncResult(
                FasterKV<Key, Value> fasterKV,
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                status = Status.PENDING;
                output = default;
                rmwAsyncInternal = new RmwAsyncInternal<Input, Output, Context, Functions>(fasterKV, clientSession, pendingContext, diskRequest);
            }

            /// <summary>
            /// Complete the RMW operation, issuing additional (rare) I/O asynchronously if needed.
            /// It is usually preferable to use Complete() instead of this.
            /// </summary>
            /// <returns>ValueTask for RMW result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<RmwAsyncResult<Input, Output, Context, Functions>> CompleteAsync(CancellationToken token = default)
            {
                if (status != Status.PENDING)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context, Functions>>(new RmwAsyncResult<Input, Output, Context, Functions>(status, default));

                return rmwAsyncInternal.CompleteAsync(token);
            }

            /// <summary>
            /// Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.
            /// </summary>
            /// <returns>Status of RMW operation</returns>
            public Status Complete()
            {
                if (status != Status.PENDING)
                    return status;

                var t = rmwAsyncInternal.CompleteAsync();
                if (t.IsCompleted)
                    return t.Result.status;

                // Handle rare case
                var r = t.GetAwaiter().GetResult();
                while (r.status == Status.PENDING)
                    r = r.CompleteAsync().GetAwaiter().GetResult();
                return r.status;
            }

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<RmwAsyncResult<Input, Output, Context, Functions>> RmwAsync<Input, Output, Context, Functions>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Input input, Context context, long serialNo, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var diskRequest = default(AsyncIOContext<Key, Value>);

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;

                do
                    internalStatus = InternalRMW(ref key, ref input, ref context, ref pcontext, clientSession.FasterSession, clientSession.ctx, serialNo);
                while (internalStatus == OperationStatus.RETRY_NOW || internalStatus == OperationStatus.RETRY_LATER);

                
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<RmwAsyncResult<Input, Output, Context, Functions>>(new RmwAsyncResult<Input, Output, Context, Functions>((Status)internalStatus, default));
                }
                else
                {
                    var status = HandleOperationStatus(clientSession.ctx, clientSession.ctx, ref pcontext, clientSession.FasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<RmwAsyncResult<Input, Output, Context, Functions>>(new RmwAsyncResult<Input, Output, Context, Functions>(status, default));
                }
            }
            finally
            {
                clientSession.ctx.serialNum = serialNo;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            return SlowRmwAsync(this, clientSession, pcontext, diskRequest, token);
        }

        private static async ValueTask<RmwAsyncResult<Input, Output, Context, Functions>> SlowRmwAsync<Input, Output, Context, Functions>(
            FasterKV<Key, Value> @this,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            clientSession.ctx.asyncPendingCount++;
            clientSession.ctx.pendingReads.Add();

            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                diskRequest = await diskRequest.asyncOperation.Task;
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

            return new RmwAsyncResult<Input, Output, Context, Functions>(@this, clientSession, pendingContext, diskRequest);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<Status> UpsertAsync<Input, Output, Context, Functions>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Value value, Context context, long serialNo, CancellationToken token)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                do
                    internalStatus = InternalUpsert(ref key, ref value, ref context, ref pcontext, clientSession.FasterSession, clientSession.ctx, serialNo);
                while (internalStatus == OperationStatus.RETRY_NOW);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<Status>((Status)internalStatus);
                }
                else
                {
                    var status = HandleOperationStatus(clientSession.ctx, clientSession.ctx, ref pcontext, clientSession.FasterSession, internalStatus, true, out _);
                    Debug.Assert(status != Status.PENDING);
                    return new ValueTask<Status>(status);
                }
            }
            finally
            {
                clientSession.ctx.serialNum = serialNo;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }
        }
    }
}

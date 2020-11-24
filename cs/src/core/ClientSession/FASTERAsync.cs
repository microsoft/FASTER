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
    {

        /// <summary>
        /// Check if at least one (sync) request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="currentCtx"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal async ValueTask ReadyToCompletePendingAsync<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> currentCtx, CancellationToken token = default)
        {
            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (currentCtx.phase == Phase.IN_PROGRESS || currentCtx.phase == Phase.WAIT_PENDING)
                {
                    if (currentCtx.prevCtx.SyncIoPendingCount != 0)
                        await currentCtx.prevCtx.readyResponses.WaitForEntryAsync(token);
                }
            }
            #endregion

            if (currentCtx.SyncIoPendingCount != 0)
                await currentCtx.readyResponses.WaitForEntryAsync(token);
        }

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, CancellationToken token = default)
        {
            bool done = true;

            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (currentCtx.phase == Phase.IN_PROGRESS
                    ||
                    currentCtx.phase == Phase.WAIT_PENDING)
                {

                    await currentCtx.prevCtx.pendingReads.WaitEmptyAsync();

                    await InternalCompletePendingRequestsAsync(currentCtx.prevCtx, currentCtx, fasterSession, token);
                    Debug.Assert(currentCtx.prevCtx.SyncIoPendingCount == 0);

                    if (currentCtx.prevCtx.retryRequests.Count > 0)
                    {
                        fasterSession.UnsafeResumeThread();
                        InternalCompleteRetryRequests(currentCtx.prevCtx, currentCtx, fasterSession);
                        fasterSession.UnsafeSuspendThread();
                    }

                    done &= (currentCtx.prevCtx.HasNoPendingRequests);
                }
            }
            #endregion

            await InternalCompletePendingRequestsAsync(currentCtx, currentCtx, fasterSession, token);

            fasterSession.UnsafeResumeThread();
            InternalCompleteRetryRequests(currentCtx, currentCtx, fasterSession);
            fasterSession.UnsafeSuspendThread();

            Debug.Assert(currentCtx.HasNoPendingRequests);

            done &= (currentCtx.HasNoPendingRequests);

            if (!done)
            {
                throw new Exception("CompletePendingAsync did not complete");
            }
        }

        internal sealed class ReadAsyncInternal<Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            PendingContext<Input, Output, Context> _pendingContext;
            readonly AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;
            internal RecordInfo _recordInfo;

            internal ReadAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession, FasterExecutionContext<Input, Output, Context> currentCtx,
                                       PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
                _recordInfo = default;
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
                        if (_exception == default)
                        {
                            _fasterSession.UnsafeResumeThread();
                            try
                            {
                                Debug.Assert(_fasterKV.RelaxedCPR);

                                var status = _fasterKV.InternalCompletePendingRequestFromContext(_currentCtx, _currentCtx, _fasterSession, _diskRequest, ref _pendingContext, true, out _);
                                Debug.Assert(status != Status.PENDING);
                                _result = (status, _pendingContext.output);
                                unsafe { _recordInfo = _fasterKV.hlog.GetInfoFromBytePointer(_diskRequest.record.GetValidPointer()); }
                                _pendingContext.Dispose();
                            }
                            finally
                            {
                                _fasterSession.UnsafeSuspendThread();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _currentCtx.ioPendingRequests.Remove(_pendingContext.id);
                        _currentCtx.asyncPendingCount--;
                        _currentCtx.pendingReads.Remove();
                    }
                }

                if (_exception != default)
                    _exception.Throw();
                return _result;
            }

            internal (Status, Output) Complete(out RecordInfo recordInfo)
            {
                var result = this.Complete();
                recordInfo = _recordInfo;
                return result;
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, Output, Context>
        {
            internal readonly Status status;
            internal readonly Output output;
            readonly RecordInfo recordInfo;

            internal readonly ReadAsyncInternal<Input, Output, Context> readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output, RecordInfo recordInfo)
            {
                this.status = status;
                this.output = output;
                this.recordInfo = recordInfo;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value> fasterKV,
                IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx,
                PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                status = Status.PENDING;
                output = default;
                this.recordInfo = default;
                readAsyncInternal = new ReadAsyncInternal<Input, Output, Context>(fasterKV, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
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

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result and the previous address in the Read key's hash chain, or throws an exception if error encountered.</returns>
            public (Status, Output) Complete(out RecordInfo recordInfo)
            {
                if (status != Status.PENDING)
                {
                    recordInfo = this.recordInfo;
                    return (status, output);
                }

                return readAsyncInternal.Complete(out recordInfo);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context>> ReadAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref Key key, ref Input input, long startAddress, Context context, long serialNo, CancellationToken token, byte operationFlags = 0)
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.operationFlags = operationFlags;
            var diskRequest = default(AsyncIOContext<Key, Value>);
            Output output = default;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus = InternalRead(ref key, ref input, ref output, startAddress, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
                Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);
                Debug.Assert(internalStatus != OperationStatus.RETRY_LATER);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>((Status)internalStatus, output, pcontext.recordInfo));
                }
                else
                {
                    var status = HandleOperationStatus(currentCtx, currentCtx, ref pcontext, fasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>(status, output, pcontext.recordInfo));
                }
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, fasterSession, currentCtx, pcontext, diskRequest, token);
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context>> SlowReadAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            currentCtx.asyncPendingCount++;
            currentCtx.pendingReads.Add();

            ExceptionDispatchInfo exceptionDispatchInfo = default;

            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                diskRequest = await diskRequest.asyncOperation.Task;
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }

            return new ReadAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
        }

        internal sealed class RmwAsyncInternal<Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            PendingContext<Input, Output, Context> _pendingContext;
            AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;

            internal RmwAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, 
                                      AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
            }

            internal ValueTask<RmwAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
            {
                Debug.Assert(_fasterKV.RelaxedCPR);

                AsyncIOContext<Key, Value> newDiskRequest = default;

                if (_diskRequest.asyncOperation != null
                    && CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                        {
                            _fasterSession.UnsafeResumeThread();
                            try
                            {
                                var status = _fasterKV.InternalCompletePendingRequestFromContext(_currentCtx, _currentCtx, _fasterSession, _diskRequest, ref _pendingContext, true, out newDiskRequest);
                                _pendingContext.Dispose();
                                if (status != Status.PENDING)
                                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, default));
                            }
                            finally
                            {
                                _fasterSession.UnsafeSuspendThread();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _currentCtx.ioPendingRequests.Remove(_pendingContext.id);
                        _currentCtx.asyncPendingCount--;
                        _currentCtx.pendingReads.Remove();
                    }
                }

                if (_exception != default)
                    _exception.Throw();

                return SlowRmwAsync(_fasterKV, _fasterSession, _currentCtx, _pendingContext, newDiskRequest, token);
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct RmwAsyncResult<Input, Output, Context>
        {
            internal readonly Status status;
            internal readonly Output output;

            internal readonly RmwAsyncInternal<Input, Output, Context> rmwAsyncInternal;

            internal RmwAsyncResult(Status status, Output output)
            {
                this.status = status;
                this.output = output;
                this.rmwAsyncInternal = default;
            }

            internal RmwAsyncResult(
                FasterKV<Key, Value> fasterKV,
                IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx,
                PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                status = Status.PENDING;
                output = default;
                rmwAsyncInternal = new RmwAsyncInternal<Input, Output, Context>(fasterKV, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
            }

            /// <summary>
            /// Complete the RMW operation, issuing additional (rare) I/O asynchronously if needed.
            /// It is usually preferable to use Complete() instead of this.
            /// </summary>
            /// <returns>ValueTask for RMW result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<RmwAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
            {
                if (status != Status.PENDING)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, default));

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
        internal ValueTask<RmwAsyncResult<Input, Output, Context>> RmwAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, ref Input input, Context context, long serialNo, CancellationToken token = default)
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var diskRequest = default(AsyncIOContext<Key, Value>);

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;

                do
                    internalStatus = InternalRMW(ref key, ref input, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
                while (internalStatus == OperationStatus.RETRY_NOW || internalStatus == OperationStatus.RETRY_LATER);

                
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>((Status)internalStatus, default));
                }
                else
                {
                    var status = HandleOperationStatus(currentCtx, currentCtx, ref pcontext, fasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, default));
                }
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowRmwAsync(this, fasterSession, currentCtx, pcontext, diskRequest, token);
        }

        private static async ValueTask<RmwAsyncResult<Input, Output, Context>> SlowRmwAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            currentCtx.asyncPendingCount++;
            currentCtx.pendingReads.Add();

            ExceptionDispatchInfo exceptionDispatchInfo = default;
            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                diskRequest = await diskRequest.asyncOperation.Task;
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }

            return new RmwAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
        }
    }
}

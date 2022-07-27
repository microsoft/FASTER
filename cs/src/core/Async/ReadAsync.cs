// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal sealed class ReadAsyncInternal<Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            PendingContext<Input, Output, Context> _pendingContext;
            ReadOptions _readOptions;
            AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;
            internal RecordMetadata _recordMetadata;

            internal ReadAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession, FasterExecutionContext<Input, Output, Context> currentCtx,
                                       PendingContext<Input, Output, Context> pendingContext, ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _readOptions = readOptions;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
                _recordMetadata = default;
            }

            internal (Status, Output) Complete()
            {
                (Status, Output) _result = default;
                if (CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                        {
                            _fasterSession.UnsafeResumeThread();
                            try
                            {
                                Status status;
                                do
                                {
                                    status = !this._diskRequest.IsDefault()
                                        ? _fasterKV.InternalCompletePendingRequestFromContext(_currentCtx, _currentCtx, _fasterSession, _diskRequest, ref _pendingContext, out var newDiskRequest)
                                        : _fasterKV.CallInternalRead(_fasterSession, _currentCtx, ref _pendingContext, ref _pendingContext.key.Get(), ref _pendingContext.input.Get(), ref _pendingContext.output,
                                                        ref _readOptions, _pendingContext.userContext, _pendingContext.serialNum, out newDiskRequest);
                                    this._diskRequest = newDiskRequest;
                                    Debug.Assert(this._diskRequest.IsDefault() || _pendingContext.flushEvent.IsDefault(), "Cannot have both flushEvent and diskRequest");
                                } while (status.IsPending);
                                _result = (status, _pendingContext.output);
                                _recordMetadata = new(_pendingContext.recordInfo, _pendingContext.logicalAddress);
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

            internal (Status, Output) Complete(out RecordMetadata recordMetadata)
            {
                var result = this.Complete();
                recordMetadata = _recordMetadata;
                return result;
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, Output, Context>
        {
            private readonly Status status;
            private readonly Output output;
            readonly RecordMetadata recordMetadata;

            internal readonly ReadAsyncInternal<Input, Output, Context> readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output, RecordMetadata recordMetadata)
            {
                this.status = status;
                this.output = output;
                this.recordMetadata = recordMetadata;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value> fasterKV,
                IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx,
                PendingContext<Input, Output, Context> pendingContext, ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                status = new(StatusCode.Pending);
                output = default;
                this.recordMetadata = default;
                readAsyncInternal = new ReadAsyncInternal<Input, Output, Context>(fasterKV, fasterSession, currentCtx, pendingContext, readOptions, diskRequest, exceptionDispatchInfo);
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result, or throws an exception if error encountered.</returns>
            public (Status status, Output output) Complete()
            {
                if (!status.IsPending)
                    return (status, output);
                return readAsyncInternal.Complete();
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result and the previous address in the Read key's hash chain, or throws an exception if error encountered.</returns>
            public (Status status, Output output) Complete(out RecordMetadata recordMetadata)
            {
                if (!status.IsPending)
                {
                    recordMetadata = this.recordMetadata;
                    return (status, output);
                }

                return readAsyncInternal.Complete(out recordMetadata);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context>> ReadAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref Key key, ref Input input, ref ReadOptions readOptions, Context context, long serialNo, CancellationToken token, bool noKey = false)
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            var operationFlags = PendingContext<Input, Output, Context>.GetOperationFlags(MergeReadFlags(currentCtx.ReadFlags, readOptions.ReadFlags), noKey);
            pcontext.SetOperationFlags(operationFlags, readOptions.StopAddress);
            var diskRequest = default(AsyncIOContext<Key, Value>);

            fasterSession.UnsafeResumeThread();
            try
            {
                Output output = default;
                var status = CallInternalRead(fasterSession, currentCtx, ref pcontext, ref key, ref input, ref output, ref readOptions, context, serialNo, out diskRequest);
                if (!status.IsPending)
                    return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, fasterSession, currentCtx, pcontext, readOptions, diskRequest, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status CallInternalRead<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context context, long serialNo,
            out AsyncIOContext<Key, Value> diskRequest)
        {
            diskRequest = default;
            OperationStatus internalStatus;
            do
            {
                internalStatus = InternalRead(ref key, ref input, ref output, readOptions.StartAddress, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
                Debug.Assert(internalStatus != OperationStatus.RETRY_LATER, "Read should not return RETRY_LATER");
            } while (HandleImmediateRetryStatus(internalStatus, currentCtx, currentCtx, fasterSession, ref pcontext));

            if (OperationStatusUtils.TryConvertToCompletedStatusCode(internalStatus, out Status status))
                return status;
            if (internalStatus == OperationStatus.ALLOCATE_FAILED)
            {
                Debug.Assert(!pcontext.flushEvent.IsDefault(), "Expected flushEvent");
                return new(StatusCode.Pending);
            }

            status = HandleOperationStatus(currentCtx, ref pcontext, internalStatus, out diskRequest);
            return status;
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context>> SlowReadAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo;
            (diskRequest, exceptionDispatchInfo) = await WaitForFlushOrIOCompletionAsync(@this, currentCtx, pcontext.flushEvent, diskRequest, token);
            pcontext.flushEvent = default;
            return new ReadAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, readOptions, diskRequest, exceptionDispatchInfo);
        }
    }
}

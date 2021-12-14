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
        // UpsertAsync, DeleteAsync, and RMWAsync can go pending when they generate Flush operations on BlockAllocate when inserting new records at the tail.
        // RMW can also go pending with a disk operation. Define some interfaces to and a shared UpdateAsyncInternal class to consolidate the code.

        internal interface IUpdateAsyncOperation<Input, Output, Context, TAsyncResult>
        {
            /// <summary>
            /// This creates an instance of the <typeparamref name="TAsyncResult"/>, for example <see cref="RmwAsyncResult{Input, Output, Context}"/>
            /// </summary>
            /// <param name="status">The status code; for this variant of <typeparamref name="TAsyncResult"/> intantiation, this will not be <see cref="Status.PENDING"/></param>
            /// <param name="output">The completed output of the operation, if any</param>
            /// <param name="recordMetadata">The record metadata from the operation (currently used by RMW only)</param>
            /// <returns></returns>
            TAsyncResult CreateResult(Status status, Output output, RecordMetadata recordMetadata);

            /// <summary>
            /// This performs the low-level synchronous operation for the implementation class of <typeparamref name="TAsyncResult"/>; for example,
            /// <see cref="FasterKV{Key, Value}.InternalRMW"/>.
            /// </summary>
            /// <param name="fasterKV">The <see cref="FasterKV{Key, Value}"/> instance the async call was made on</param>
            /// <param name="pendingContext">The <see cref="PendingContext{Input, Output, Context}"/> for the pending operation</param>
            /// <param name="fasterSession">The <see cref="IFasterSession{Key, Value, Input, Output, Context}"/> for this operation</param>
            /// <param name="currentCtx">The <see cref="FasterExecutionContext{Input, Output, Context}"/> for this operation</param>
            /// <param name="asyncOp">Whether this is from a synchronous or asynchronous completion call</param>
            /// <param name="flushEvent">The event to wait for flush completion on</param>
            /// <param name="output">The output to be populated by this operation</param>
            /// <returns></returns>
            Status DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, bool asyncOp, out CompletionEvent flushEvent, out Output output);
            /// <summary>
            /// Performs the asynchronous operation. This may be a wait for <paramref name="flushEvent"/> or a disk IO.
            /// </summary>
            /// <param name="fasterKV">The <see cref="FasterKV{Key, Value}"/> instance the async call was made on</param>
            /// <param name="fasterSession">The <see cref="IFasterSession{Key, Value, Input, Output, Context}"/> for this operation</param>
            /// <param name="currentCtx">The <see cref="FasterExecutionContext{Input, Output, Context}"/> for this operation</param>
            /// <param name="pendingContext">The <see cref="PendingContext{Input, Output, Context}"/> for the pending operation</param>
            /// <param name="flushEvent">The event to wait for flush completion on</param>
            /// <param name="token">The cancellation token, if any</param>
            /// <returns></returns>
            ValueTask<TAsyncResult> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext,
                                            CompletionEvent flushEvent, CancellationToken token);

            /// <summary>
            /// For RMW only, completes any pending IO; no-op for other implementations.
            /// </summary>
            /// <param name="fasterSession">The <see cref="IFasterSession{Key, Value, Input, Output, Context}"/> for this operation</param>
            /// <returns>Whether the pending operation was complete</returns>
            bool CompletePendingIO(IFasterSession<Key, Value, Input, Output, Context> fasterSession);

            /// <summary>
            /// For RMW only, decrements the count of pending IOs and async operations; no-op for other implementations.
            /// </summary>
            /// <param name="currentCtx">The <see cref="FasterExecutionContext{Input, Output, Context}"/> for this operation</param>
            /// <param name="pendingContext">The <see cref="PendingContext{Input, Output, Context}"/> for the pending operation</param>
            void DecrementPending(FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pendingContext);
        }

        internal sealed class UpdateAsyncInternal<Input, Output, Context, TAsyncOperation, TAsyncResult>
            where TAsyncOperation : IUpdateAsyncOperation<Input, Output, Context, TAsyncResult>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            TAsyncOperation _asyncOperation;
            PendingContext<Input, Output, Context> _pendingContext;
            int CompletionComputeStatus;

            internal UpdateAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext,
                                      ExceptionDispatchInfo exceptionDispatchInfo, TAsyncOperation asyncOperation)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _asyncOperation = asyncOperation;
                CompletionComputeStatus = Pending;
            }

            internal ValueTask<TAsyncResult> CompleteAsync(CancellationToken token = default)
            {
                Debug.Assert(_fasterKV.RelaxedCPR);

                // Note: We currently do not await anything here, and we must never do any post-await work inside CompleteAsync; this includes any code in
                // a 'finally' block. All post-await work must be re-initiated by end user on the mono-threaded session.

                if (TryCompleteAsyncState(asyncOp: true, out CompletionEvent flushEvent, out var asyncResult))
                    return new ValueTask<TAsyncResult>(asyncResult);

                if (_exception != default)
                    _exception.Throw();
                return _asyncOperation.DoSlowOperation(_fasterKV, _fasterSession, _currentCtx, _pendingContext, flushEvent, token);
            }

            internal TAsyncResult Complete()
            {
                if (!TryCompleteAsyncState(asyncOp: false, out CompletionEvent flushEvent, out TAsyncResult asyncResult))
                {
                    while (true) { 
                        if (_exception != default)
                            _exception.Throw();

                        if (!flushEvent.IsDefault())
                            flushEvent.Wait();
                        else if (_asyncOperation.CompletePendingIO(_fasterSession))
                            break;

                        if (this.TryCompleteSync(asyncOp: false, out flushEvent, out asyncResult))
                            break;
                    }
                }
                return asyncResult;
            }

            private bool TryCompleteAsyncState(bool asyncOp, out CompletionEvent flushEvent, out TAsyncResult asyncResult)
            {
                // This makes one attempt to complete the async operation's synchronous state, and clears the async pending counters.
                if (CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                            return TryCompleteSync(asyncOp, out flushEvent, out asyncResult);
                    }
                    finally
                    {
                        _asyncOperation.DecrementPending(_currentCtx, ref _pendingContext);
                    }
                }

                flushEvent = default;
                asyncResult = default;
                return false;
            }

            private bool TryCompleteSync(bool asyncOp, out CompletionEvent flushEvent, out TAsyncResult asyncResult)
            {
                _fasterSession.UnsafeResumeThread();
                try
                {
                    Status status = _asyncOperation.DoFastOperation(_fasterKV, ref _pendingContext, _fasterSession, _currentCtx, asyncOp, out flushEvent, out Output output);

                    if (status != Status.PENDING)
                    {
                        _pendingContext.Dispose();
                        asyncResult = _asyncOperation.CreateResult(status, output, new RecordMetadata(_pendingContext.recordInfo, _pendingContext.logicalAddress));
                        return true;
                    }
                }
                catch (Exception e)
                {
                    _exception = ExceptionDispatchInfo.Capture(e);
                    flushEvent = default;
                }
                finally
                {
                    _fasterSession.UnsafeSuspendThread();
                }

                asyncResult = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Status TranslateStatus(OperationStatus internalStatus)
        {
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                return (Status)internalStatus;
            Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
            return Status.PENDING;
        }

        private static async ValueTask<ExceptionDispatchInfo> WaitForFlushCompletionAsync<Input, Output, Context>(FasterKV<Key, Value> @this, FasterExecutionContext<Input, Output, Context> currentCtx, CompletionEvent flushEvent, CancellationToken token)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = default;
            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                await flushEvent.WaitAsync(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }
            return exceptionDispatchInfo;
        }
    }
}

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
        // UpsertAsync and DeleteAsync can only go pending when they generate Flush operations on BlockAllocate when inserting new records at the tail.
        // Define a couple interfaces to allow defining a shared UpdelAsyncInternal class rather than duplicating.

        internal interface IUpdelAsyncOperation<Input, Output, Context, TAsyncResult>
        {
            TAsyncResult CreateResult(OperationStatus internalStatus);

            OperationStatus DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx);
            ValueTask<TAsyncResult> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext,
                                            CompletionEvent flushEvent, CancellationToken token);
        }

        internal interface IUpdelAsyncResult<Input, Output, Context, TAsyncResult>
        {
            ValueTask<TAsyncResult> CompleteAsync(CancellationToken token = default);

            Status Status { get; }
        }

        internal struct UpsertAsyncOperation<Input, Output, Context> : IUpdelAsyncOperation<Input, Output, Context, UpsertAsyncResult<Input, Output, Context>>
        {
            public UpsertAsyncResult<Input, Output, Context> CreateResult(OperationStatus internalStatus) => new UpsertAsyncResult<Input, Output, Context>(internalStatus);

            public OperationStatus DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx)
                => fasterKV.InternalUpsert(ref pendingContext.key.Get(), ref pendingContext.value.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);

            public ValueTask<UpsertAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowUpsertAsync(fasterKV, fasterSession, currentCtx, pendingContext, flushEvent, token);
        }

        internal struct DeleteAsyncOperation<Input, Output, Context> : IUpdelAsyncOperation<Input, Output, Context, DeleteAsyncResult<Input, Output, Context>>
        {
            public DeleteAsyncResult<Input, Output, Context> CreateResult(OperationStatus internalStatus) => new DeleteAsyncResult<Input, Output, Context>(internalStatus);

            public OperationStatus DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx)
                => fasterKV.InternalDelete(ref pendingContext.key.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);

            public ValueTask<DeleteAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowDeleteAsync(fasterKV, fasterSession, currentCtx, pendingContext, flushEvent, token);
        }

        internal sealed class UpdelAsyncInternal<Input, Output, Context, TAsyncOperation, TAsyncResult>
            where TAsyncOperation : IUpdelAsyncOperation<Input, Output, Context, TAsyncResult>, new()
            where TAsyncResult : IUpdelAsyncResult<Input, Output, Context, TAsyncResult>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            internal readonly TAsyncOperation asyncOperation;
            PendingContext<Input, Output, Context> _pendingContext;
            int CompletionComputeStatus;

            internal UpdelAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext,
                                      ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                asyncOperation = new TAsyncOperation();
                CompletionComputeStatus = Pending;
            }

            internal ValueTask<TAsyncResult> CompleteAsync(CancellationToken token = default)
            {
                Debug.Assert(_fasterKV.RelaxedCPR);

                // Note: We currently do not await anything here, and we must never do any post-await work inside CompleteAsync; this includes any code in
                // a 'finally' block. All post-await work must be re-initiated by end user on the mono-threaded session.

                if (TryCompleteAsyncState(out CompletionEvent flushEvent, out var rmwAsyncResult))
                    return new ValueTask<TAsyncResult>(rmwAsyncResult);

                if (_exception != default)
                    _exception.Throw();
                return asyncOperation.DoSlowOperation(_fasterKV, _fasterSession, _currentCtx, _pendingContext, flushEvent, token);
            }

            internal bool TryCompleteAsyncState(out CompletionEvent flushEvent, out TAsyncResult asyncResult)
            {
                // This makes one attempt to complete the async operation's synchronous state, and clears the async pending counters.
                if (CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                            return TryCompleteSync(out flushEvent, out asyncResult);
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _currentCtx.asyncPendingCount--;
                    }
                }

                flushEvent = default;
                asyncResult = default;
                return false;
            }

            internal bool TryCompleteSync(out CompletionEvent flushEvent, out TAsyncResult asyncResult)
            {
                _fasterSession.UnsafeResumeThread();
                try
                {
                    OperationStatus internalStatus;
                    do
                    {
                        flushEvent = _fasterKV.hlog.FlushEvent;
                        internalStatus = asyncOperation.DoFastOperation(_fasterKV, ref _pendingContext, _fasterSession, _currentCtx);
                    } while (internalStatus == OperationStatus.RETRY_NOW);

                    if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                    {
                        _pendingContext.Dispose();
                        asyncResult = asyncOperation.CreateResult(internalStatus);
                        return true;
                    }
                    Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
                }
                finally
                {
                    _fasterSession.UnsafeSuspendThread();
                }

                asyncResult = default;
                return false;
            }

            internal Status Complete()
            {
                if (!TryCompleteAsyncState(out CompletionEvent flushEvent, out TAsyncResult asyncResult))
                {
                    flushEvent.Wait();
                    while (!this.TryCompleteSync(out flushEvent, out asyncResult))
                        flushEvent.Wait();
                }
                return asyncResult.Status;
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
            currentCtx.asyncPendingCount++;

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

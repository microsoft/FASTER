// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, StoreFunctions>
    {
        internal struct DeleteAsyncOperation<Input, Output, Context, Allocator> : IUpdateAsyncOperation<Input, Output, Context, Allocator, DeleteAsyncResult<Input, Output, Context, Allocator>>
        {
            /// <inheritdoc/>
            public DeleteAsyncResult<Input, Output, Context, Allocator> CreateResult(Status status, Output output, RecordMetadata recordMetadata) => new(status);

            /// <inheritdoc/>
            public Status DoFastOperation(FasterKV<Key, Value, StoreFunctions> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, bool asyncOp, out CompletionEvent flushEvent, out Output output)
            {
                OperationStatus internalStatus;
                do
                {
                    flushEvent = fasterKV.hlog.FlushEvent;
                    internalStatus = fasterKV.InternalDelete<Input, Output, Context, Allocator, IFasterSession<Key, Value, Input, Output, Context, Allocator>>(
                            ref pendingContext.key.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                } while (internalStatus == OperationStatus.RETRY_NOW);
                output = default;
                return TranslateStatus(internalStatus);
            }

            /// <inheritdoc/>
            public ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>> DoSlowOperation(FasterKV<Key, Value, StoreFunctions> fasterKV, IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowDeleteAsync(fasterKV, fasterSession, currentCtx, pendingContext, flushEvent, token);

            /// <inheritdoc/>
            public bool CompletePendingIO(IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession) => false;

            /// <inheritdoc/>
            public void DecrementPending(FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pendingContext) { }
        }

        /// <summary>
        /// State storage for the completion of an async Delete, or the result if the Delete was completed synchronously
        /// </summary>
        public struct DeleteAsyncResult<Input, Output, Context, Allocator>
        {
            internal readonly UpdateAsyncInternal<Input, Output, Context, Allocator, DeleteAsyncOperation<Input, Output, Context, Allocator>, DeleteAsyncResult<Input, Output, Context, Allocator>> updateAsyncInternal;

            /// <summary>Current status of the Upsert operation</summary>
            public Status Status { get; }

            internal DeleteAsyncResult(Status status)
            {
                this.Status = status;
                this.updateAsyncInternal = default;
            }

            internal DeleteAsyncResult(FasterKV<Key, Value, StoreFunctions> fasterKV, IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                this.Status = new(StatusCode.Pending);
                updateAsyncInternal = new UpdateAsyncInternal<Input, Output, Context, Allocator, DeleteAsyncOperation<Input, Output, Context, Allocator>, DeleteAsyncResult<Input, Output, Context, Allocator>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new DeleteAsyncOperation<Input, Output, Context, Allocator>());
            }

            /// <summary>Complete the Delete operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Delete result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>> CompleteAsync(CancellationToken token = default)
                => this.Status.IsPending
                    ? updateAsyncInternal.CompleteAsync(token)
                    : new ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>>(new DeleteAsyncResult<Input, Output, Context, Allocator>(this.Status));

            /// <summary>Complete the Delete operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Delete operation</returns>
            public Status Complete() => this.Status.IsPending ? updateAsyncInternal.Complete().Status : this.Status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>> DeleteAsync<Input, Output, Context, Allocator, FasterSession>(FasterSession fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, Context userContext, long serialNo, CancellationToken token = default)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context, Allocator>
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            return DeleteAsync<Input, Output, Context, Allocator, FasterSession>(fasterSession, currentCtx, ref pcontext, ref key, userContext, serialNo, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>> DeleteAsync<Input, Output, Context, Allocator, FasterSession>(FasterSession fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, Context userContext, long serialNo, CancellationToken token)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context, Allocator>
        {
            CompletionEvent flushEvent;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                do
                {
                    flushEvent = hlog.FlushEvent;
                    internalStatus = InternalDelete<Input, Output, Context, Allocator, IFasterSession<Key, Value, Input, Output, Context, Allocator>>(
                            ref key, ref userContext, ref pcontext, fasterSession, currentCtx, serialNo);
                } while (internalStatus == OperationStatus.RETRY_NOW);

                if (OperationStatusUtils.TryConvertToStatusCode(internalStatus, out Status status))
                    return new ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>>(new DeleteAsyncResult<Input, Output, Context, Allocator>(new(internalStatus)));

                Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowDeleteAsync(this, fasterSession, currentCtx, pcontext, flushEvent, token);
        }

        private static async ValueTask<DeleteAsyncResult<Input, Output, Context, Allocator>> SlowDeleteAsync<Input, Output, Context, Allocator>(
            FasterKV<Key, Value, StoreFunctions> @this,
            IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, CompletionEvent flushEvent, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = await WaitForFlushCompletionAsync(@this, currentCtx, flushEvent, token).ConfigureAwait(false);
            return new DeleteAsyncResult<Input, Output, Context, Allocator>(@this, fasterSession, currentCtx, pcontext, exceptionDispatchInfo);
        }
    }
}

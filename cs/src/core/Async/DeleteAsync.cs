// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal struct DeleteAsyncOperation<Input, Output, Context> : IUpdateAsyncOperation<Input, Output, Context, DeleteAsyncResult<Input, Output, Context>>
        {
            public DeleteAsyncResult<Input, Output, Context> CreateResult(OperationStatus internalStatus) => new DeleteAsyncResult<Input, Output, Context>(internalStatus);

            public OperationStatus DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx)
                => fasterKV.InternalDelete(ref pendingContext.key.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);

            public ValueTask<DeleteAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowDeleteAsync(fasterKV, fasterSession, currentCtx, pendingContext, flushEvent, token);

            public void DecrementPending(FasterExecutionContext<Input, Output, Context> currentCtx) => currentCtx.asyncPendingCount--;

            public Status GetStatus(DeleteAsyncResult<Input, Output, Context> asyncResult) => asyncResult.Status;
        }

        /// <summary>
        /// Contained state storage for the completion of an async Delete, or the result if the Delete was completed synchronously
        /// </summary>
        public struct DeleteAsyncResult<Input, Output, Context>
        {
            private readonly OperationStatus internalStatus;
            internal readonly UpdateAsyncInternal<Input, Output, Context, DeleteAsyncOperation<Input, Output, Context>, DeleteAsyncResult<Input, Output, Context>> updelAsyncInternal;

            /// <summary>Current status of the Upsert operation</summary>
            public Status Status => TranslateStatus(internalStatus);

            internal DeleteAsyncResult(OperationStatus internalStatus)
            {
                Debug.Assert(internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND);
                this.internalStatus = internalStatus;
                this.updelAsyncInternal = default;
            }

            internal DeleteAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                internalStatus = OperationStatus.ALLOCATE_FAILED;
                updelAsyncInternal = new UpdateAsyncInternal<Input, Output, Context, DeleteAsyncOperation<Input, Output, Context>, DeleteAsyncResult<Input, Output, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new DeleteAsyncOperation<Input, Output, Context>());
            }

            /// <summary>Complete the Delete operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Delete result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<DeleteAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
            {
                if (internalStatus != OperationStatus.ALLOCATE_FAILED)
                    return new ValueTask<DeleteAsyncResult<Input, Output, Context>>(new DeleteAsyncResult<Input, Output, Context>(internalStatus));
                return updelAsyncInternal.CompleteAsync(token);
            }

            /// <summary>Complete the Delete operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Delete operation</returns>
            public Status Complete()
            {
                if (internalStatus != OperationStatus.ALLOCATE_FAILED)
                    return this.Status;
                return updelAsyncInternal.Complete();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<DeleteAsyncResult<Input, Output, Context>> DeleteAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, Context userContext, long serialNo, CancellationToken token = default)
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            return DeleteAsync(fasterSession, currentCtx, ref pcontext, ref key, userContext, serialNo, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<DeleteAsyncResult<Input, Output, Context>> DeleteAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, Context userContext, long serialNo, CancellationToken token)
        {
            CompletionEvent flushEvent;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                do
                {
                    flushEvent = hlog.FlushEvent;
                    internalStatus = InternalDelete(ref key, ref userContext, ref pcontext, fasterSession, currentCtx, serialNo);
                } while (internalStatus == OperationStatus.RETRY_NOW);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                    return new ValueTask<DeleteAsyncResult<Input, Output, Context>>(new DeleteAsyncResult<Input, Output, Context>(internalStatus));
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

        private static async ValueTask<DeleteAsyncResult<Input, Output, Context>> SlowDeleteAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, CompletionEvent flushEvent, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = await WaitForFlushCompletionAsync(@this, currentCtx, flushEvent, token).ConfigureAwait(false);
            return new DeleteAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, exceptionDispatchInfo);
        }
    }
}

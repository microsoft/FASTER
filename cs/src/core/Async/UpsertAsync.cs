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
        internal struct UpsertAsyncOperation<Input, Output, Context> : IUpdateAsyncOperation<Input, Output, Context, UpsertAsyncResult<Input, Output, Context>>
        {
            /// <inheritdoc/>
            public UpsertAsyncResult<Input, Output, Context> CreateResult(Status status, Output output) => new UpsertAsyncResult<Input, Output, Context>(status);

            /// <inheritdoc/>
            public Status DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, bool asyncOp, out CompletionEvent flushEvent, out Output output)
            {
                OperationStatus internalStatus;
                do
                {
                    flushEvent = fasterKV.hlog.FlushEvent;
                    internalStatus = fasterKV.InternalUpsert(ref pendingContext.key.Get(), ref pendingContext.value.Get(), ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                } while (internalStatus == OperationStatus.RETRY_NOW);
                output = default;
                return TranslateStatus(internalStatus);
            }

            /// <inheritdoc/>
            public ValueTask<UpsertAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowUpsertAsync(fasterKV, fasterSession, currentCtx, pendingContext, flushEvent, token);

            /// <inheritdoc/>
            public bool CompletePendingIO(IFasterSession<Key, Value, Input, Output, Context> fasterSession) => false;

            /// <inheritdoc/>
            public void DecrementPending(FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pendingContext) { }

            /// <inheritdoc/>
            public Status GetStatus(UpsertAsyncResult<Input, Output, Context> asyncResult) => asyncResult.Status;
        }

        /// <summary>
        /// State storage for the completion of an async Upsert, or the result if the Upsert was completed synchronously
        /// </summary>
        public struct UpsertAsyncResult<Input, Output, Context>
        {
            internal readonly UpdateAsyncInternal<Input, Output, Context, UpsertAsyncOperation<Input, Output, Context>, UpsertAsyncResult<Input, Output, Context>> updateAsyncInternal;

            /// <summary>Current status of the Upsert operation</summary>
            public Status Status { get; }

            internal UpsertAsyncResult(Status status)
            {
                this.Status = status;
                this.updateAsyncInternal = default;
            }

            internal UpsertAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                this.Status = Status.PENDING;
                updateAsyncInternal = new UpdateAsyncInternal<Input, Output, Context, UpsertAsyncOperation<Input, Output, Context>, UpsertAsyncResult<Input, Output, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new UpsertAsyncOperation<Input, Output, Context>());
            }

            /// <summary>Complete the Upsert operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Upsert result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<UpsertAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default) 
                => this.Status != Status.PENDING
                    ? new ValueTask<UpsertAsyncResult<Input, Output, Context>>(new UpsertAsyncResult<Input, Output, Context>(this.Status))
                    : updateAsyncInternal.CompleteAsync(token);

            /// <summary>Complete the Upsert operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Upsert operation</returns>
            public Status Complete() => this.Status != Status.PENDING ? this.Status : updateAsyncInternal.Complete();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<UpsertAsyncResult<Input, Output, Context>> UpsertAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, ref Value value, Context userContext, long serialNo, CancellationToken token = default)
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            return UpsertAsync(fasterSession, currentCtx, ref pcontext, ref key, ref value, userContext, serialNo, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask<UpsertAsyncResult<Input, Output, Context>> UpsertAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, ref Value value, Context userContext, long serialNo, CancellationToken token)
        {
            CompletionEvent flushEvent;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                do
                {
                    flushEvent = hlog.FlushEvent;
                    internalStatus = InternalUpsert(ref key, ref value, ref userContext, ref pcontext, fasterSession, currentCtx, serialNo);
                } while (internalStatus == OperationStatus.RETRY_NOW);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                    return new ValueTask<UpsertAsyncResult<Input, Output, Context>>(new UpsertAsyncResult<Input, Output, Context>((Status)internalStatus));
                Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowUpsertAsync(this, fasterSession, currentCtx, pcontext, flushEvent, token);
        }

        private static async ValueTask<UpsertAsyncResult<Input, Output, Context>> SlowUpsertAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, CompletionEvent flushEvent, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = await WaitForFlushCompletionAsync(@this, currentCtx, flushEvent, token).ConfigureAwait(false);
            return new UpsertAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, exceptionDispatchInfo);
        }
    }
}

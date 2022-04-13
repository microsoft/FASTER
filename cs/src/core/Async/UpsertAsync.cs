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
        internal struct UpsertAsyncOperation<Input, Output, Context, Allocator> : IUpdateAsyncOperation<Input, Output, Context, Allocator, UpsertAsyncResult<Input, Output, Context, Allocator>>
        {
            /// <inheritdoc/>
            public UpsertAsyncResult<Input, Output, Context, Allocator> CreateResult(Status status, Output output, RecordMetadata recordMetadata) => new(status, output, recordMetadata);

            /// <inheritdoc/>
            public Status DoFastOperation(FasterKV<Key, Value, StoreFunctions> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, bool asyncOp, out CompletionEvent flushEvent, out Output output)
            {
                output = default;
                OperationStatus internalStatus;
                do
                {
                    flushEvent = fasterKV.hlog.FlushEvent;
                    internalStatus = fasterKV.InternalUpsert<Input, Output, Context, Allocator, IFasterSession<Key, Value, Input, Output, Context, Allocator>>(
                            ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.value.Get(), ref output, ref pendingContext.userContext, ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                } while (internalStatus == OperationStatus.RETRY_NOW);
                output = default;
                return TranslateStatus(internalStatus);
            }

            /// <inheritdoc/>
            public ValueTask<UpsertAsyncResult<Input, Output, Context, Allocator>> DoSlowOperation(FasterKV<Key, Value, StoreFunctions> fasterKV, IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowUpsertAsync(fasterKV, fasterSession, currentCtx, pendingContext, flushEvent, token);

            /// <inheritdoc/>
            public bool CompletePendingIO(IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession) => false;

            /// <inheritdoc/>
            public void DecrementPending(FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pendingContext) { }
        }

        /// <summary>
        /// State storage for the completion of an async Upsert, or the result if the Upsert was completed synchronously
        /// </summary>
        public struct UpsertAsyncResult<Input, TOutput, Context, Allocator>
        {
            internal readonly UpdateAsyncInternal<Input, TOutput, Context, Allocator, UpsertAsyncOperation<Input, TOutput, Context, Allocator>, UpsertAsyncResult<Input, TOutput, Context, Allocator>> updateAsyncInternal;

            /// <summary>Current status of the Upsert operation</summary>
            public Status Status { get; }

            /// <summary>Output of the Upsert operation</summary>
            public TOutput Output { get; }

            /// <summary>Metadata of the updated record</summary>
            public RecordMetadata RecordMetadata { get; }

            internal UpsertAsyncResult(Status status, TOutput output, RecordMetadata recordMetadata)
            {
                this.Status = status;
                this.Output = output;
                this.RecordMetadata = recordMetadata;
                this.updateAsyncInternal = default;
            }

            internal UpsertAsyncResult(FasterKV<Key, Value, StoreFunctions> fasterKV, IFasterSession<Key, Value, Input, TOutput, Context, Allocator> fasterSession,
                FasterExecutionContext<Input, TOutput, Context> currentCtx, PendingContext<Input, TOutput, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                this.Status = new(StatusCode.Pending);
                this.Output = default;
                this.RecordMetadata = default;
                updateAsyncInternal = new UpdateAsyncInternal<Input, TOutput, Context, Allocator, UpsertAsyncOperation<Input, TOutput, Context, Allocator>, UpsertAsyncResult<Input, TOutput, Context, Allocator>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new UpsertAsyncOperation<Input, TOutput, Context, Allocator>());
            }

            /// <summary>Complete the Upsert operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Upsert result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<UpsertAsyncResult<Input, TOutput, Context, Allocator>> CompleteAsync(CancellationToken token = default) 
                => this.Status.IsPending
                    ? updateAsyncInternal.CompleteAsync(token)
                    : new ValueTask<UpsertAsyncResult<Input, TOutput, Context, Allocator>>(new UpsertAsyncResult<Input, TOutput, Context, Allocator>(this.Status, this.Output, this.RecordMetadata));

            /// <summary>Complete the Upsert operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Upsert operation</returns>
            public Status Complete() => this.Status.IsPending ? updateAsyncInternal.Complete().Status : this.Status;

            /// <summary>Complete the Upsert operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status and Output of Upsert operation</returns>
            public (Status status, TOutput output) Complete(out RecordMetadata recordMetadata)
            {
                if (!this.Status.IsPending)
                {
                    recordMetadata = this.RecordMetadata;
                    return (this.Status, this.Output);
                }
                var upsertAsyncResult = updateAsyncInternal.Complete();
                recordMetadata = upsertAsyncResult.RecordMetadata;
                return (upsertAsyncResult.Status, upsertAsyncResult.Output);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<UpsertAsyncResult<Input, Output, Context, Allocator>> UpsertAsync<Input, Output, Context, Allocator, FasterSession>(FasterSession fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, ref Input input, ref Value value, Context userContext, long serialNo, CancellationToken token = default)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context, Allocator>
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            return UpsertAsync<Input, Output, Context, Allocator, FasterSession>(fasterSession, currentCtx, ref pcontext, ref key, ref input, ref value, userContext, serialNo, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask<UpsertAsyncResult<Input, Output, Context, Allocator>> UpsertAsync<Input, Output, Context, Allocator, FasterSession>(FasterSession fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, ref Input input, ref Value value, Context userContext, long serialNo, CancellationToken token)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context, Allocator>
        {
            CompletionEvent flushEvent;
            Output output = default;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                do
                {
                    flushEvent = hlog.FlushEvent;
                    internalStatus = InternalUpsert<Input, Output, Context, Allocator, IFasterSession<Key, Value, Input, Output, Context, Allocator>>(
                            ref key, ref input, ref value, ref output, ref userContext, ref pcontext, fasterSession, currentCtx, serialNo);
                } while (internalStatus == OperationStatus.RETRY_NOW);

                if (OperationStatusUtils.TryConvertToStatusCode(internalStatus, out Status status))
                    return new ValueTask<UpsertAsyncResult<Input, Output, Context, Allocator>>(new UpsertAsyncResult<Input, Output, Context, Allocator>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
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

        private static async ValueTask<UpsertAsyncResult<Input, Output, Context, Allocator>> SlowUpsertAsync<Input, Output, Context, Allocator>(
            FasterKV<Key, Value, StoreFunctions> @this,
            IFasterSession<Key, Value, Input, Output, Context, Allocator> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, CompletionEvent flushEvent, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = await WaitForFlushCompletionAsync(@this, currentCtx, flushEvent, token).ConfigureAwait(false);
            return new UpsertAsyncResult<Input, Output, Context, Allocator>(@this, fasterSession, currentCtx, pcontext, exceptionDispatchInfo);
        }
    }
}

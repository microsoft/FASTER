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
            public UpsertAsyncResult<Input, Output, Context> CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata) => new UpsertAsyncResult<Input, Output, Context>(status, output, recordMetadata);

            /// <inheritdoc/>
            public Status DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, out Output output)
            {
                output = default;
                OperationStatus internalStatus;
                do
                {
                    internalStatus = fasterKV.InternalUpsert(ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.value.Get(), ref output, ref pendingContext.userContext, ref pendingContext, 
                                                             fasterSession, currentCtx, pendingContext.serialNum);
                } while (fasterKV.HandleImmediateRetryStatus(internalStatus, currentCtx, currentCtx, fasterSession, ref pendingContext));
                return TranslateStatus(internalStatus);
            }

            /// <inheritdoc/>
            public ValueTask<UpsertAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowUpsertAsync(fasterKV, fasterSession, currentCtx, pendingContext, token);

            /// <inheritdoc/>
            public bool HasPendingIO => false;
        }

        /// <summary>
        /// State storage for the completion of an async Upsert, or the result if the Upsert was completed synchronously
        /// </summary>
        public struct UpsertAsyncResult<Input, TOutput, Context>
        {
            internal readonly UpdateAsyncInternal<Input, TOutput, Context, UpsertAsyncOperation<Input, TOutput, Context>, UpsertAsyncResult<Input, TOutput, Context>> updateAsyncInternal;

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

            internal UpsertAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, TOutput, Context> fasterSession,
                FasterExecutionContext<Input, TOutput, Context> currentCtx, PendingContext<Input, TOutput, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                this.Status = new(StatusCode.Pending);
                this.Output = default;
                this.RecordMetadata = default;
                updateAsyncInternal = new UpdateAsyncInternal<Input, TOutput, Context, UpsertAsyncOperation<Input, TOutput, Context>, UpsertAsyncResult<Input, TOutput, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new ());
            }

            /// <summary>Complete the Upsert operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Upsert result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<UpsertAsyncResult<Input, TOutput, Context>> CompleteAsync(CancellationToken token = default) 
                => this.Status.IsPending
                    ? updateAsyncInternal.CompleteAsync(token)
                    : new ValueTask<UpsertAsyncResult<Input, TOutput, Context>>(new UpsertAsyncResult<Input, TOutput, Context>(this.Status, this.Output, this.RecordMetadata));

            /// <summary>Complete the Upsert operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Upsert operation</returns>
            public Status Complete() => this.Status.IsPending ? updateAsyncInternal.CompleteSync().Status : this.Status;

            /// <summary>Complete the Upsert operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status and Output of Upsert operation</returns>
            public (Status status, TOutput output) Complete(out RecordMetadata recordMetadata)
            {
                if (!this.Status.IsPending)
                {
                    recordMetadata = this.RecordMetadata;
                    return (this.Status, this.Output);
                }
                var upsertAsyncResult = updateAsyncInternal.CompleteSync();
                recordMetadata = upsertAsyncResult.RecordMetadata;
                return (upsertAsyncResult.Status, upsertAsyncResult.Output);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<UpsertAsyncResult<Input, Output, Context>> UpsertAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, ref Input input, ref Value value, Context userContext, long serialNo, CancellationToken token = default)
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            return UpsertAsync(fasterSession, currentCtx, ref pcontext, ref key, ref input, ref value, userContext, serialNo, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask<UpsertAsyncResult<Input, Output, Context>> UpsertAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, ref Input input, ref Value value, Context userContext, long serialNo, CancellationToken token)
        {
            Output output = default;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                do
                {
                    internalStatus = InternalUpsert(ref key, ref input, ref value, ref output, ref userContext, ref pcontext, fasterSession, currentCtx, serialNo);
                } while (HandleImmediateRetryStatus(internalStatus, currentCtx, currentCtx, fasterSession, ref pcontext));

                if (OperationStatusUtils.TryConvertToCompletedStatusCode(internalStatus, out Status status))
                    return new ValueTask<UpsertAsyncResult<Input, Output, Context>>(new UpsertAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
                Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowUpsertAsync(this, fasterSession, currentCtx, pcontext, token);
        }

        private static async ValueTask<UpsertAsyncResult<Input, Output, Context>> SlowUpsertAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = await WaitForFlushCompletionAsync(@this, pcontext.flushEvent, token).ConfigureAwait(false);
            pcontext.flushEvent = default;
            return new UpsertAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, exceptionDispatchInfo);
        }
    }
}

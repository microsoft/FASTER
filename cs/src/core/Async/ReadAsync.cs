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
        internal struct ReadAsyncOperation<Input, Output, Context> : IUpdateAsyncOperation<Input, Output, Context, ReadAsyncResult<Input, Output, Context>>
        {
            AsyncIOContext<Key, Value> diskRequest;
            ReadOptions readOptions;

            internal ReadAsyncOperation(AsyncIOContext<Key, Value> diskRequest, ref ReadOptions readOptions)
            {
                this.diskRequest = diskRequest;
                this.readOptions = readOptions;
            }

            /// <inheritdoc/>
            public ReadAsyncResult<Input, Output, Context> CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata) => new(status, output, recordMetadata);

            /// <inheritdoc/>
            public Status DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, out Output output)
            {
                Status status = !this.diskRequest.IsDefault()
                    ? fasterKV.InternalCompletePendingRequestFromContext(currentCtx, currentCtx, fasterSession, this.diskRequest, ref pendingContext, out var newDiskRequest)
                    : fasterKV.CallInternalRead(fasterSession, currentCtx, ref pendingContext, ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.output,
                                    ref this.readOptions, pendingContext.userContext, pendingContext.serialNum, out newDiskRequest);
                output = pendingContext.output;
                this.diskRequest = newDiskRequest;
                return status;
            }

            /// <inheritdoc/>
            public ValueTask<ReadAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowReadAsync(fasterKV, fasterSession, currentCtx, pendingContext, this.readOptions, this.diskRequest, token);

            /// <inheritdoc/>
            public bool HasPendingIO => !this.diskRequest.IsDefault();
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the Read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, TOutput, Context>
        {
            internal readonly UpdateAsyncInternal<Input, TOutput, Context, ReadAsyncOperation<Input, TOutput, Context>, ReadAsyncResult<Input, TOutput, Context>> updateAsyncInternal;

            /// <summary>Current status of the RMW operation</summary>
            public Status Status { get; }

            /// <summary>Output of the RMW operation if current status is not pending</summary>
            public TOutput Output { get; }

            /// <summary>Metadata of the updated record</summary>
            public RecordMetadata RecordMetadata { get; }

            internal ReadAsyncResult(Status status, TOutput output, RecordMetadata recordMetadata)
            {
                this.Status = status;
                this.Output = output;
                this.RecordMetadata = recordMetadata;
                this.updateAsyncInternal = default;
            }

            internal ReadAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, TOutput, Context> fasterSession,
                FasterExecutionContext<Input, TOutput, Context> currentCtx, PendingContext<Input, TOutput, Context> pendingContext,
                ref ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = new(StatusCode.Pending);
                this.Output = default;
                this.RecordMetadata = default;
                updateAsyncInternal = new UpdateAsyncInternal<Input, TOutput, Context, ReadAsyncOperation<Input, TOutput, Context>, ReadAsyncResult<Input, TOutput, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new ReadAsyncOperation<Input, TOutput, Context>(diskRequest, ref readOptions));
            }

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.</summary>
            /// <returns>Status of RMW operation</returns>
            public (Status status, TOutput output) Complete()
                => Complete(out _);

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.</summary>
            /// <returns>Status of RMW operation</returns>
            public (Status status, TOutput output) Complete(out RecordMetadata recordMetadata)
            {
                if (!this.Status.IsPending)
                {
                    recordMetadata = this.RecordMetadata;
                    return (this.Status, this.Output);
                }
                var readAsyncResult = updateAsyncInternal.CompleteSync();
                recordMetadata = readAsyncResult.RecordMetadata;
                return (readAsyncResult.Status, readAsyncResult.Output);
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
                internalStatus = InternalRead(ref key, ref input, ref output, readOptions.StartAddress, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, currentCtx, currentCtx, fasterSession, ref pcontext));

            return HandleOperationStatus(currentCtx, ref pcontext, internalStatus, out diskRequest);
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context>> SlowReadAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pcontext, ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo;
            (diskRequest, exceptionDispatchInfo) = await WaitForFlushOrIOCompletionAsync(@this, currentCtx, pcontext.flushEvent, diskRequest, token);
            pcontext.flushEvent = default;
            return new ReadAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, ref readOptions, diskRequest, exceptionDispatchInfo);
        }
    }
}

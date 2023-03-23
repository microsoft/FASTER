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
        internal struct RmwAsyncOperation<Input, Output, Context> : IAsyncOperation<Input, Output, Context, RmwAsyncResult<Input, Output, Context>>
        {
            AsyncIOContext<Key, Value> diskRequest;
            internal RmwAsyncOperation(AsyncIOContext<Key, Value> diskRequest) => this.diskRequest = diskRequest;

            /// <inheritdoc/>
            public RmwAsyncResult<Input, Output, Context> CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata) => new(status, output, recordMetadata);

            /// <inheritdoc/>
            public Status DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            out Output output)
            {
                Status status = !this.diskRequest.IsDefault()
                    ? fasterKV.InternalCompletePendingRequestFromContext(fasterSession, this.diskRequest, ref pendingContext, out AsyncIOContext<Key, Value> newDiskRequest)
                    : fasterKV.CallInternalRMW(fasterSession, ref pendingContext, ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.output, pendingContext.userContext,
                                                     pendingContext.serialNum, out newDiskRequest);
                output = pendingContext.output;
                this.diskRequest = newDiskRequest;
                return status;
            }

            /// <inheritdoc/>
            public ValueTask<RmwAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowRmwAsync(fasterKV, fasterSession, pendingContext, diskRequest, token);

            /// <inheritdoc/>
            public bool HasPendingIO => !this.diskRequest.IsDefault();
        }

        /// <summary>
        /// State storage for the completion of an async RMW, or the result if the RMW was completed synchronously
        /// </summary>
        public struct RmwAsyncResult<Input, TOutput, Context>
        {
            internal readonly AsyncOperationInternal<Input, TOutput, Context, RmwAsyncOperation<Input, TOutput, Context>, RmwAsyncResult<Input, TOutput, Context>> updateAsyncInternal;

            /// <summary>Current status of the RMW operation</summary>
            public Status Status { get; }

            /// <summary>Output of the RMW operation if current status is not pending</summary>
            public TOutput Output { get; }

            /// <summary>Metadata of the updated record</summary>
            public RecordMetadata RecordMetadata { get; }

            internal RmwAsyncResult(Status status, TOutput output, RecordMetadata recordMetadata)
            {
                Debug.Assert(!status.IsPending);
                this.Status = status;
                this.Output = output;
                this.RecordMetadata = recordMetadata;
                this.updateAsyncInternal = default;
            }

            internal RmwAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, TOutput, Context> fasterSession,
                PendingContext<Input, TOutput, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = new(StatusCode.Pending);
                this.Output = default;
                this.RecordMetadata = default;
                updateAsyncInternal = new AsyncOperationInternal<Input, TOutput, Context, RmwAsyncOperation<Input, TOutput, Context>, RmwAsyncResult<Input, TOutput, Context>>(
                                        fasterKV, fasterSession, pendingContext, exceptionDispatchInfo, new (diskRequest));
            }

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for RMW result. User needs to await again if result status is pending.</returns>
            public ValueTask<RmwAsyncResult<Input, TOutput, Context>> CompleteAsync(CancellationToken token = default) 
                => this.Status.IsPending
                    ? updateAsyncInternal.CompleteAsync(token)
                    : new ValueTask<RmwAsyncResult<Input, TOutput, Context>>(new RmwAsyncResult<Input, TOutput, Context>(this.Status, this.Output, this.RecordMetadata));

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
                var rmwAsyncResult = updateAsyncInternal.CompleteSync();
                recordMetadata = rmwAsyncResult.RecordMetadata;
                return (rmwAsyncResult.Status, rmwAsyncResult.Output);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<RmwAsyncResult<Input, Output, Context>> RmwAsync<Input, Output, Context, FasterSession>(FasterSession fasterSession,
                ref Key key, ref Input input, Context context, long serialNo, CancellationToken token = default)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            var diskRequest = default(AsyncIOContext<Key, Value>);

            fasterSession.UnsafeResumeThread();
            try
            {
                Output output = default;
                var status = CallInternalRMW(fasterSession, ref pcontext, ref key, ref input, ref output, context, serialNo, out diskRequest);
                if (!status.IsPending)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
            }
            finally
            {
                Debug.Assert(serialNo >= fasterSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
                fasterSession.Ctx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowRmwAsync(this, fasterSession, pcontext, diskRequest, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status CallInternalRMW<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession, ref PendingContext<Input, Output, Context> pcontext,
                    ref Key key, ref Input input, ref Output output, Context context, long serialNo, out AsyncIOContext<Key, Value> diskRequest)
        {
            OperationStatus internalStatus;
            do
                internalStatus = InternalRMW(ref key, ref input, ref output, ref context, ref pcontext, fasterSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, fasterSession, ref pcontext));

            return HandleOperationStatus(fasterSession.Ctx, ref pcontext, internalStatus, out diskRequest);
        }

        private static async ValueTask<RmwAsyncResult<Input, Output, Context>> SlowRmwAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            PendingContext<Input, Output, Context> pcontext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo;
            (diskRequest, exceptionDispatchInfo) = await WaitForFlushOrIOCompletionAsync(@this, fasterSession.Ctx, pcontext.flushEvent, diskRequest, token);
            pcontext.flushEvent = default;
            return new RmwAsyncResult<Input, Output, Context>(@this, fasterSession, pcontext, diskRequest, exceptionDispatchInfo);
        }
    }
}

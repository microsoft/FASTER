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
        internal struct RmwAsyncOperation<Input, Output, Context> : IUpdateAsyncOperation<Input, Output, Context, RmwAsyncResult<Input, Output, Context>>
        {
            AsyncIOContext<Key, Value> diskRequest;

            internal RmwAsyncOperation(AsyncIOContext<Key, Value> diskRequest) => this.diskRequest = diskRequest;

            public RmwAsyncResult<Input, Output, Context> CreateResult(Status status, Output output) => new RmwAsyncResult<Input, Output, Context>(status, output);

            public Status DoFastOperation(FasterKV<Key, Value> fasterKV, ref PendingContext<Input, Output, Context> pendingContext, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, bool asyncOp, out CompletionEvent flushEvent, out Output output)
            {
                output = default;
                flushEvent = fasterKV.hlog.FlushEvent;
                Status status = !this.diskRequest.IsDefault()
                    ? fasterKV.InternalCompletePendingRequestFromContext(currentCtx, currentCtx, fasterSession, this.diskRequest, ref pendingContext, asyncOp, out AsyncIOContext<Key, Value> newDiskRequest)
                    : fasterKV.CallInternalRMW(fasterSession, currentCtx, ref pendingContext, ref pendingContext.key.Get(), ref pendingContext.input.Get(), pendingContext.userContext,
                                                     pendingContext.serialNum, asyncOp, out flushEvent, out newDiskRequest);

                if (status == Status.PENDING && !newDiskRequest.IsDefault())
                {
                    flushEvent = default;
                    this.diskRequest = newDiskRequest;
                }
                return status;
            }

            public ValueTask<RmwAsyncResult<Input, Output, Context>> DoSlowOperation(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, CompletionEvent flushEvent, CancellationToken token)
                => SlowRmwAsync(fasterKV, fasterSession, currentCtx, pendingContext, diskRequest, flushEvent, token);

            public bool CompletePendingIO(IFasterSession<Key, Value, Input, Output, Context> fasterSession)
            {
                if (this.diskRequest.IsDefault())
                    return false;

                // CompletePending() may encounter OperationStatus.ALLOCATE_FAILED; if so, we don't have a more current flushEvent to pass back.
                fasterSession.CompletePendingWithOutputs(out var completedOutputs, wait: true, spinWaitForCommit: false);
                var status = completedOutputs.Next() ? completedOutputs.Current.Status : Status.ERROR;
                completedOutputs.Dispose();
                return status != Status.PENDING;
            }

            public void DecrementPending(FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pendingContext)
            {
                currentCtx.ioPendingRequests.Remove(pendingContext.id);
                currentCtx.asyncPendingCount--;
                currentCtx.pendingReads.Remove();
            }

            public Status GetStatus(RmwAsyncResult<Input, Output, Context> asyncResult) => asyncResult.Status;
        }

        /// <summary>
        /// State storage for the completion of an async RMW, or the result if the RMW was completed synchronously
        /// </summary>
        public struct RmwAsyncResult<Input, Output, Context>
        {
            internal readonly UpdateAsyncInternal<Input, Output, Context, RmwAsyncOperation<Input, Output, Context>, RmwAsyncResult<Input, Output, Context>> updateAsyncInternal;
            internal readonly Output output;

            /// <summary>Current status of the RMW operation</summary>
            public Status Status { get; }

            internal RmwAsyncResult(Status status, Output output)
            {
                this.Status = status;
                this.output = output;
                this.updateAsyncInternal = default;
            }

            internal RmwAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext,
                AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = Status.PENDING;
                output = default;
                updateAsyncInternal = new UpdateAsyncInternal<Input, Output, Context, RmwAsyncOperation<Input, Output, Context>, RmwAsyncResult<Input, Output, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo, new RmwAsyncOperation<Input, Output, Context>(diskRequest));
            }

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for RMW result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<RmwAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default) 
                => this.Status != Status.PENDING
                    ? new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(this.Status, default))
                    : updateAsyncInternal.CompleteAsync(token);

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.</summary>
            /// <returns>Status of RMW operation</returns>
            public Status Complete() => this.Status != Status.PENDING ? this.Status : updateAsyncInternal.Complete();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<RmwAsyncResult<Input, Output, Context>> RmwAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, ref Input input, Context context, long serialNo, CancellationToken token = default)
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.IsAsync = true;
            return RmwAsync(fasterSession, currentCtx, ref pcontext, ref key, ref input, context, serialNo, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<RmwAsyncResult<Input, Output, Context>> RmwAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, ref Input input, Context context, long serialNo, CancellationToken token)
        {
            var diskRequest = default(AsyncIOContext<Key, Value>);
            CompletionEvent flushEvent;

            fasterSession.UnsafeResumeThread();
            try
            {
                var status = CallInternalRMW(fasterSession, currentCtx, ref pcontext, ref key, ref input, context, serialNo, asyncOp: true, out flushEvent, out diskRequest);
                if (status != Status.PENDING)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, default));
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowRmwAsync(this, fasterSession, currentCtx, pcontext, diskRequest, flushEvent, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status CallInternalRMW<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref PendingContext<Input, Output, Context> pcontext, ref Key key, ref Input input, Context context, long serialNo,
            bool asyncOp, out CompletionEvent flushEvent, out AsyncIOContext<Key, Value> diskRequest)
        {
            diskRequest = default;
            OperationStatus internalStatus;
            do
            {
                flushEvent = hlog.FlushEvent;
                internalStatus = InternalRMW(ref key, ref input, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
            } while (internalStatus == OperationStatus.RETRY_NOW || internalStatus == OperationStatus.RETRY_LATER);

            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                return (Status)internalStatus;
            if (internalStatus == OperationStatus.ALLOCATE_FAILED)
                return Status.PENDING;    // This plus diskRequest.IsDefault() means allocate failed

            flushEvent = default;
            return HandleOperationStatus(currentCtx, currentCtx, ref pcontext, fasterSession, internalStatus, asyncOp, out diskRequest);
        }

        private static async ValueTask<RmwAsyncResult<Input, Output, Context>> SlowRmwAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pcontext,
            AsyncIOContext<Key, Value> diskRequest, CompletionEvent flushEvent, CancellationToken token = default)
        {
            currentCtx.asyncPendingCount++;
            currentCtx.pendingReads.Add();

            ExceptionDispatchInfo exceptionDispatchInfo = default;
            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                // If we are here because of flushEvent, then _diskRequest is default--there is no pending disk operation.
                if (diskRequest.IsDefault())
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                else
                {
                    using (token.Register(() => diskRequest.asyncOperation.TrySetCanceled()))
                        diskRequest = await diskRequest.asyncOperation.Task.WithCancellationAsync(token).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }

            return new RmwAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, diskRequest, exceptionDispatchInfo);
        }
    }
}

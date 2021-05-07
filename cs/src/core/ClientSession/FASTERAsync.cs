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
    /// <summary>
    /// The FASTER key-value store
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        #region CompletePendingAsync
        /// <summary>
        /// Check if at least one (sync) request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="currentCtx"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal async ValueTask ReadyToCompletePendingAsync<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> currentCtx, CancellationToken token = default)
        {
            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (currentCtx.phase == Phase.IN_PROGRESS || currentCtx.phase == Phase.WAIT_PENDING)
                {
                    if (currentCtx.prevCtx.SyncIoPendingCount != 0)
                        await currentCtx.prevCtx.readyResponses.WaitForEntryAsync(token);
                }
            }
            #endregion

            if (currentCtx.SyncIoPendingCount != 0)
                await currentCtx.readyResponses.WaitForEntryAsync(token);
        }

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, CancellationToken token,
                                      CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs)
        {
            while (true)
            {
                bool done = true;

                #region Previous pending requests
                if (!RelaxedCPR)
                {
                    if (currentCtx.phase == Phase.IN_PROGRESS || currentCtx.phase == Phase.WAIT_PENDING)
                    {
                        fasterSession.UnsafeResumeThread();
                        try
                        {
                            InternalCompletePendingRequests(currentCtx.prevCtx, currentCtx, fasterSession, completedOutputs);
                            InternalCompleteRetryRequests(currentCtx.prevCtx, currentCtx, fasterSession);
                        }
                        finally
                        {
                            fasterSession.UnsafeSuspendThread();
                        }
                        await currentCtx.prevCtx.WaitPendingAsync(token);
                        done &= currentCtx.prevCtx.HasNoPendingRequests;
                    }
                }
                #endregion

                fasterSession.UnsafeResumeThread();
                try
                {
                    InternalCompletePendingRequests(currentCtx, currentCtx, fasterSession, completedOutputs);
                    InternalCompleteRetryRequests(currentCtx, currentCtx, fasterSession);
                }
                finally
                {
                    fasterSession.UnsafeSuspendThread();
                }

                await currentCtx.WaitPendingAsync(token);
                done &= currentCtx.HasNoPendingRequests;

                if (done) return;

                InternalRefresh(currentCtx, fasterSession);

                Thread.Yield();
            }
        }
        #endregion CompletePendingAsync

        #region ReadAsync
        internal sealed class ReadAsyncInternal<Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            PendingContext<Input, Output, Context> _pendingContext;
            readonly AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;
            internal RecordInfo _recordInfo;

            internal ReadAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession, FasterExecutionContext<Input, Output, Context> currentCtx,
                                       PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
                _recordInfo = default;
            }

            internal (Status, Output) Complete()
            {
                (Status, Output) _result = default;
                if (_diskRequest.asyncOperation != null
                    && CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                        {
                            _fasterSession.UnsafeResumeThread();
                            try
                            {
                                Debug.Assert(_fasterKV.RelaxedCPR);

                                var status = _fasterKV.InternalCompletePendingRequestFromContext(_currentCtx, _currentCtx, _fasterSession, _diskRequest, ref _pendingContext, true, out _);
                                Debug.Assert(status != Status.PENDING);
                                _result = (status, _pendingContext.output);
                                _recordInfo = _pendingContext.recordInfo;
                                _pendingContext.Dispose();
                            }
                            finally
                            {
                                _fasterSession.UnsafeSuspendThread();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _currentCtx.ioPendingRequests.Remove(_pendingContext.id);
                        _currentCtx.asyncPendingCount--;
                        _currentCtx.pendingReads.Remove();
                    }
                }

                if (_exception != default)
                    _exception.Throw();
                return _result;
            }

            internal (Status, Output) Complete(out RecordInfo recordInfo)
            {
                var result = this.Complete();
                recordInfo = _recordInfo;
                return result;
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, Output, Context>
        {
            internal readonly Status status;
            internal readonly Output output;
            readonly RecordInfo recordInfo;

            internal readonly ReadAsyncInternal<Input, Output, Context> readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output, RecordInfo recordInfo)
            {
                this.status = status;
                this.output = output;
                this.recordInfo = recordInfo;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value> fasterKV,
                IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx,
                PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                status = Status.PENDING;
                output = default;
                this.recordInfo = default;
                readAsyncInternal = new ReadAsyncInternal<Input, Output, Context>(fasterKV, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result, or throws an exception if error encountered.</returns>
            public (Status status, Output output) Complete()
            {
                if (status != Status.PENDING)
                    return (status, output);

                return readAsyncInternal.Complete();
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result and the previous address in the Read key's hash chain, or throws an exception if error encountered.</returns>
            public (Status status, Output output) Complete(out RecordInfo recordInfo)
            {
                if (status != Status.PENDING)
                {
                    recordInfo = this.recordInfo;
                    return (status, output);
                }

                return readAsyncInternal.Complete(out recordInfo);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context>> ReadAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref Key key, ref Input input, long startAddress, Context context, long serialNo, CancellationToken token, byte operationFlags = 0)
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.operationFlags = operationFlags;
            var diskRequest = default(AsyncIOContext<Key, Value>);
            Output output = default;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus = InternalRead(ref key, ref input, ref output, startAddress, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
                Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);
                Debug.Assert(internalStatus != OperationStatus.RETRY_LATER);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>((Status)internalStatus, output, pcontext.recordInfo));
                }
                else
                {
                    var status = HandleOperationStatus(currentCtx, currentCtx, ref pcontext, fasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>(status, output, pcontext.recordInfo));
                }
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, fasterSession, currentCtx, pcontext, diskRequest, token);
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context>> SlowReadAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            currentCtx.asyncPendingCount++;
            currentCtx.pendingReads.Add();

            ExceptionDispatchInfo exceptionDispatchInfo = default;

            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                using (token.Register(() => diskRequest.asyncOperation.TrySetCanceled()))
                    diskRequest = await diskRequest.asyncOperation.Task;
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }

            return new ReadAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
        }
        #endregion ReadAsync

        #region UpdelAsync

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
        #endregion UpdelAsync

        #region UpsertAsync
        /// <summary>
        /// State storage for the completion of an async Upsert, or the result if the Upsert was completed synchronously
        /// </summary>
        public struct UpsertAsyncResult<Input, Output, Context> : IUpdelAsyncResult<Input, Output, Context, UpsertAsyncResult<Input, Output, Context>>
        {
            private readonly OperationStatus internalStatus;
            internal readonly UpdelAsyncInternal<Input, Output, Context, UpsertAsyncOperation<Input, Output, Context>, UpsertAsyncResult<Input, Output, Context>> updelAsyncInternal;

            /// <summary>Current status of the Upsert operation</summary>
            public Status Status => TranslateStatus(internalStatus);

            internal UpsertAsyncResult(OperationStatus internalStatus)
            {
                Debug.Assert(internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND);
                this.internalStatus = internalStatus;
                this.updelAsyncInternal = default;
            }

            internal UpsertAsyncResult(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                internalStatus = OperationStatus.ALLOCATE_FAILED;
                updelAsyncInternal = new UpdelAsyncInternal<Input, Output, Context, UpsertAsyncOperation<Input, Output, Context>, UpsertAsyncResult<Input, Output, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo);
            }

            /// <summary>Complete the Upsert operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Upsert result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<UpsertAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
            {
                if (internalStatus != OperationStatus.ALLOCATE_FAILED)
                    return new ValueTask<UpsertAsyncResult<Input, Output, Context>>(new UpsertAsyncResult<Input, Output, Context>(internalStatus));
                return updelAsyncInternal.CompleteAsync(token);
            }

            /// <summary>Complete the Upsert operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Upsert operation</returns>
            public Status Complete()
            {
                if (internalStatus != OperationStatus.ALLOCATE_FAILED)
                    return this.Status;
                return updelAsyncInternal.Complete();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<UpsertAsyncResult<Input, Output, Context>> UpsertAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx, ref Key key, ref Value value, Context userContext, long serialNo, CancellationToken token = default)
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.IsAsync = true;
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
                    return new ValueTask<UpsertAsyncResult<Input, Output, Context>>(new UpsertAsyncResult<Input, Output, Context>(internalStatus));
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
        #endregion UpsertAsync

        #region DeleteAsync
        /// <summary>
        /// Contained state storage for the completion of an async Delete, or the result if the Delete was completed synchronously
        /// </summary>
        public struct DeleteAsyncResult<Input, Output, Context> : IUpdelAsyncResult<Input, Output, Context, DeleteAsyncResult<Input, Output, Context>>
        {
            private readonly OperationStatus internalStatus;
            internal readonly UpdelAsyncInternal<Input, Output, Context, DeleteAsyncOperation<Input, Output, Context>, DeleteAsyncResult<Input, Output, Context>> updelAsyncInternal;

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
                updelAsyncInternal = new UpdelAsyncInternal<Input, Output, Context, DeleteAsyncOperation<Input, Output, Context>, DeleteAsyncResult<Input, Output, Context>>(
                                        fasterKV, fasterSession, currentCtx, pendingContext, exceptionDispatchInfo);
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
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.IsAsync = true;
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
        #endregion DeleteAsync

        #region RMWAsync
        internal sealed class RmwAsyncInternal<Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            PendingContext<Input, Output, Context> _pendingContext;
            AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;

            internal RmwAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext,
                                      AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
            }

            internal void SetDiskRequest(AsyncIOContext<Key, Value> diskRequest) => _diskRequest = diskRequest;

            internal ValueTask<RmwAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
            {
                Debug.Assert(_fasterKV.RelaxedCPR);

                // Note: We currently do not await anything here, and we must never do any post-await work inside CompleteAsync; this includes any code in
                // a 'finally' block. All post-await work must be re-initiated by end user on the mono-threaded session.

                if (TryCompleteAsyncState(asyncOp: true, out AsyncIOContext<Key, Value> newDiskRequest, out CompletionEvent flushEvent, out var rmwAsyncResult))
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(rmwAsyncResult);

                if (_exception != default)
                    _exception.Throw();
                return SlowRmwAsync(_fasterKV, _fasterSession, _currentCtx, _pendingContext, newDiskRequest, flushEvent, token);
            }

            internal bool TryCompleteAsyncState(bool asyncOp, out AsyncIOContext<Key, Value> newDiskRequest, out CompletionEvent flushEvent, out RmwAsyncResult<Input, Output, Context> rmwAsyncResult)
            {
                // This makes one attempt to complete the async operation's synchronous state, and clears the async pending counters.
                if (CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                            return TryCompleteSync(asyncOp, out newDiskRequest, out flushEvent, out rmwAsyncResult);
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _currentCtx.ioPendingRequests.Remove(_pendingContext.id);
                        _currentCtx.asyncPendingCount--;
                        _currentCtx.pendingReads.Remove();
                    }
                }

                newDiskRequest = default;
                flushEvent = default;
                rmwAsyncResult = default;
                return false;
            }

            internal bool TryCompleteSync(bool asyncOp, out AsyncIOContext<Key, Value> newDiskRequest, out CompletionEvent flushEvent, out RmwAsyncResult<Input, Output, Context> rmwAsyncResult)
            {
                _fasterSession.UnsafeResumeThread();
                try
                {
                    Status status;
                    if (!_diskRequest.IsDefault())
                    {
                        flushEvent = _fasterKV.hlog.FlushEvent;
                        status = _fasterKV.InternalCompletePendingRequestFromContext(_currentCtx, _currentCtx, _fasterSession, _diskRequest, ref _pendingContext, asyncOp, out newDiskRequest);
                    }
                    else
                    {
                        status = _fasterKV.CallInternalRMW(_fasterSession, _currentCtx, ref _pendingContext, ref _pendingContext.key.Get(), ref _pendingContext.input.Get(), _pendingContext.userContext,
                                                         _pendingContext.serialNum, asyncOp, out flushEvent, out newDiskRequest);
                    }

                    if (status != Status.PENDING)
                    {
                        _pendingContext.Dispose();
                        rmwAsyncResult = new RmwAsyncResult<Input, Output, Context>(status, default);
                        return true;
                    }
                }
                finally
                {
                    _fasterSession.UnsafeSuspendThread();
                }
                rmwAsyncResult = default;
                return false;
            }

            internal Status CompletePending()
            {
                _fasterSession.CompletePendingWithOutputs(out var completedOutputs, wait: true, spinWaitForCommit: false);
                var status = completedOutputs.Next() ? completedOutputs.Current.Status : Status.ERROR;
                completedOutputs.Dispose();
                return status;
            }
        }

        /// <summary>
        /// State storage for the completion of an async RMW, or the result if the RMW was completed synchronously
        /// </summary>
        public struct RmwAsyncResult<Input, Output, Context>
        {
            /// <summary>Current status of the RMW operation</summary>
            public Status Status { get; }

            internal readonly Output output;

            private readonly RmwAsyncInternal<Input, Output, Context> rmwAsyncInternal;

            internal RmwAsyncResult(Status status, Output output)
            {
                this.Status = status;
                this.output = output;
                this.rmwAsyncInternal = default;
            }

            internal RmwAsyncResult( FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx, PendingContext<Input, Output, Context> pendingContext, 
                AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = Status.PENDING;
                output = default;
                rmwAsyncInternal = new RmwAsyncInternal<Input, Output, Context>(fasterKV, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
            }

            /// <summary>
            /// Complete the RMW operation, issuing additional (rare) I/O asynchronously if needed.
            /// It is usually preferable to use Complete() instead of this.
            /// </summary>
            /// <returns>ValueTask for RMW result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<RmwAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
            {
                if (Status != Status.PENDING)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(Status, default));
                return rmwAsyncInternal.CompleteAsync(token);
            }

            /// <summary>
            /// Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.
            /// </summary>
            /// <returns>Status of RMW operation</returns>
            public Status Complete()
            {
                if (Status != Status.PENDING)
                    return Status;

                if (!rmwAsyncInternal.TryCompleteAsyncState(asyncOp: false, out var newDiskRequest, out var flushEvent, out var rmwAsyncResult))
                {
                    if (newDiskRequest.IsDefault())
                        flushEvent.Wait();
                    rmwAsyncInternal.SetDiskRequest(newDiskRequest);

                    while (!rmwAsyncInternal.TryCompleteSync(asyncOp: false, out newDiskRequest, out flushEvent, out rmwAsyncResult))
                    {
                        // CompletePending() may encounter OperationStatus.ALLOCATE_FAILED; if so, we don't have a more current flushEvent.
                        if (!newDiskRequest.IsDefault() && rmwAsyncInternal.CompletePending() != Status.PENDING)
                            break;
                        flushEvent.Wait();
                    }
                }
                return rmwAsyncResult.Status;
            }
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
                        diskRequest = await diskRequest.asyncOperation.Task;
                }
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }

            return new RmwAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pcontext, diskRequest, exceptionDispatchInfo);
        }
#endregion RMWAsync
    }
}

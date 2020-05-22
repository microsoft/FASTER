// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

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
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    /// <typeparam name="Context">Context</typeparam>
    /// <typeparam name="Functions">Functions</typeparam>
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {

        /// <summary>
        /// Check if at least one request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="clientSession"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal async ValueTask ReadyToCompletePendingAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
        {
            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (clientSession.ctx.phase == Phase.IN_PROGRESS || clientSession.ctx.phase == Phase.WAIT_PENDING)
                {
                    if (clientSession.ctx.prevCtx.ioPendingRequests.Count != 0)
                        await clientSession.ctx.prevCtx.readyResponses.WaitForEntryAsync();
                }
            }
            #endregion

            if (clientSession.ctx.ioPendingRequests.Count != 0)
                await clientSession.ctx.readyResponses.WaitForEntryAsync();
        }

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, CancellationToken token = default)
        {
            bool done = true;

            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (clientSession.ctx.phase == Phase.IN_PROGRESS
                    ||
                    clientSession.ctx.phase == Phase.WAIT_PENDING)
                {

                    await clientSession.ctx.prevCtx.pendingReads.WaitEmptyAsync();

                    await InternalCompletePendingRequestsAsync(clientSession.ctx.prevCtx, clientSession.ctx, clientSession, token);
                    Debug.Assert(clientSession.ctx.prevCtx.ioPendingRequests.Count == 0);

                    if (clientSession.ctx.prevCtx.retryRequests.Count > 0)
                    {
                        InternalCompleteRetryRequests(clientSession.ctx.prevCtx, clientSession.ctx, clientSession);
                    }

                    done &= (clientSession.ctx.prevCtx.HasNoPendingRequests);
                }
            }
            #endregion

            await InternalCompletePendingRequestsAsync(clientSession.ctx, clientSession.ctx, clientSession, token);
            InternalCompleteRetryRequests(clientSession.ctx, clientSession.ctx, clientSession);

            Debug.Assert(clientSession.ctx.HasNoPendingRequests);

            done &= (clientSession.ctx.HasNoPendingRequests);

            if (!done)
            {
                throw new Exception("CompletePendingAsync did not complete");
            }
        }

        internal sealed class ReadAsyncInternal
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value, Input, Output, Context, Functions> _fasterKV;
            readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;
            PendingContext _pendingContext;
            AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;

            internal ReadAsyncInternal(FasterKV<Key, Value, Input, Output, Context, Functions> fasterKV, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, PendingContext pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                _exception = default;
                _fasterKV = fasterKV;
                _clientSession = clientSession;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
            }

            internal (Status, Output) CompleteRead()
            {
                (Status, Output) _result = default;
                if (_diskRequest.asyncOperation != null
                    && CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_clientSession.SupportAsync) _clientSession.UnsafeResumeThread();
                        try
                        {
                            Debug.Assert(_fasterKV.RelaxedCPR);

                            _result = _fasterKV.InternalCompletePendingReadRequestAsync(
                                _clientSession.ctx, _clientSession.ctx, _diskRequest, _pendingContext);
                        }
                        finally
                        {
                            if (_clientSession.SupportAsync) _clientSession.UnsafeSuspendThread();
                        }
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _clientSession.ctx.ioPendingRequests.Remove(_pendingContext.id);
                    }
                }

                if (_exception != default)
                    _exception.Throw();
                return _result;
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult
        {
            readonly Status status;
            readonly Output output;

            readonly ReadAsyncInternal readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output)
            {
                this.status = status;
                this.output = output;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value, Input, Output, Context, Functions> fasterKV, 
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, 
                PendingContext pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                status = Status.PENDING;
                output = default;
                readAsyncInternal = new ReadAsyncInternal(fasterKV, clientSession, pendingContext, diskRequest);
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result, or throws an exception if error encountered.</returns>
            public (Status, Output) CompleteRead()
            {
                if (status != Status.PENDING)
                    return (status, output);

                return readAsyncInternal.CompleteRead();
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult> ReadAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Input input, Context context = default, CancellationToken token = default)
        {
            var pcontext = default(PendingContext);
            Output output = default;
            OperationStatus internalStatus;
            var nextSerialNum = clientSession.ctx.serialNum + 1;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
            TryReadAgain:

                internalStatus = InternalRead(ref key, ref input, ref output, ref context, ref pcontext, clientSession.ctx, nextSerialNum);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult>(new ReadAsyncResult((Status)internalStatus, output));
                }

                if (internalStatus == OperationStatus.CPR_SHIFT_DETECTED)
                {
                    SynchronizeEpoch(clientSession.ctx, clientSession.ctx, ref pcontext);
                    goto TryReadAgain;
                }
            }
            finally
            {
                clientSession.ctx.serialNum = nextSerialNum;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, clientSession, pcontext, token);

            static async ValueTask<ReadAsyncResult> SlowReadAsync(
                FasterKV<Key, Value, Input, Output, Context, Functions> @this,
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                PendingContext pendingContext, CancellationToken token = default)
            {
                var diskRequest = @this.ScheduleGetFromDisk(clientSession.ctx, ref pendingContext);
                clientSession.ctx.ioPendingRequests.Add(pendingContext.id, pendingContext);
                clientSession.ctx.pendingReads.Add();

                try
                {
                    token.ThrowIfCancellationRequested();

                    if (@this.epoch.ThisInstanceProtected())
                        throw new NotSupportedException("Async operations not supported over protected epoch");

                    diskRequest = await diskRequest.asyncOperation.ValueTaskOfT;
                }
                catch
                {
                    clientSession.ctx.ioPendingRequests.Remove(pendingContext.id);
                    throw;
                }
                finally 
                {
                    clientSession.ctx.pendingReads.Remove();
                }

                return new ReadAsyncResult(@this, clientSession, pendingContext, diskRequest);
            }
        }

        internal bool AtomicSwitch(FasterExecutionContext fromCtx, FasterExecutionContext toCtx, int version)
        {
            lock (toCtx)
            {
                if (toCtx.version < version)
                {
                    CopyContext(fromCtx, toCtx);
                    if (toCtx.serialNum != -1)
                    {
                        _hybridLogCheckpoint.info.checkpointTokens.TryAdd(toCtx.guid,
                            new CommitPoint
                            {
                                UntilSerialNo = toCtx.serialNum,
                                ExcludedSerialNos = toCtx.excludedSerialNos
                            });
                    }
                    return true;
                }
            }
            return false;
        }
    }
}

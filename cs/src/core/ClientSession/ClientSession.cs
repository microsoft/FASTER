// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Thread-independent session interface to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal readonly bool SupportAsync = false;
        private readonly FasterKV<Key, Value, Input, Output, Context> fht;
        internal readonly FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext ctx;
        internal CommitPoint LatestCommitPoint;

        internal Functions functions;

        internal ClientSessionSynchronizationListener Listener => new ClientSessionSynchronizationListener(this);

        internal ClientSession(
            FasterKV<Key, Value, Input, Output, Context> fht,
            FasterKV<Key, Value, Input, Output, Context>.FasterExecutionContext ctx,
            Functions functions,
            bool supportAsync)
        {
            this.fht = fht;
            this.ctx = ctx;
            this.functions = functions;
            this.SupportAsync = supportAsync;
            LatestCommitPoint = new CommitPoint { UntilSerialNo = -1, ExcludedSerialNos = null };
            // Session runs on a single thread
            if (!supportAsync)
                UnsafeResumeThread();
        }

        /// <summary>
        /// Get session ID
        /// </summary>
        public string ID { get { return ctx.guid; } }

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            CompletePending(true);

            // Session runs on a single thread
            if (!SupportAsync)
                UnsafeSuspendThread();
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, userContext, functions, serialNo, ctx, this.Listener);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="token"></param>
        /// <returns>ReadAsyncResult - call CompleteRead on the return value to complete the read operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, Input, Output, Context>.ReadAsyncResult<Functions>> ReadAsync(ref Key key, ref Input input, Context context = default, CancellationToken token = default)
        {
            return fht.ReadAsync(this, ref key, ref input, context, token);
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextUpsert(ref key, ref desiredValue, userContext, functions, serialNo, ctx, this.Listener);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="context"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask UpsertAsync(ref Key key, ref Value desiredValue, Context context = default, bool waitForCommit = false, CancellationToken token = default)
        {
            var status = Upsert(ref key, ref desiredValue, context, ctx.serialNum + 1);

            if (status == Status.OK && !waitForCommit)
                return default;

            return SlowUpsertAsync(this, waitForCommit, status, token);
        }

        private static async ValueTask SlowUpsertAsync(ClientSession<Key, Value, Input, Output, Context, Functions> @this, bool waitForCommit, Status status, CancellationToken token)
        {
            if (status == Status.PENDING)
                await @this.CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await @this.WaitForCommitAsync(token);
        }

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRMW(ref key, ref input, userContext, functions, serialNo, ctx, this.Listener);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RMWAsync(ref Key key, ref Input input, Context context = default, bool waitForCommit = false, CancellationToken token = default)
        {
            var status = RMW(ref key, ref input, context, ctx.serialNum + 1);

            if (status == Status.OK && !waitForCommit)
                return default;

            return SlowRMWAsync(this, waitForCommit, status, token);
        }

        private static async ValueTask SlowRMWAsync(ClientSession<Key, Value, Input, Output, Context, Functions> @this, bool waitForCommit, Status status, CancellationToken token)
        {

            if (status == Status.PENDING)
                await @this.CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await @this.WaitForCommitAsync(token);
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextDelete(ref key, userContext, functions, serialNo, ctx, this.Listener);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Async delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="context"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DeleteAsync(ref Key key, Context context = default, bool waitForCommit = false, CancellationToken token = default)
        {
            var status = Delete(ref key, context, ctx.serialNum + 1);

            if (status == Status.OK && !waitForCommit)
                return default;

            return SlowDeleteAsync(this, waitForCommit, status, token);
        }

        private static async ValueTask SlowDeleteAsync(ClientSession<Key, Value, Input, Output, Context, Functions> @this, bool waitForCommit, Status status, CancellationToken token)
        {

            if (status == Status.PENDING)
                await @this.CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await @this.WaitForCommitAsync(token);
        }

        /// <summary>
        /// Experimental feature
        /// Checks whether specified record is present in memory
        /// (between HeadAddress and tail, or between fromAddress
        /// and tail)
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="fromAddress">Look until this address</param>
        /// <returns>Status</returns>
        internal Status ContainsKeyInMemory(ref Key key, long fromAddress = -1)
        {
            return fht.InternalContainsKeyInMemory(ref key, ctx, this.Listener, fromAddress);
        }

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests()
        {
            foreach (var kvp in ctx.prevCtx?.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.prevCtx?.retryRequests)
                yield return val.serialNum;

            foreach (var kvp in ctx.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.retryRequests)
                yield return val.serialNum;
        }

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh()
        {
            if (SupportAsync) UnsafeResumeThread();
            fht.InternalRefresh(ctx, this.Listener);
            if (SupportAsync) UnsafeSuspendThread();
        }

        /// <summary>
        /// Sync complete outstanding pending operations
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                var result = fht.InternalCompletePending(ctx, functions, this.Listener, spinWait);
                if (spinWaitForCommit)
                {
                    if (spinWait != true)
                    {
                        throw new FasterException("Can spin-wait for checkpoint completion only if spinWait is true");
                    }
                    do
                    {
                        fht.InternalCompletePending(ctx, functions, this.Listener, spinWait);
                        if (fht.InRestPhase())
                        {
                            fht.InternalCompletePending(ctx, functions, this.Listener, spinWait);
                            return true;
                        }
                    } while (spinWait);
                }
                return result;
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await fht.CompletePendingAsync(this, token);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(token);
        }

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // Complete all pending operations on session
            await CompletePendingAsync();

            var task = fht.CheckpointTask;
            CommitPoint localCommitPoint = LatestCommitPoint;
            if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                return;

            while (true)
            {
                await task.WithCancellationAsync(token);
                Refresh();

                task = fht.CheckpointTask;
                localCommitPoint = LatestCommitPoint;
                if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                    break;
            }
        }

        /// <summary>
        /// Resume session on current thread
        /// Call SuspendThread before any async op
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            fht.epoch.Resume();
            fht.InternalRefresh(ctx, this.Listener);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            fht.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(int version)
        {
            fht.AtomicSwitch(ctx, ctx.prevCtx, version);
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult
        {
            readonly Status status;
            readonly Output output;

            readonly FasterKV<Key, Value, Input, Output, Context>.ReadAsyncInternal<Functions> readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output)
            {
                this.status = status;
                this.output = output;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value, Input, Output, Context> fasterKV,
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                FasterKV<Key, Value, Input, Output, Context>.PendingContext pendingContext, AsyncIOContext<Key, Value> diskRequest)
            {
                status = Status.PENDING;
                output = default;
                readAsyncInternal = new FasterKV<Key, Value, Input, Output, Context>.ReadAsyncInternal<Functions>(fasterKV, clientSession, pendingContext, diskRequest);
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

        internal struct ClientSessionSynchronizationListener : ISynchronizationListener
        {
            private ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public ClientSessionSynchronizationListener(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public void OnCheckpointCompletion(string guid, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(guid, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }

            public void UnsafeResumeThread()
            {
                _clientSession.UnsafeResumeThread();
            }

            public void UnsafeSuspendThread()
            {
                _clientSession.UnsafeSuspendThread();
            }
        }
    }
}

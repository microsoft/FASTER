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
    public sealed partial class ClientSession<Key, Value, Input, Output, Context, Functions> :
                                IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal readonly bool SupportAsync = false;
        private readonly FasterKV<Key, Value, Input, Output, Context, Functions> fht;
        internal readonly FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx;
        internal CommitPoint LatestCommitPoint;

        internal ClientSession(
            FasterKV<Key, Value, Input, Output, Context, Functions> fht,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            bool supportAsync)
        {
            this.fht = fht;
            this.ctx = ctx;
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
            fht.DisposeClientSession(ID);

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
                return fht.ContextRead(ref key, ref input, ref output, userContext, serialNo, ctx);
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
        public ValueTask<FasterKV<Key, Value, Input, Output, Context, Functions>.ReadAsyncResult> ReadAsync(ref Key key, ref Input input, Context context = default, CancellationToken token = default)
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
            var updateArgs = new PSFUpdateArgs<Key, Value>();
            FasterKVProviderData<Key, Value> providerData = null;
            Status status;

            if (SupportAsync) UnsafeResumeThread();
            try
            {
                status = fht.ContextUpsert(ref key, ref desiredValue, userContext, serialNo, ctx, ref updateArgs);
                if (status == Status.OK && this.fht.PSFManager.HasPSFs)
                {
                    providerData = updateArgs.ChangeTracker is null
                                        ? new FasterKVProviderData<Key, Value>(this.fht.hlog, ref key, ref desiredValue)
                                        : updateArgs.ChangeTracker.AfterData;
                }
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }

            return providerData is null
                ? status
                : this.fht.PSFManager.Upsert(providerData, updateArgs.LogicalAddress, updateArgs.ChangeTracker);
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

            static async ValueTask SlowUpsertAsync(
                ClientSession<Key, Value, Input, Output, Context, Functions> @this,
                bool waitForCommit, Status status, CancellationToken token
                )
            {
                if (status == Status.PENDING)
                    await @this.CompletePendingAsync(waitForCommit, token);
                else if (waitForCommit)
                    await @this.WaitForCommitAsync(token);
            }
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
            var updateArgs = new PSFUpdateArgs<Key, Value>();
            Status status;

            if (SupportAsync) UnsafeResumeThread();
            try
            {
                status = fht.ContextRMW(ref key, ref input, userContext, serialNo, ctx, ref updateArgs);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }

            return (status == Status.OK || status == Status.NOTFOUND) && this.fht.PSFManager.HasPSFs
                ? this.fht.PSFManager.Update(updateArgs.ChangeTracker)
                : status;
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

            static async ValueTask SlowRMWAsync(
                ClientSession<Key, Value, Input, Output, Context, Functions> @this,
                bool waitForCommit, Status status, CancellationToken token
                )
            {

                if (status == Status.PENDING)
                    await @this.CompletePendingAsync(waitForCommit, token);
                else if (waitForCommit)
                    await @this.WaitForCommitAsync(token);
            }
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
            var updateArgs = new PSFUpdateArgs<Key, Value>();
            Status status;

            if (SupportAsync) UnsafeResumeThread();
            try
            {
                status = fht.ContextDelete(ref key, userContext, serialNo, ctx, ref updateArgs);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }

            return status == Status.OK && this.fht.PSFManager.HasPSFs
                ? this.fht.PSFManager.Delete(updateArgs.ChangeTracker)
                : status;
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

            static async ValueTask SlowDeleteAsync(
                ClientSession<Key, Value, Input, Output, Context, Functions> @this,
                bool waitForCommit, Status status, CancellationToken token
                )
            {

                if (status == Status.PENDING)
                    await @this.CompletePendingAsync(waitForCommit, token);
                else if (waitForCommit)
                    await @this.WaitForCommitAsync(token);
            }
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
            return fht.InternalContainsKeyInMemory(ref key, ctx, fromAddress);
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
            fht.InternalRefresh(ctx, this);
            if (SupportAsync) UnsafeSuspendThread();
        }

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                var result = fht.InternalCompletePending(ctx, spinWait);
                if (spinWaitForCommit)
                {
                    if (spinWait != true)
                    {
                        throw new FasterException("Can spin-wait for checkpoint completion only if spinWait is true");
                    }
                    do
                    {
                        fht.InternalCompletePending(ctx, spinWait);
                        if (fht.InRestPhase())
                        {
                            fht.InternalCompletePending(ctx, spinWait);
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
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await fht.ReadyToCompletePendingAsync(this, token);
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
            fht.InternalRefresh(ctx, this);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            fht.epoch.Suspend();
        }

    }
}

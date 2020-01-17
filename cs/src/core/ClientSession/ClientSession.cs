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
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly bool supportAsync = false;
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
            this.supportAsync = supportAsync;
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
            if (!supportAsync)
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
            if (supportAsync) UnsafeResumeThread();
            var status = fht.ContextRead(ref key, ref input, ref output, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(Status, Output)> ReadAsync(Key key, Input input, bool waitForCommit = false, CancellationToken token = default)
        {
            Output output = default;
            Context context = default;
            var status = Read(ref key, ref input, ref output, context, ctx.serialNum + 1);
            if (status == Status.PENDING)
                return await CompletePendingReadAsync(ctx.serialNum, waitForCommit, token);
            else if (waitForCommit)
                await WaitForCommitAsync(token);
            return (status, output);
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
            if (supportAsync) UnsafeResumeThread();
            var status = fht.ContextUpsert(ref key, ref desiredValue, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask UpsertAsync(Key key, Value desiredValue, bool waitForCommit = false, CancellationToken token = default)
        {
            Context context = default;
            var status = Upsert(ref key, ref desiredValue, context, ctx.serialNum + 1);
            if (status == Status.PENDING)
                await CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await WaitForCommitAsync(token);
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
            if (supportAsync) UnsafeResumeThread();
            var status = fht.ContextRMW(ref key, ref input, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask RMWAsync(Key key, Input input, bool waitForCommit = false, CancellationToken token = default)
        {
            Context context = default;
            var status = RMW(ref key, ref input, context, ctx.serialNum + 1);
            if (status == Status.PENDING)
                await CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await WaitForCommitAsync(token);
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
            if (supportAsync) UnsafeResumeThread();
            var status = fht.ContextDelete(ref key, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask DeleteAsync(Key key, bool waitForCommit = false, CancellationToken token = default)
        {
            Context context = default;
            var status = Delete(ref key, context, ctx.serialNum + 1);
            if (status == Status.PENDING)
                await CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await WaitForCommitAsync(token);
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
            if (supportAsync) UnsafeResumeThread();
            fht.InternalRefresh(ctx, this);
            if (supportAsync) UnsafeSuspendThread();
        }

        /// <summary>
        /// Sync complete outstanding pending operations
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
        {
            if (supportAsync) UnsafeResumeThread();
            var result = fht.InternalCompletePending(ctx, spinWait);
            if (spinWaitForCommit)
            {
                if (spinWait != true)
                {
                    if (supportAsync) UnsafeSuspendThread();
                    throw new FasterException("Can spin-wait for checkpoint completion only if spinWait is true");
                }
                do
                {
                    fht.InternalCompletePending(ctx, spinWait);
                    if (fht.InRestPhase())
                    {
                        fht.InternalCompletePending(ctx, spinWait);
                        if (supportAsync) UnsafeSuspendThread();
                        return true;
                    }
                } while (spinWait);
            }
            if (supportAsync) UnsafeSuspendThread();
            return result;
        }

        /// <summary>
        /// Async complete outstanding pending operations
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

        private async ValueTask<(Status, Output)> CompletePendingReadAsync(long serialNo, bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            if (waitForCommit)
            {
                (Status, Output) s = await fht.CompletePendingReadAsync(serialNo, this, token);
                await WaitForCommitAsync(token);
                return s;
            }
            else
                return await fht.CompletePendingReadAsync(serialNo, this, token);
        }

        /// <summary>
        /// Wait for commit of all operations until current point in session.
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

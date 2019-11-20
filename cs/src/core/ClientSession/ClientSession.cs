// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Runtime.CompilerServices;
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

        internal ClientSession(
            FasterKV<Key, Value, Input, Output, Context, Functions> fht,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx, 
            bool supportAsync)
        {
            this.fht = fht;
            this.ctx = ctx;
            this.supportAsync = supportAsync;

            // Session runs on a single thread
            if (!supportAsync)
                UnsafeResumeThread();
        }

        /// <summary>
        /// Get session Guid
        /// </summary>
        public Guid ID { get { return ctx.guid; } }

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
            var status = fht.Read(ref key, ref input, ref output, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// 
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
            var status = fht.Upsert(ref key, ref desiredValue, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// 
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
            var status = fht.RMW(ref key, ref input, userContext, serialNo, ctx);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext, long serialNo)
        {
            if (supportAsync) UnsafeResumeThread();
            var status = fht.Delete(ref key, userContext, serialNo);
            if (supportAsync) UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// Refresh session, handling checkpointing if needed
        /// </summary>
        public void Refresh()
        {
            if (supportAsync) UnsafeResumeThread();
            fht.InternalRefresh(ctx);
            if (supportAsync) UnsafeSuspendThread();
        }

        /// <summary>
        /// Sync complete outstanding pending operations
        /// </summary>
        /// <param name="spinWait"></param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false)
        {
            if (supportAsync) UnsafeResumeThread();
            var result = fht.InternalCompletePending(ctx, spinWait);
            if (supportAsync) UnsafeSuspendThread();
            return result;
        }

        /// <summary>
        /// Async complete outstanding pending operations
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompletePendingAsync()
        {
            if (!supportAsync) throw new NotSupportedException();
            await fht.CompletePendingAsync(this);
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="spinWait"></param>
        /// <returns></returns>
        public bool CompleteCheckpoint(bool spinWait = false)
        {
            if (supportAsync) UnsafeResumeThread();
            var result = fht.CompleteCheckpoint(spinWait);
            if (supportAsync) UnsafeSuspendThread();
            return result;
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompleteCheckpointAsync()
        {
            if (!supportAsync) throw new NotSupportedException();
            await fht.CompleteCheckpointAsync(ctx, this);
        }

        /// <summary>
        /// Resume session on current thread
        /// Call SuspendThread before any async op
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            fht.epoch.Resume();
            fht.InternalRefresh(ctx);
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

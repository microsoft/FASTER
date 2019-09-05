// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
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
    public class ClientSession<Key, Value, Input, Output, Context, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private FasterKV<Key, Value, Input, Output, Context, Functions> fht;
        internal FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevCtx;
        internal FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx;

        internal ClientSession(
            FasterKV<Key, Value, Input, Output, Context, Functions> fht,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevCtx,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx)
        {
            this.fht = fht;
            this.prevCtx = prevCtx;
            this.ctx = ctx;
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
            TryCompletePending(true);
            fht.DisposeClientSession(ID);
        }

        /// <summary>
        /// Resume session on current thread
        /// Call SuspendThread before any async op
        /// </summary>
        public void UnsafeResumeThread()
        {
            fht.ResumeSession(prevCtx, ctx);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        public void UnsafeSuspendThread()
        {
            fht.SuspendSession();
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext, long monotonicSerialNum)
        {
            UnsafeResumeThread();
            var status = fht.Read(ref key, ref input, ref output, userContext, monotonicSerialNum);
            UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext, long monotonicSerialNum)
        {
            UnsafeResumeThread();
            var status = fht.Upsert(ref key, ref desiredValue, userContext, monotonicSerialNum);
            UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public Status RMW(ref Key key, ref Input input, Context userContext, long monotonicSerialNum)
        {
            UnsafeResumeThread();
            var status = fht.RMW(ref key, ref input, userContext, monotonicSerialNum);
            UnsafeSuspendThread();
            return status;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public Status Delete(ref Key key, Context userContext, long monotonicSerialNum)
        {
            UnsafeResumeThread();
            var status = fht.Delete(ref key, userContext, monotonicSerialNum);
            UnsafeSuspendThread();
            return status;
        }


        /// <summary>
        /// Sync complete outstanding pending operations
        /// </summary>
        /// <param name="spinWait"></param>
        /// <returns></returns>
        public bool TryCompletePending(bool spinWait = false)
        {
            UnsafeResumeThread();
            var result = fht.CompletePending(spinWait);
            UnsafeSuspendThread();
            return result;
        }

        /// <summary>
        /// Async complete outstanding pending operations
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompletePendingAsync()
        {
            await fht.CompletePendingAsync(this);
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="spinWait"></param>
        /// <returns></returns>
        public bool TryCompleteCheckpoint(bool spinWait = false)
        {
            UnsafeResumeThread();
            var result = fht.CompleteCheckpoint(spinWait);
            UnsafeSuspendThread();
            return result;
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompleteCheckpointAsync()
        {
            await fht.CompleteCheckpointAsync(this);
        }
    }
}

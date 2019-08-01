// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;

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
        private FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevCtx;
        private FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx;

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
            Resume();
            fht.CompletePending(true);
            fht.StopSession();
        }

        /// <summary>
        /// Resume session on current thread
        /// </summary>
        public void Resume()
        {
            fht.SetContext(prevCtx, ctx);
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
            Resume();
            return fht.Read(ref key, ref input, ref output, userContext, monotonicSerialNum);
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
            Resume();
            return fht.Upsert(ref key, ref desiredValue, userContext, monotonicSerialNum);
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
            Resume();
            return fht.RMW(ref key, ref input, userContext, monotonicSerialNum);
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
            Resume();
            return fht.Delete(ref key, userContext, monotonicSerialNum);
        }

        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        public bool CompletePending(bool wait = false)
        {
            Resume();
            return fht.CompletePending(wait);
        }
    }
}

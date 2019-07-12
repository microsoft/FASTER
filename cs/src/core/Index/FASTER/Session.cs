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
    public class Session<Key, Value, Input, Output, Context, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private static Session<Key, Value, Input, Output, Context, Functions>[] _sessions
            = new Session<Key, Value, Input, Output, Context, Functions>[LightEpoch.kTableSize];

        private FasterKV<Key, Value, Input, Output, Context, Functions> fht;
        private FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevThreadCtx;
        private FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext threadCtx;
        private readonly int epochEntry;

        internal static Session<Key, Value, Input, Output, Context, Functions> GetOrCreate(
            FasterKV<Key, Value, Input, Output, Context, Functions> fht,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevThreadCtx,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext threadCtx,
            int epochEntry)
        {
            if (_sessions[epochEntry] == null)
            {
                _sessions[epochEntry] = new Session<Key, Value, Input, Output, Context, Functions>(fht, prevThreadCtx, threadCtx, epochEntry);
                return _sessions[epochEntry];
            }

            var session = _sessions[epochEntry];
            session.fht = fht;
            session.prevThreadCtx = prevThreadCtx;
            session.threadCtx = threadCtx;
            return session;
        }

        internal Session(
            FasterKV<Key, Value, Input, Output, Context, Functions> fht,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevThreadCtx,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext threadCtx,
            int epochEntry)
        {
            this.fht = fht;
            this.prevThreadCtx = prevThreadCtx;
            this.threadCtx = threadCtx;
            this.epochEntry = epochEntry;
        }

        /// <summary>
        /// Get session Guid
        /// </summary>
        public Guid ID { get { return threadCtx.guid; } }

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            Resume();
            fht.StopSession();
        }

        /// <summary>
        /// Resume session on current thread
        /// </summary>
        public void Resume()
        {
            fht.SetContext(prevThreadCtx, threadCtx, epochEntry);
        }

        /// <summary>
        /// Return shared (not thread-specific) session with FASTER
        /// </summary>
        /// <returns></returns>
        public void Return(bool completePending = false)
        {
            fht.SuspendSession(completePending);
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

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
        FasterKV<Key, Value, Input, Output, Context, Functions> fht;
        private FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext prevThreadCtx;
        private FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext threadCtx;
        private int epochEntry;

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
            prevThreadCtx = threadCtx = null;
            epochEntry = 0;
            fht = null;
        }

        /// <summary>
        /// Resume session on current thread
        /// </summary>
        public void Resume()
        {
            fht.SetContext(prevThreadCtx, threadCtx, epochEntry);
        }
    }
}

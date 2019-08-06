// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Start new shared (not thread-specific) session with FASTER.
        /// Session starts in dormant state.
        /// </summary>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> StartClientSession()
        {
            Guid guid = Guid.NewGuid();
            var ctx = new FasterExecutionContext();
            InitContext(ctx, guid);
            var prevCtx = new FasterExecutionContext();
            InitContext(prevCtx, guid);
            return new ClientSession<Key, Value, Input, Output, Context, Functions>(this, prevCtx, ctx);
        }


        /// <summary>
        /// Resume session with FASTER
        /// </summary>
        /// <param name="prevThreadCtx"></param>
        /// <param name="threadCtx"></param>
        internal void ResumeSession(FasterExecutionContext prevThreadCtx, FasterExecutionContext threadCtx)
        {
            epoch.Resume();

            // Copy contexts to thread-local
            this.prevThreadCtx.InitializeThread();
            this.threadCtx.InitializeThread();

            this.prevThreadCtx.Value = prevThreadCtx;
            this.threadCtx.Value = threadCtx;
        }

        /// <summary>
        /// Suspend session with FASTER
        /// </summary>
        internal void SuspendSession()
        {
            epoch.Suspend();
        }
    }
}

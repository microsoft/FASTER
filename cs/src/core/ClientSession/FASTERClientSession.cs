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
        /// Start new shared (not thread-specific) session with FASTER
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


        internal void SetContext(FasterExecutionContext prevThreadCtx, FasterExecutionContext threadCtx)
        {
            if (!epoch.ThreadEntry.IsInitializedForThread)
            {
                epoch.Acquire();
                epoch.ThreadEntry.InitializeThread();
                this.prevThreadCtx.InitializeThread();
                this.threadCtx.InitializeThread();
            }
            this.prevThreadCtx.Value = prevThreadCtx;
            this.threadCtx.Value = threadCtx;
            Refresh();
        }

        /// <summary>
        /// Suspend session with FASTER
        /// </summary>
        internal void SuspendSession(bool completePending = false)
        {
            if (completePending)
            {
                while (true)
                {
                    bool done = true;
                    if (threadCtx.Value.retryRequests.Count != 0 || threadCtx.Value.ioPendingRequests.Count != 0)
                        done = false;

                    if (prevThreadCtx.Value != default(FasterExecutionContext))
                    {
                        if (prevThreadCtx.Value.retryRequests.Count != 0 || prevThreadCtx.Value.ioPendingRequests.Count != 0)
                            done = false;
                    }
                    if (threadCtx.Value.phase != Phase.REST)
                        done = false;

                    if (done) break;
                    Refresh();
                }
            }
            epoch.Release();
        }
    }
}

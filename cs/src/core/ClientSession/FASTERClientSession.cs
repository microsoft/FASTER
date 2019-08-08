// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private Dictionary<Guid, ClientSession<Key, Value, Input, Output, Context, Functions>> _activeSessions;

        /// <summary>
        /// Start new client session (not thread-specific) with FASTER.
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
            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<Guid, ClientSession<Key, Value, Input, Output, Context, Functions>>(), null);
            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, prevCtx, ctx);
            lock (_activeSessions)
                _activeSessions.Add(guid, session);
            return session;
        }

        /// <summary>
        /// Continue session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <param name="lsn"></param>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> ContinueClientSession(Guid guid, out long lsn)
        {
            lsn = InternalContinue(guid);
            if (lsn == -1)
                throw new Exception($"Unable to find session {guid} to recover");

            var prevCtx = this.prevThreadCtx.Value;
            var ctx = this.threadCtx.Value;
            SuspendSession();

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, prevCtx, ctx);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<Guid, ClientSession<Key, Value, Input, Output, Context, Functions>>(), null);
            lock (_activeSessions)
                _activeSessions.Add(guid, session);
            return session;
        }

        /// <summary>
        /// Dispose session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        public void DisposeClientSession(Guid guid)
        {
            lock (_activeSessions)
                _activeSessions.Remove(guid);
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

            InternalRefresh();
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

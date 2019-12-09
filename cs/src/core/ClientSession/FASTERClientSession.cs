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
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private Dictionary<string, ClientSession<Key, Value, Input, Output, Context, Functions>> _activeSessions;

        /// <summary>
        /// Start new client session (not thread-specific) with FASTER.
        /// Session starts in dormant state.
        /// </summary>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession(string sessionName = null, bool supportAsync = true)
        {
            if (supportAsync)
                UseRelaxedCPR();

            if (sessionName == null)
                sessionName = Guid.NewGuid().ToString();
            var ctx = new FasterExecutionContext();
            InitContext(ctx, sessionName);
            var prevCtx = new FasterExecutionContext();
            InitContext(prevCtx, sessionName);
            prevCtx.version--;

            ctx.prevCtx = prevCtx;

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, ClientSession<Key, Value, Input, Output, Context, Functions>>(), null);

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, supportAsync);
            lock (_activeSessions)
                _activeSessions.Add(sessionName, session);
            return session;
        }

        /// <summary>
        /// Continue session with FASTER
        /// </summary>
        /// <param name="sessionName"></param>
        /// <param name="cp"></param>
        /// <param name="supportAsync"></param>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession(string sessionName, out CommitPoint cp, bool supportAsync = true)
        {
            if (supportAsync)
                UseRelaxedCPR();

            cp = InternalContinue(sessionName, out FasterExecutionContext ctx);
            if (cp.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionName} to recover");

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, supportAsync);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, ClientSession<Key, Value, Input, Output, Context, Functions>>(), null);
            lock (_activeSessions)
                _activeSessions.Add(sessionName, session);
            return session;
        }

        /// <summary>
        /// Dispose session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        internal void DisposeClientSession(string guid)
        {
            lock (_activeSessions)
                _activeSessions.Remove(guid);
        }
    }
}

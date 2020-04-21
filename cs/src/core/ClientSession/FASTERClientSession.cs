// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal Dictionary<string, ClientSession<Key, Value, Input, Output, Context, Functions>> _activeSessions;

        /// <summary>
        /// Start a new client session with FASTER.
        /// </summary>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession(string sessionId = null, bool threadAffinitized = false)
        {
            if (!threadAffinitized)
                UseRelaxedCPR();

            if (sessionId == null)
                sessionId = Guid.NewGuid().ToString();
            var ctx = new FasterExecutionContext();
            InitContext(ctx, sessionId);
            var prevCtx = new FasterExecutionContext();
            InitContext(prevCtx, sessionId);
            prevCtx.version--;

            ctx.prevCtx = prevCtx;

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, ClientSession<Key, Value, Input, Output, Context, Functions>>(), null);

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, !threadAffinitized);
            lock (_activeSessions)
                _activeSessions.Add(sessionId, session);
            return session;
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false)
        {
            if (!threadAffinitized)
                UseRelaxedCPR();

            commitPoint = InternalContinue(sessionId, out FasterExecutionContext ctx);
            if (commitPoint.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionId} to recover");

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, !threadAffinitized);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, ClientSession<Key, Value, Input, Output, Context, Functions>>(), null);
            lock (_activeSessions)
                _activeSessions.Add(sessionId, session);
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

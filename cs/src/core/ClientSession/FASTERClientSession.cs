// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal Dictionary<string, IClientSession> _activeSessions = new Dictionary<string, IClientSession>();

        /// <summary>
        /// Client session type helper
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        public struct ClientSessionBuilder<Input, Output, Context>
        {
            private readonly FasterKV<Key, Value> _fasterKV;
            private readonly IFunctions<Key, Value, Input, Output, Context> _functions;

            internal ClientSessionBuilder(FasterKV<Key, Value> fasterKV)
            {
                _fasterKV = fasterKV;
                _functions = null;
            }

            internal ClientSessionBuilder(FasterKV<Key, Value> fasterKV, IFunctions<Key, Value, Input, Output, Context> functions)
            {
                _fasterKV = fasterKV;
                _functions = functions;
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.NewSession<Input, Output, Context, Functions>(functions, sessionId, threadAffinitized, inputVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER, used during
            /// recovery from failure.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.ResumeSession<Input, Output, Context, Functions>(functions, sessionId, out commitPoint, threadAffinitized, inputVariableLengthStructSettings);
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(string sessionId = null, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.NewSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, threadAffinitized, inputVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER, used during
            /// recovery from failure.
            /// </summary>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.ResumeSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, out commitPoint, threadAffinitized, inputVariableLengthStructSettings);
            }
        }

        /// <summary>
        /// Helper method to specify Input, Output and Context for callback functions
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public ClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>()
        {
            return new ClientSessionBuilder<Input, Output, Context>(this);
        }

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public ClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions)
        {
            return new ClientSessionBuilder<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Start a new client session with FASTER.
        /// For performance reasons, pleae use session.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionId = null, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
        {
            return NewSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, threadAffinitized, inputVariableLengthStructSettings);
        }

        /// <summary>
        /// Start a new client session with FASTER.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Input, Output, Context, Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            if (!threadAffinitized)
                UseRelaxedCPR();

            if (sessionId == null)
                sessionId = Guid.NewGuid().ToString();
            var ctx = new FasterExecutionContext<Input, Output, Context>();
            InitContext(ctx, sessionId);
            var prevCtx = new FasterExecutionContext<Input, Output, Context>();
            InitContext(prevCtx, sessionId);
            prevCtx.version--;

            ctx.prevCtx = prevCtx;
            
            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, IClientSession>(), null);

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, inputVariableLengthStructSettings);
            lock (_activeSessions)
                _activeSessions.Add(sessionId, session);
            return session;
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
        {
            return ResumeSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, out commitPoint, threadAffinitized, inputVariableLengthStructSettings);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="inputVariableLengthStructSettings">Input-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>

        public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Input, Output, Context, Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, InputVariableLengthStructSettings<Value, Input> inputVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            if (!threadAffinitized)
                UseRelaxedCPR();

            commitPoint = InternalContinue<Input, Output, Context>(sessionId, out var ctx);
            if (commitPoint.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionId} to recover");


            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, inputVariableLengthStructSettings);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, IClientSession>(), null);
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
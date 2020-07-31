// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
        where Key : new()
        where Value : new()
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

            internal ClientSessionBuilder(FasterKV<Key, Value> fasterKV)
            {
                _fasterKV = fasterKV;
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="variableLengthStruct">Implementation of input-specific length computation for variable-length structs</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false, IVariableLengthStruct<Value, Input> variableLengthStruct = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.NewSession<Input, Output, Context, Functions>(functions, sessionId, threadAffinitized, variableLengthStruct);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER, used during
            /// recovery from failure.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="variableLengthStruct">Implementation of input-specific length computation for variable-length structs</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, IVariableLengthStruct<Value, Input> variableLengthStruct = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.ResumeSession<Input, Output, Context, Functions>(functions, sessionId, out commitPoint, threadAffinitized, variableLengthStruct);
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
        /// Start a new client session with FASTER.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="variableLengthStruct">Implementation of input-specific length computation for variable-length structs</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionId = null, bool threadAffinitized = false, IVariableLengthStruct<Value, Input> variableLengthStruct = null)
        {
            return NewSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, threadAffinitized, variableLengthStruct);
        }

        /// <summary>
        /// Start a new client session with FASTER.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="variableLengthStruct">Implementation of input-specific length computation for variable-length structs</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Input, Output, Context, Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false, IVariableLengthStruct<Value, Input> variableLengthStruct = null)
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

            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, variableLengthStruct);
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
        /// <param name="variableLengthStruct">Implementation of input-specific length computation for variable-length structs</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, IVariableLengthStruct<Value, Input> variableLengthStruct = null)
        {
            return ResumeSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, out commitPoint, threadAffinitized, variableLengthStruct);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="variableLengthStruct">Implementation of input-specific length computation for variable-length structs</param>
        /// <returns>Session instance</returns>

        public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Input, Output, Context, Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, IVariableLengthStruct<Value, Input> variableLengthStruct = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            if (!threadAffinitized)
                UseRelaxedCPR();

            commitPoint = InternalContinue<Input, Output, Context>(sessionId, out var ctx);
            if (commitPoint.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionId} to recover");


            var session = new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, variableLengthStruct);

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
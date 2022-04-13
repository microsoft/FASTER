// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, StoreFunctions>
    {
        internal Dictionary<int, SessionInfo> _activeSessions = new();

        /// <summary>
        /// Client session type helper
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        public struct ClientSessionBuilder<Input, Output, Context>
        {
            private readonly FasterKV<Key, Value, StoreFunctions> _fasterKV;
            private readonly IFunctions<Key, Value, Input, Output, Context> _functions;

            internal ClientSessionBuilder(FasterKV<Key, Value, StoreFunctions> fasterKV, IFunctions<Key, Value, Input, Output, Context> functions)
            {
                _fasterKV = fasterKV;
                _functions = functions;
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionName">Name of session (optional)</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> NewSession<Functions, Allocator>(Functions functions, string sessionName = null,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
                where Allocator : AllocatorBase<Key, Value>
            {
                return _fasterKV.NewSession<Input, Output, Context, Functions, Allocator>(functions, sessionName, sessionVariableLengthStructSettings, readFlags);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER; used during recovery from failure.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionName">Name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> ResumeSession<Functions, Allocator>(Functions functions, string sessionName, out CommitPoint commitPoint,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
                where Allocator : AllocatorBase<Key, Value>
            {
                return _fasterKV.ResumeSession<Input, Output, Context, Functions, Allocator>(functions, sessionName, out commitPoint, sessionVariableLengthStructSettings, readFlags);
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="sessionName">Name of session (optional)</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> NewSession<Functions, Allocator>(string sessionName = null, 
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
                where Allocator : AllocatorBase<Key, Value>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.NewSession<Input, Output, Context, Functions, Allocator>((Functions)_functions, sessionName, sessionVariableLengthStructSettings, readFlags);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER, used during
            /// recovery from failure.
            /// </summary>
            /// <param name="sessionName">Name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> ResumeSession<Functions, Allocator>(string sessionName, out CommitPoint commitPoint,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
                where Allocator : AllocatorBase<Key, Value>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.ResumeSession<Input, Output, Context, Functions, Allocator>((Functions)_functions, sessionName, out commitPoint, sessionVariableLengthStructSettings, readFlags);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER, used during
            /// recovery from failure.
            /// </summary>
            /// <param name="sessionID">ID of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> ResumeSession<Functions, Allocator>(int sessionID, out CommitPoint commitPoint,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
                where Allocator : AllocatorBase<Key, Value>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.ResumeSession<Input, Output, Context, Functions, Allocator>((Functions)_functions, sessionID, out commitPoint, sessionVariableLengthStructSettings, readFlags);
            }
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
        /// For performance reasons, please use <see cref="FasterKV{Key, Value, StoreFunctions}.For{Input, Output, Context}(IFunctions{Key, Value, Input, Output, Context})"/> instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of session (auto-generated if not provided)</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>, StoreFunctions, Allocator> NewSession<Input, Output, Context, Allocator>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionName = null,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Allocator : AllocatorBase<Key, Value>
        {
            return NewSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>, Allocator>(functions, sessionName, sessionVariableLengthStructSettings, readFlags);
        }

        /// <summary>
        /// Start a new client session with FASTER.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of session (optional)</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        internal ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> NewSession<Input, Output, Context, Functions, Allocator>(Functions functions, string sessionName = null,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
                where Allocator : AllocatorBase<Key, Value>
            => InternalNewSession<Input, Output, Context, Functions, ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator>>(functions, sessionName,
                            ctx => new ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator>(this, ctx, functions, sessionVariableLengthStructSettings), readFlags);

        private TSession InternalNewSession<Input, Output, Context, Functions, TSession>(Functions functions, string sessionName,
                                                                            Func<FasterExecutionContext<Input, Output, Context>, TSession> sessionCreator, ReadFlags readFlags)
            where TSession : IClientSession
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            if (sessionName == "")
                throw new FasterException("Cannot use empty string as session name");

            if (sessionName != null && _recoveredSessionNameMap != null && _recoveredSessionNameMap.ContainsKey(sessionName))
                throw new FasterException($"Session named {sessionName} already exists in recovery info, use RecoverSession to resume it");

            int sessionID = Interlocked.Increment(ref maxSessionID);
            var ctx = new FasterExecutionContext<Input, Output, Context>();
            InitContext(ctx, sessionID, sessionName);
            ctx.ReadFlags = readFlags;
            var prevCtx = new FasterExecutionContext<Input, Output, Context>();
            InitContext(prevCtx, sessionID, sessionName);
            prevCtx.version--;
            prevCtx.ReadFlags = readFlags;

            ctx.prevCtx = prevCtx;

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<int, SessionInfo>(), null);

            var session = sessionCreator(ctx);
            lock (_activeSessions)
                _activeSessions.Add(sessionID, new SessionInfo { sessionName = sessionName, session = session, isActive = true });
            return session;
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER; used during recovery from failure.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>, StoreFunctions, Allocator> ResumeSession<Input, Output, Context, Allocator>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionName,
                out CommitPoint commitPoint, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Allocator : AllocatorBase<Key, Value>
        {
            return ResumeSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>, Allocator>(functions, sessionName, out commitPoint, sessionVariableLengthStructSettings, readFlags);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionName">Name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        internal ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> ResumeSession<Input, Output, Context, Functions, Allocator>(Functions functions, string sessionName, out CommitPoint commitPoint,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where Allocator : AllocatorBase<Key, Value>
        {
            if (_recoveredSessionNameMap == null || !_recoveredSessionNameMap.TryRemove(sessionName, out int sessionID))
                throw new FasterException($"Unable to find session named {sessionName} to recover");

            return InternalResumeSession<Input, Output, Context, Functions, ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator>>(functions, sessionID, out commitPoint,
                        ctx => new ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator>(this, ctx, functions, sessionVariableLengthStructSettings), readFlags);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER; used during recovery from failure.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionID">ID of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>, StoreFunctions, Allocator> ResumeSession<Input, Output, Context, Allocator>(IFunctions<Key, Value, Input, Output, Context> functions, int sessionID,
                out CommitPoint commitPoint, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Allocator : AllocatorBase<Key, Value>
        {
            return ResumeSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>, Allocator>(functions, sessionID, out commitPoint, sessionVariableLengthStructSettings, readFlags);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionID">ID of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <param name="readFlags">ReadFlags for this session; override those specified at FasterKV level, and may be overridden on individual Read operations</param>
        /// <returns>Session instance</returns>
        internal ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator> ResumeSession<Input, Output, Context, Functions, Allocator>(Functions functions, int sessionID, out CommitPoint commitPoint,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null, ReadFlags readFlags = ReadFlags.Default)
                where Allocator : AllocatorBase<Key, Value>
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            return InternalResumeSession<Input, Output, Context, Functions, ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator>>(functions, sessionID, out commitPoint,
                        ctx => new ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator>(this, ctx, functions, sessionVariableLengthStructSettings), readFlags);
        }

        private TSession InternalResumeSession<Input, Output, Context, Functions, TSession>(Functions functions, int sessionID, out CommitPoint commitPoint,
                                                                                            Func<FasterExecutionContext<Input, Output, Context>, TSession> sessionCreator, ReadFlags readFlags)
             where TSession : IClientSession
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            string sessionName;
            (sessionName, commitPoint) = InternalContinue<Input, Output, Context>(sessionID, out var ctx);
            if (commitPoint.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionID} to recover");
            ctx.ReadFlags = readFlags;

            var session = sessionCreator(ctx);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<int, SessionInfo>(), null);
            lock (_activeSessions)
                _activeSessions.Add(sessionID, new SessionInfo { sessionName = sessionName, session = session, isActive = true });
            return session;
        }

        /// <summary>
        /// Dispose session with FASTER
        /// </summary>
        /// <param name="sessionID"></param>
        /// <param name="sessionPhase"></param>
        /// <returns></returns>
        internal void DisposeClientSession(int sessionID, Phase sessionPhase)
        {
            // If a session is disposed during a checkpoint cycle, we mark the session
            // as inactive, but wait until the end of checkpoint before disposing it
            lock (_activeSessions)
            {
                if (sessionPhase == Phase.REST || sessionPhase == Phase.PREPARE_GROW || sessionPhase == Phase.IN_PROGRESS_GROW)
                    _activeSessions.Remove(sessionID);
                else
                    _activeSessions[sessionID].isActive = false;
            }
        }
    }
}
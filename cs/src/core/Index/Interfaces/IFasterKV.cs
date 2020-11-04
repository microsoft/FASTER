// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Interface to FASTER key-value store
    /// </summary>
    public interface IFasterKV<Key, Value> : IDisposable
    {
        #region New Session Operations

        /// <summary>
        /// Start a new client session with FASTER.
        /// For performance reasons, please use FasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions.</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);

        /// <summary>
        /// Start a new advanced client session with FASTER.
        /// For performance reasons, please use FasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public AdvancedClientSession<Key, Value, Input, Output, Context, IAdvancedFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IAdvancedFunctions<Key, Value, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions.</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);

        /// <summary>
        /// Resume (continue) prior client session with FASTER using advanced functions; used during recovery from failure.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public AdvancedClientSession<Key, Value, Input, Output, Context, IAdvancedFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IAdvancedFunctions<Key, Value, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);

        #endregion

        #region Growth and Recovery

        /// <summary>
        /// Grow the hash index
        /// </summary>
        /// <returns>Whether the request succeeded</returns>
        bool GrowIndex();

        /// <summary>
        /// Initiate full (index + log) checkpoint of FASTER
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <returns>Whether we successfully initiated the checkpoint (initiation may fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to await completion.</returns>
        /// <remarks>Uses the checkpoint type specified in the <see cref="CheckpointSettings"/></remarks>
        bool TakeFullCheckpoint(out Guid token);

        /// <summary>
        /// Initiate full (index + log) checkpoint of FASTER
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <returns>Whether we successfully initiated the checkpoint (initiation mayfail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to await completion.</returns>
        public bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType);

        /// <summary>
        /// Take full (index + log) checkpoint of FASTER asynchronously
        /// </summary>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <param name="cancellationToken">A token to cancel the operation</param>
        /// <returns>A (bool success, Guid token) tuple.
        /// success: Whether we successfully initiated the checkpoint (initiation may fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint.
        /// Await the task to complete checkpoint, if initiated successfully</returns>
        public ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Initiate checkpoint of FASTER index only (not log)
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to await completion.</returns>
        bool TakeIndexCheckpoint(out Guid token);

        /// <summary>
        /// Take asynchronous checkpoint of FASTER index only (not log)
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the operation</param>
        /// <returns>A (bool success, Guid token) tuple.
        /// success: Whether we successfully initiated the checkpoint (initiation may fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint.
        /// Await the task to complete checkpoint, if initiated successfully</returns>
        public ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Initiate checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to await completion.</returns>
        bool TakeHybridLogCheckpoint(out Guid token);

        /// <summary>
        /// Take asynchronous checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <returns>Whether we successfully initiated the checkpoint (initiation mayfail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to await completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType);

        /// <summary>
        /// Initiate checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the operation</param>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <returns>A (bool success, Guid token) tuple.
        /// success: Whether we successfully initiated the checkpoint (initiation may fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint.
        /// Await the task to complete checkpoint, if initiated successfully</returns>
        public ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Recover from last successful index and log checkpoints
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        void Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        void Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Complete ongoing checkpoint (spin-wait)
        /// </summary>
        /// <returns>Whether checkpoint has completed</returns>
        ValueTask CompleteCheckpointAsync(CancellationToken token = default);

        #endregion

        #region Other Operations

        /// <summary>
        /// Get number of (non-zero) hash entries in FASTER
        /// </summary>
        long EntryCount { get; }

        /// <summary>
        /// Get size of index in #cache lines (64 bytes each)
        /// </summary>
        long IndexSize { get; }

        /// <summary>
        /// Get comparer used by this instance of FASTER
        /// </summary>
        IFasterEqualityComparer<Key> Comparer { get; }

        /// <summary>
        /// Dump distribution of #entries in hash table
        /// </summary>
        string DumpDistribution();

        /// <summary>
        /// Get accessor for FASTER hybrid log
        /// </summary>
        LogAccessor<Key, Value> Log { get; }

        /// <summary>
        /// Get accessor for FASTER read cache
        /// </summary>
        LogAccessor<Key, Value> ReadCache { get; }

        #endregion
    }
}
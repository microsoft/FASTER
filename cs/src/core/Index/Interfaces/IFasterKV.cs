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
        /// <param name="sessionName">Name of session (optional)</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions,
                string sessionName = null, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions.</param>
        /// <param name="sessionName">Name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions,
                string sessionName, out CommitPoint commitPoint, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);

        /// <summary>
        /// Resume (continue) prior client session with FASTER; used during recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionID">ID of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, int sessionID,
                out CommitPoint commitPoint, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null);
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
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>Whether we successfully initiated the checkpoint (initiation mayfail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to await completion.</returns>
        public bool TryInitiateFullCheckpoint(out Guid token, CheckpointType checkpointType, long targetVersion = -1);

        /// <summary>
        /// Take full (index + log) checkpoint of FASTER asynchronously
        /// </summary>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <param name="cancellationToken">A token to cancel the operation</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>A (bool success, Guid token) tuple.
        /// success: Whether we successfully initiated the checkpoint (initiation may fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint.
        /// Await the task to complete checkpoint, if initiated successfully</returns>
        public ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default, long targetVersion = -1);

        /// <summary>
        /// Initiate checkpoint of FASTER index only (not log)
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to await completion.</returns>
        bool TryInitiateIndexCheckpoint(out Guid token);

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
        /// Take asynchronous checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>Whether we successfully initiated the checkpoint (initiation mayfail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to await completion.</returns>
        public bool TryInitiateHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental = false, long targetVersion = -1);

        /// <summary>
        /// Initiate checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="checkpointType">The checkpoint type to use (ignores the checkpoint type specified in the <see cref="CheckpointSettings"/>)</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <param name="cancellationToken">A token to cancel the operation</param>
        /// <param name="targetVersion">
        /// intended version number of the next version. Checkpoint will not execute if supplied version is not larger
        /// than current version. Actual new version may have version number greater than supplied number. If the supplied
        /// number is -1, checkpoint will unconditionally create a new version. 
        /// </param>
        /// <returns>A (bool success, Guid token) tuple.
        /// success: Whether we successfully initiated the checkpoint (initiation may fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint.
        /// Await the task to complete checkpoint, if initiated successfully</returns>
        public ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, bool tryIncremental = false, CancellationToken cancellationToken = default, long targetVersion = -1);

        /// <summary>
        /// Recover from last successful index and log checkpoints
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with version after checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="recoverTo"> specific version requested within the checkpoint, if checkpoint supports multiple versions (e.g. incremental snapshot checkpoints), or -1 for latest version</param>
        /// <returns>Version we actually recovered to</returns>
        long Recover(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1);

        /// <summary>
        /// Asynchronously recover from last successful index and log checkpoint
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with version after checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="recoverTo"> specific version requested within the checkpoint, if checkpoint supports multiple versions (e.g. incremental snapshot checkpoints), or -1 for latest version</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        ValueTask<long> RecoverAsync(int numPagesToPreload = -1, bool undoNextVersion = true, long recoverTo = -1, CancellationToken cancellationToken = default);

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with version after checkpoint version need to be undone (and invalidated on log)</param>
        /// <returns>Version we actually recovered to</returns>
        long Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true);

        /// <summary>
        /// Asynchronously recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with version after checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        ValueTask<long> RecoverAsync(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoNextVersion = true, CancellationToken cancellationToken = default);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with version after checkpoint version need to be undone (and invalidated on log)</param>
        /// <returns>Version we actually recovered to</returns>
        long Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoNextVersion = true);

        /// <summary>
        /// Asynchronously recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoNextVersion">Whether records with version after checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Version we actually recovered to</returns>
        ValueTask<long> RecoverAsync(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoNextVersion = true, CancellationToken cancellationToken = default);

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
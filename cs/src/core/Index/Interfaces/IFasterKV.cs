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
        /// </summary>
        /// <param name="functions">Callback functions.</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Input, Output, Context, Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>;

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
        ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Input, Output, Context, Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>;

        #endregion

        #region Growth and Recovery

        /// <summary>
        /// Grow the hash index
        /// </summary>
        /// <returns></returns>
        bool GrowIndex();

        /// <summary>
        /// Take full checkpoint of FASTER
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <returns>Whether checkpoint was initiated</returns>
        bool TakeFullCheckpoint(out Guid token);

        /// <summary>
        /// Take checkpoint of FASTER index only (not log)
        /// </summary>
        /// <param name="token">Token describing checkpoin</param>
        /// <returns>Whether checkpoint was initiated</returns>
        bool TakeIndexCheckpoint(out Guid token);

        /// <summary>
        /// Take checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="token">Token describing checkpoin</param>
        /// <returns>Whether checkpoint was initiated</returns>
        bool TakeHybridLogCheckpoint(out Guid token);

        /// <summary>
        /// Recover from last successful index and log checkpoint
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Asynchronously recover from last successful index and log checkpoint
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        ValueTask RecoverAsync(int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        void Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Asynchronously recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        ValueTask RecoverAsync(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        void Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true);

        /// <summary>
        /// Asynchronously recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        ValueTask RecoverAsync(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true);

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
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
    public interface IFasterKV<Key, Value, Input, Output, Context, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        /*
        #region Session Operations (Deprecated)

        /// <summary>
        /// Start a session with FASTER. FASTER sessions correspond to threads issuing
        /// operations to FASTER.
        /// </summary>
        /// <returns>Session identifier</returns>
        Guid StartSession();

        /// <summary>
        /// Continue a session after recovery. Provide FASTER with the identifier of the
        /// session that is being continued.
        /// </summary>
        /// <param name="guid"></param>
        /// <returns>Sequence number for resuming operations</returns>
        CommitPoint ContinueSession(Guid guid);

        /// <summary>
        /// Stop a session and de-register the thread from FASTER.
        /// </summary>
        void StopSession();

        /// <summary>
        /// Refresh the session epoch. The caller is required to invoke Refresh periodically
        /// in order to guarantee system liveness.
        /// </summary>
        void Refresh();

        #endregion
        */

        /*
        #region Core Index Operations (Deprecated)

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by Reader to select what part of value to read</param>
        /// <param name="output">Reader stores the read result in output</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status Read(ref Key key, ref Input input, ref Output output, Context context, long serialNo);

        /// <summary>
        /// (Blind) upsert operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="value">Value being upserted</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status Upsert(ref Key key, ref Value value, Context context, long serialNo);

        /// <summary>
        /// Atomic read-modify-write operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by RMW callback to perform operation</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status RMW(ref Key key, ref Input input, Context context, long serialNo);

        /// <summary>
        /// Delete entry (use tombstone if necessary)
        /// Hash entry is removed as a best effort (if key is in memory and at 
        /// the head of hash chain.
        /// Value is set to null (using ConcurrentWrite) if it is in mutable region
        /// </summary>
        /// <param name="key">Key of delete</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status Delete(ref Key key, Context context, long serialNo);

        /// <summary>
        /// Complete all pending operations issued by this session
        /// </summary>
        /// <param name="wait">Whether we spin-wait for pending operations to complete</param>
        /// <returns>Whether all pending operations have completed</returns>
        bool CompletePending(bool wait);

        #endregion
        */

        #region New Session Operations
        /// <summary>
        /// Start new client session (not thread-specific) with FASTER.
        /// Session starts in dormant state.
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="supportAsync"></param>
        /// <returns></returns>
        ClientSession<Key, Value, Input, Output, Context, Functions> NewSession(string sessionId = null, bool supportAsync = true);

        /// <summary>
        /// Continue client session with FASTER
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="cp"></param>
        /// <param name="supportAsync"></param>
        /// <returns></returns>
        ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession(string sessionId, out CommitPoint cp, bool supportAsync = true);

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
        /// Recover from last successfuly checkpoints
        /// </summary>
        void Recover();

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        void Recover(Guid fullcheckpointToken);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        void Recover(Guid indexToken, Guid hybridLogToken);

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
        LogAccessor<Key, Value, Input, Output, Context, Functions> Log { get; }

        /// <summary>
        /// Get accessor for FASTER read cache
        /// </summary>
        LogAccessor<Key, Value, Input, Output, Context, Functions> ReadCache { get; }

        #endregion
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Interface to FASTER key-value store
    /// (customized for sample types Key, Value, Input, Output, Context)
    /// Since there are pointers in the API, we cannot automatically create a
    /// generic version covering arbitrary blittable types. Instead, the
    /// user defines the customized interface and provides it to FASTER
    /// so it can return a (generated) instance for that interface.
    /// </summary>
    public unsafe interface IFasterKV
    {
        /* Thread-related operations */

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
        long ContinueSession(Guid guid);

        /// <summary>
        /// Stop a session and de-register the thread from FASTER.
        /// </summary>
        void StopSession();

        /// <summary>
        /// Refresh the session epoch. The caller is required to invoke Refresh periodically
        /// in order to guarantee system liveness.
        /// </summary>
        void Refresh();

        /* Store Interface */

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by Reader to select what part of value to read</param>
        /// <param name="output">Reader stores the read result in output</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="lsn">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status Read(Key* key, Input* input, Output* output, Context* context, long lsn);

        /// <summary>
        /// (Blind) upsert operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="value">Value being upserted</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="lsn">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status Upsert(Key* key, Value* value, Context* context, long lsn);

        /// <summary>
        /// Atomic read-modify-write operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by RMW callback to perfom operation</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="lsn">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        Status RMW(Key* key, Input* input, Context* context, long lsn);

        /// <summary>
        /// Complete all pending operations issued by this session
        /// </summary>
        /// <param name="wait">Whether we spin-wait for pending operations to complete</param>
        /// <returns>Whether all pending operations have completed</returns>
        bool CompletePending(bool wait);

        /* Recovery */

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
        /// <param name="wait"></param>
        /// <returns>Whether checkpoint has completed</returns>
        bool CompleteCheckpoint(bool wait);

        /* Statistics */
        /// <summary>
        /// Get size of FASTER
        /// </summary>
        long LogTailAddress { get; }

        /// <summary>
        /// Dump distribution of #entries in hash table, to console
        /// </summary>
        void DumpDistribution();
    }
}

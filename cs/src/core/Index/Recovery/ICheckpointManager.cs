// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// Interface for users to control creation and retrieval of checkpoint-related data
    /// FASTER calls this interface during checkpoint/recovery in this sequence:
    /// 
    /// Checkpoint:
    ///   InitializeIndexCheckpoint (for index checkpoints) -> 
    ///   GetIndexDevice (for index checkpoints) ->
    ///   InitializeLogCheckpoint (for log checkpoints) ->
    ///   GetSnapshotLogDevice (for log checkpoints in snapshot mode) ->
    ///   GetSnapshotObjectLogDevice (for log checkpoints in snapshot mode with objects) ->
    ///   CommitLogCheckpoint (for log checkpoints) ->
    ///   CommitIndexCheckpoint (for index checkpoints) ->
    /// 
    /// Recovery:
    ///   GetIndexCommitMetadata ->
    ///   GetLogCommitMetadata ->
    ///   GetIndexDevice ->
    ///   GetSnapshotLogDevice (for recovery in snapshot mode) ->
    ///   GetSnapshotObjectLogDevice (for recovery in snapshot mode with objects)
    /// 
    /// Provided devices will be closed directly by FASTER when done.
    /// </summary>
    public interface ICheckpointManager
    {
        /// <summary>
        /// Initialize index checkpoint
        /// </summary>
        /// <param name="indexToken"></param>
        void InitializeIndexCheckpoint(Guid indexToken);

        /// <summary>
        /// Initialize log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        void InitializeLogCheckpoint(Guid logToken);

        /// <summary>
        /// Commit index checkpoint
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="commitMetadata"></param>
        /// <returns></returns>
        void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata);

        /// <summary>
        /// Commit log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitMetadata"></param>
        /// <returns></returns>
        void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata);

        /// <summary>
        /// Retrieve commit metadata for specified index checkpoint
        /// </summary>
        /// <param name="indexToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        byte[] GetIndexCheckpointMetadata(Guid indexToken);

        /// <summary>
        /// Retrieve commit metadata for specified log checkpoint
        /// </summary>
        /// <param name="logToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        byte[] GetLogCheckpointMetadata(Guid logToken);

        /// <summary>
        /// Get list of index checkpoint tokens, in order of usage preference
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Guid> GetIndexCheckpointTokens();

        /// <summary>
        /// Get list of log checkpoint tokens, in order of usage preference
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Guid> GetLogCheckpointTokens();


        /// <summary>
        /// Provide device to store index checkpoint (including overflow buckets)
        /// </summary>
        /// <param name="indexToken"></param>
        /// <returns></returns>
        IDevice GetIndexDevice(Guid indexToken);

        /// <summary>
        /// Provide device to store snapshot of log (required only for snapshot checkpoints)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        IDevice GetSnapshotLogDevice(Guid token);

        /// <summary>
        /// Provide device to store snapshot of object log (required only for snapshot checkpoints)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        IDevice GetSnapshotObjectLogDevice(Guid token);
    }
}
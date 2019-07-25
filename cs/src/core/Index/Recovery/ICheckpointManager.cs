// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Interface for users to control creation and retrieval of checkpoint-related data
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
        byte[] GetIndexCommitMetadata(Guid indexToken);

        /// <summary>
        /// Retrieve commit metadata for specified log checkpoint
        /// </summary>
        /// <param name="logToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        byte[] GetLogCommitMetadata(Guid logToken);

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

        /// <summary>
        /// Get latest valid checkpoint for recovery
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="logToken"></param>
        /// <returns>true if latest valid checkpoint found, false otherwise</returns>
        bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken);
    }
}
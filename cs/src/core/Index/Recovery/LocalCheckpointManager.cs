// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.core
{
    /// <summary>
    /// Older implementation of checkpoint interface for local file storage (left for backward compatibility)
    /// </summary>
    public sealed class LocalCheckpointManager : ICheckpointManager
    {
        private readonly DirectoryConfiguration directoryConfiguration;

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="CheckpointDir"></param>
        public LocalCheckpointManager(string CheckpointDir)
        {
            directoryConfiguration = new DirectoryConfiguration(CheckpointDir);
        }

        /// <summary>
        /// Cleanup all files in this folder
        /// </summary>
        public void PurgeAll()
        {
            try { new DirectoryInfo(directoryConfiguration.checkpointDir).Delete(true); } catch { }
        }

        /// <summary>
        /// Initialize index checkpoint
        /// </summary>
        /// <param name="indexToken"></param>
        public void InitializeIndexCheckpoint(Guid indexToken)
        {
            directoryConfiguration.CreateIndexCheckpointFolder(indexToken);
        }

        /// <summary>
        /// Initialize log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        public void InitializeLogCheckpoint(Guid logToken)
        {
            directoryConfiguration.CreateHybridLogCheckpointFolder(logToken);
        }

        /// <summary>
        /// Commit index checkpoint
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="commitMetadata"></param>
        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            string filename = directoryConfiguration.GetIndexCheckpointMetaFileName(indexToken);
            using (var writer = new BinaryWriter(new FileStream(filename, FileMode.Create)))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
                writer.Flush();
            }

            string completed_filename = directoryConfiguration.GetIndexCheckpointFolder(indexToken);
            completed_filename += Path.DirectorySeparatorChar + "completed.dat";
            using (var file = new FileStream(completed_filename, FileMode.Create))
            {
                file.Flush();
            }
        }

        /// <summary>
        /// Commit log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitMetadata"></param>
        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            string filename = directoryConfiguration.GetHybridLogCheckpointMetaFileName(logToken);
            using (var writer = new BinaryWriter(new FileStream(filename, FileMode.Create)))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
                writer.Flush();
            }

            string completed_filename = directoryConfiguration.GetHybridLogCheckpointFolder(logToken);
            completed_filename += Path.DirectorySeparatorChar + "completed.dat";
            using (var file = new FileStream(completed_filename, FileMode.Create))
            {
                file.Flush();
            }
        }

        /// <summary>
        /// Retrieve commit metadata for specified index checkpoint
        /// </summary>
        /// <param name="indexToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetIndexCheckpointMetadata(Guid indexToken)
        {
            var dir = new DirectoryInfo(directoryConfiguration.GetIndexCheckpointFolder(indexToken));
            if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                return null;

            string filename = directoryConfiguration.GetIndexCheckpointMetaFileName(indexToken);
            using (var reader = new BinaryReader(new FileStream(filename, FileMode.Open)))
            {
                var len = reader.ReadInt32();
                return reader.ReadBytes(len);
            }
        }

        /// <summary>
        /// Retrieve commit metadata for specified log checkpoint
        /// </summary>
        /// <param name="logToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetLogCheckpointMetadata(Guid logToken)
        {
            var dir = new DirectoryInfo(directoryConfiguration.GetHybridLogCheckpointFolder(logToken));
            if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                return null;

            string checkpointInfoFile = directoryConfiguration.GetHybridLogCheckpointMetaFileName(logToken);
            using (var reader = new BinaryReader(new FileStream(checkpointInfoFile, FileMode.Open)))
            {
                var len = reader.ReadInt32();
                return reader.ReadBytes(len);
            }
        }

        /// <summary>
        /// Provide device to store index checkpoint (including overflow buckets)
        /// </summary>
        /// <param name="indexToken"></param>
        /// <returns></returns>
        public IDevice GetIndexDevice(Guid indexToken)
        {
            return Devices.CreateLogDevice(directoryConfiguration.GetPrimaryHashTableFileName(indexToken), false);
        }

        /// <summary>
        /// Provide device to store snapshot of log (required only for snapshot checkpoints)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetSnapshotLogDevice(Guid token)
        {
            return Devices.CreateLogDevice(directoryConfiguration.GetLogSnapshotFileName(token), false);
        }

        /// <summary>
        /// Provide device to store snapshot of object log (required only for snapshot checkpoints)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            return Devices.CreateLogDevice(directoryConfiguration.GetObjectLogSnapshotFileName(token), false);
        }

        /// <summary>
        /// Provide device to store delta log for incremental snapshot checkpoints
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetDeltaLogDevice(Guid token)
        {
            return Devices.CreateLogDevice(directoryConfiguration.GetDeltaLogFileName(token), false);
        }

        /// <inheritdoc />
        public IEnumerable<Guid> GetIndexCheckpointTokens()
        {
            var indexCheckpointDir = new DirectoryInfo(directoryConfiguration.GetIndexCheckpointFolder());
            var dirs = indexCheckpointDir.GetDirectories();
            foreach (var dir in dirs)
            {
                // Remove incomplete checkpoints
                if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                {
                    Directory.Delete(dir.FullName, true);
                }
            }

            bool found = false;
            foreach (var folder in indexCheckpointDir.GetDirectories().OrderByDescending(f => f.LastWriteTime))
            {
                if (Guid.TryParse(folder.Name, out var indexToken))
                {
                    found = true;
                    yield return indexToken;
                }
            }

            if (!found)
                throw new FasterException("No valid index checkpoint to recover from");
        }

        /// <inheritdoc />
        public IEnumerable<Guid> GetLogCheckpointTokens()
        {
            var hlogCheckpointDir = new DirectoryInfo(directoryConfiguration.GetHybridLogCheckpointFolder());
            var dirs = hlogCheckpointDir.GetDirectories();
            foreach (var dir in dirs)
            {
                // Remove incomplete checkpoints
                if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                {
                    Directory.Delete(dir.FullName, true);
                }
            }

            bool found = false;
            foreach (var folder in hlogCheckpointDir.GetDirectories().OrderByDescending(f => f.LastWriteTime))
            {
                if (Guid.TryParse(folder.Name, out var logToken))
                {
                    found = true;
                    yield return logToken;
                }
            }

            if (!found)
                throw new FasterException("No valid hybrid log checkpoint to recover from");
        }

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }
}
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
            directoryConfiguration = new(CheckpointDir);
        }

        /// <summary>
        /// Cleanup all files in this folder
        /// </summary>
        public void PurgeAll()
        {
            try { new DirectoryInfo(directoryConfiguration.checkpointDir).Delete(true); } catch { }
        }

        public void Purge(Guid token)
        {
            // Try both because we don't know which one
            try { new DirectoryInfo(directoryConfiguration.GetHybridLogCheckpointFolder(token)).Delete(true); } catch { }
            try { new DirectoryInfo(directoryConfiguration.GetIndexCheckpointFolder(token)).Delete(true); } catch { }
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
            using (BinaryWriter writer = new(new FileStream(filename, FileMode.Create)))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
                writer.Flush();
            }

            string completed_filename = directoryConfiguration.GetIndexCheckpointFolder(indexToken);
            completed_filename += Path.DirectorySeparatorChar + "completed.dat";
            using FileStream file = new(completed_filename, FileMode.Create);
            file.Flush();
        }

        /// <summary>
        /// Commit log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitMetadata"></param>
        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            string filename = directoryConfiguration.GetHybridLogCheckpointMetaFileName(logToken);
            using (BinaryWriter writer = new(new FileStream(filename, FileMode.Create)))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
                writer.Flush();
            }

            string completed_filename = directoryConfiguration.GetHybridLogCheckpointFolder(logToken);
            completed_filename += Path.DirectorySeparatorChar + "completed.dat";
            using FileStream file = new(completed_filename, FileMode.Create);
            file.Flush();
        }

        /// <summary>
        /// Retrieve commit metadata for specified index checkpoint
        /// </summary>
        /// <param name="indexToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetIndexCheckpointMetadata(Guid indexToken)
        {
            DirectoryInfo dir = new(directoryConfiguration.GetIndexCheckpointFolder(indexToken));
            if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                return null;

            string filename = directoryConfiguration.GetIndexCheckpointMetaFileName(indexToken);
            using BinaryReader reader = new(new FileStream(filename, FileMode.Open));
            var len = reader.ReadInt32();
            return reader.ReadBytes(len);
        }

        /// <summary>
        /// Retrieve commit metadata for specified log checkpoint
        /// </summary>
        /// <param name="logToken">Token</param>
        /// <param name="deltaLog">Delta log</param>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, long version)
        {
            byte[] metadata = null;
            if (deltaLog != null)
            {
                // Get latest valid metadata from delta-log
                deltaLog.Reset();
                while (deltaLog.GetNext(out long physicalAddress, out int entryLength, out DeltaLogEntryType type))
                {
                    switch (type)
                    {
                        case DeltaLogEntryType.DELTA:
                            // consider only metadata records
                            continue;
                        case DeltaLogEntryType.CHECKPOINT_METADATA:
                            metadata = new byte[entryLength];
                            unsafe
                            {
                                fixed (byte* m = metadata)
                                {
                                    Buffer.MemoryCopy((void*)physicalAddress, m, entryLength, entryLength);
                                }
                            }
                            HybridLogRecoveryInfo recoveryInfo = new();
                            using (StreamReader s = new(new MemoryStream(metadata))) {
                                recoveryInfo.Initialize(s);
                                // Finish recovery if only specific versions are requested
                                if (recoveryInfo.version == version) goto LoopEnd;
                            }
                            continue;
                        default:
                            throw new FasterException("Unexpected entry type");
                    }
                    LoopEnd:
                        break;
                   
                }
                if (metadata != null) return metadata;
            }

            DirectoryInfo dir = new(directoryConfiguration.GetHybridLogCheckpointFolder(logToken));
            if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                return null;

            string checkpointInfoFile = directoryConfiguration.GetHybridLogCheckpointMetaFileName(logToken);
            using BinaryReader reader = new(new FileStream(checkpointInfoFile, FileMode.Open));
            var len = reader.ReadInt32();
            return reader.ReadBytes(len);
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
            DirectoryInfo indexCheckpointDir = new(directoryConfiguration.GetIndexCheckpointFolder());
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
            DirectoryInfo hlogCheckpointDir = new(directoryConfiguration.GetHybridLogCheckpointFolder());
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

        /// <inheritdoc />
        public unsafe void CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            deltaLog.Allocate(out int length, out long physicalAddress);
            if (length < commitMetadata.Length)
            {
                deltaLog.Seal(0, DeltaLogEntryType.CHECKPOINT_METADATA);
                deltaLog.Allocate(out length, out physicalAddress);
                if (length < commitMetadata.Length)
                {
                    deltaLog.Seal(0);
                    throw new Exception($"Metadata of size {commitMetadata.Length} does not fit in delta log space of size {length}");
                }
            }
            fixed (byte* ptr = commitMetadata)
            {
                Buffer.MemoryCopy(ptr, (void*)physicalAddress, commitMetadata.Length, commitMetadata.Length);
            }
            deltaLog.Seal(commitMetadata.Length, DeltaLogEntryType.CHECKPOINT_METADATA);
            deltaLog.FlushAsync().Wait();
        }

        /// <inheritdoc />
        public void OnRecovery(Guid indexToken, Guid logToken)
        {
        }
    }
}
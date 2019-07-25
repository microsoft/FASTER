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
    /// Implementation of checkpoint interface for local file storage
    /// </summary>
    public class LocalCheckpointManager : ICheckpointManager
    {
        private DirectoryConfiguration directoryConfiguration;

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="CheckpointDir"></param>
        public LocalCheckpointManager(string CheckpointDir)
        {
            directoryConfiguration = new DirectoryConfiguration(CheckpointDir);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="commitInfo"></param>
        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitInfo)
        {
            string filename = directoryConfiguration.GetIndexCheckpointMetaFileName(indexToken);
            using (var writer = new BinaryWriter(new FileStream(filename, FileMode.Create)))
            {
                writer.Write(commitInfo.Length);
                writer.Write(commitInfo);
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
        /// 
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitInfo"></param>
        public void CommitLogCheckpoint(Guid logToken, byte[] commitInfo)
        {
            string filename = directoryConfiguration.GetHybridLogCheckpointMetaFileName(logToken);
            using (var writer = new BinaryWriter(new FileStream(filename, FileMode.Create)))
            {
                writer.Write(commitInfo.Length);
                writer.Write(commitInfo);
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
        /// 
        /// </summary>
        /// <param name="indexToken"></param>
        /// <returns></returns>
        public byte[] GetIndexCommitMetadata(Guid indexToken)
        {
            directoryConfiguration.CreateIndexCheckpointFolder(indexToken);
            var dir = new DirectoryInfo(directoryConfiguration.GetIndexCheckpointFolder(indexToken));
            if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                return null;

            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="indexToken"></param>
        /// <returns></returns>
        public IDevice GetIndexDevice(Guid indexToken)
        {
            directoryConfiguration.CreateIndexCheckpointFolder(indexToken);
            return Devices.CreateLogDevice(directoryConfiguration.GetPrimaryHashTableFileName(indexToken), false);
            // Devices.CreateLogDevice(directoryConfiguration.GetOverflowBucketsFileName(token), false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="logToken"></param>
        /// <returns></returns>
        public bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
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
            var latestICFolder = indexCheckpointDir.GetDirectories().OrderByDescending(f => f.LastWriteTime).First();
            if (latestICFolder == null || !Guid.TryParse(latestICFolder.Name, out indexToken))
            {
                throw new Exception("No valid index checkpoint to recover from");
            }


            var hlogCheckpointDir = new DirectoryInfo(directoryConfiguration.GetHybridLogCheckpointFolder());
            dirs = hlogCheckpointDir.GetDirectories();
            foreach (var dir in dirs)
            {
                // Remove incomplete checkpoints
                if (!File.Exists(dir.FullName + Path.DirectorySeparatorChar + "completed.dat"))
                {
                    Directory.Delete(dir.FullName, true);
                }
            }
            var latestHLCFolder = hlogCheckpointDir.GetDirectories().OrderByDescending(f => f.LastWriteTime).First();
            if (latestHLCFolder == null || !Guid.TryParse(latestHLCFolder.Name, out logToken))
            {
                throw new Exception("No valid hybrid log checkpoint to recover from");
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="logToken"></param>
        /// <returns></returns>
        public byte[] GetLogCommitMetadata(Guid logToken)
        {
            directoryConfiguration.CreateHybridLogCheckpointFolder(logToken);
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
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetSnapshotLogDevice(Guid token)
        {
            directoryConfiguration.CreateHybridLogCheckpointFolder(token);
            return Devices.CreateLogDevice(directoryConfiguration.GetLogSnapshotFileName(token), false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            return Devices.CreateLogDevice(directoryConfiguration.GetObjectLogSnapshotFileName(token), false);
        }
    }


    class DirectoryConfiguration
    {
        private readonly string checkpointDir;

        public DirectoryConfiguration(string checkpointDir)
        {
            this.checkpointDir = checkpointDir;
        }

        public const string index_base_folder = "index-checkpoints";
        public const string index_meta_file = "info";
        public const string hash_table_file = "ht";
        public const string overflow_buckets_file = "ofb";
        public const string snapshot_file = "snapshot";

        public const string cpr_base_folder = "cpr-checkpoints";
        public const string cpr_meta_file = "info";

        public void CreateIndexCheckpointFolder(Guid token)
        {
            var directory = GetIndexCheckpointFolder(token);
            Directory.CreateDirectory(directory);
            DirectoryInfo directoryInfo = new System.IO.DirectoryInfo(directory);
            foreach (System.IO.FileInfo file in directoryInfo.GetFiles())
                file.Delete();
        }
        public void CreateHybridLogCheckpointFolder(Guid token)
        {
            var directory = GetHybridLogCheckpointFolder(token);
            Directory.CreateDirectory(directory);
            DirectoryInfo directoryInfo = new System.IO.DirectoryInfo(directory);
            foreach (System.IO.FileInfo file in directoryInfo.GetFiles())
                file.Delete();
        }

        public string GetIndexCheckpointFolder(Guid token = default(Guid))
        {
            if (token != default(Guid))
                return GetMergedFolderPath(checkpointDir, index_base_folder, token.ToString());
            else
                return GetMergedFolderPath(checkpointDir, index_base_folder);
        }

        public string GetHybridLogCheckpointFolder(Guid token = default(Guid))
        {
            if (token != default(Guid))
                return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString());
            else
                return GetMergedFolderPath(checkpointDir, cpr_base_folder);
        }

        public string GetIndexCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    index_base_folder,
                                    token.ToString(),
                                    index_meta_file,
                                    ".dat");
        }

        public string GetPrimaryHashTableFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    index_base_folder,
                                    token.ToString(),
                                    hash_table_file,
                                    ".dat");
        }

        public string GetOverflowBucketsFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    index_base_folder,
                                    token.ToString(),
                                    overflow_buckets_file,
                                    ".dat");
        }

        public string GetHybridLogCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir,
                                    cpr_base_folder,
                                    token.ToString(),
                                    cpr_meta_file,
                                    ".dat");
        }

        public string GetHybridLogCheckpointContextFileName(Guid checkpointToken, Guid sessionToken)
        {
            return GetMergedFolderPath(checkpointDir,
                                    cpr_base_folder,
                                    checkpointToken.ToString(),
                                    sessionToken.ToString(),
                                    ".dat");
        }

        public string GetLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString(), snapshot_file, ".dat");
        }

        public string GetObjectLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(checkpointDir, cpr_base_folder, token.ToString(), snapshot_file, ".obj.dat");
        }

        private static string GetMergedFolderPath(params String[] paths)
        {
            String fullPath = paths[0];

            for (int i = 1; i < paths.Length; i++)
            {
                if (i == paths.Length - 1 && paths[i].Contains("."))
                {
                    fullPath += paths[i];
                }
                else
                {
                    fullPath += Path.DirectorySeparatorChar + paths[i];
                }
            }

            return fullPath;
        }
    }
}
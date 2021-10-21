// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Older implementation of checkpoint interface for local file storage (left for backward compatibility)
    /// </summary>
    public sealed class LocalLogCommitManager : ILogCommitManager
    {
        private readonly string commitFile;

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="commitFile"></param>
        public LocalLogCommitManager(string commitFile)
        {
            this.commitFile = commitFile;
        }

        /// <summary>
        /// Perform (synchronous) commit with specified metadata
        /// </summary>
        /// <param name="beginAddress">Committed begin address (for information only, not necessary to persist)</param>
        /// <param name="untilAddress">Address committed until (for information only, not necessary to persist)</param>
        /// <param name="commitMetadata">Commit metadata</param>
        /// <param name="proposedCommitNum"> Ignored param </param>

        public void Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long proposedCommitNum = -1)
        {
            if (proposedCommitNum != -1)
                throw new FasterException("LocalLogCommitManager does not support custom commit numbering");
            // Two phase to ensure we write metadata in single Write operation
            using MemoryStream ms = new();
            using (BinaryWriter writer = new(ms))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
            }
            using (BinaryWriter writer = new(new FileStream(commitFile, FileMode.OpenOrCreate)))
            {
                writer.Write(ms.ToArray());
                writer.Flush();
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
        }

        /// <summary>
        /// Retrieve commit metadata
        /// </summary>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetCommitMetadata(long commitNum)
        {
            if (!File.Exists(commitFile))
                return null;

            using var reader = new BinaryReader(new FileStream(commitFile, FileMode.Open));
            var len = reader.ReadInt32();
            return reader.ReadBytes(len);
        }

        /// <summary>
        /// List of commit numbers
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> ListCommits()
        {
            // we only use a single commit file in this implementation
            yield return 0;
        }
    }
}
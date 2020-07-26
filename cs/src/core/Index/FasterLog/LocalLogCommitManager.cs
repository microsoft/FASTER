// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Implementation of checkpoint interface for local file storage
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
        public void Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
            }
            using (var writer = new BinaryWriter(new FileStream(commitFile, FileMode.OpenOrCreate)))
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
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Implementation of checkpoint interface for local file storage
    /// </summary>
    public class LocalLogCommitManager : ILogCommitManager
    {
        private string CommitFile;

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="CommitFile"></param>
        public LocalLogCommitManager(string CommitFile)
        {
            this.CommitFile = CommitFile;
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
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                }
                using (var writer = new BinaryWriter(new FileStream(CommitFile, FileMode.OpenOrCreate)))
                {
                    writer.Write(ms.ToArray());
                    writer.Flush();
                }
            }
        }

        /// <summary>
        /// Retrieve commit metadata
        /// </summary>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetCommitMetadata()
        {
            if (!File.Exists(CommitFile))
                return null;

            using (var reader = new BinaryReader(new FileStream(CommitFile, FileMode.Open)))
            {
                var len = reader.ReadInt32();
                return reader.ReadBytes(len);
            }
        }
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Default checkpoint naming scheme used by FASTER
    /// </summary>
    public class DefaultCheckpointNamingScheme : ICheckpointNamingScheme
    {
        readonly string baseName;

        /// <summary>
        /// Create instance of default naming scheme
        /// </summary>
        /// <param name="baseName">Overall location specifier (e.g., local path or cloud container name)</param>
        public DefaultCheckpointNamingScheme(string baseName = "")
        {
            this.baseName = baseName;
        }

        /// <inheritdoc />
        public string BaseName() => baseName;

        /// <inheritdoc />
        public FileDescriptor LogCheckpointMetadata(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "info.dat");

        /// <inheritdoc />
        public FileDescriptor LogSnapshot(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "snapshot.dat");
        /// <inheritdoc />
        public FileDescriptor ObjectLogSnapshot(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "snapshot.obj.dat");


        /// <inheritdoc />
        public FileDescriptor IndexCheckpointMetadata(Guid token) => new FileDescriptor($"{IndexCheckpointBasePath()}/{token}", "info.dat");
        /// <inheritdoc />
        public FileDescriptor HashTable(Guid token) => new FileDescriptor($"{IndexCheckpointBasePath()}/{token}", "ht.dat");
        /// <inheritdoc />
        public FileDescriptor FasterLogCommitMetadata(long commitNumber) => new FileDescriptor($"{FasterLogCommitBasePath()}", $"commit.{commitNumber}");
        
        /// <inheritdoc />
        public Guid Token(FileDescriptor fileDescriptor) => Guid.Parse(fileDescriptor.directoryName.Split('/')[1]);
        /// <inheritdoc />
        public long CommitNumber(FileDescriptor fileDescriptor) => long.Parse(fileDescriptor.fileName.Split('.')[1]);

        /// <inheritdoc />
        public string IndexCheckpointBasePath() => "index-checkpoints";
        /// <inheritdoc />
        public string LogCheckpointBasePath() => "cpr-checkpoints";
        /// <inheritdoc />
        public string FasterLogCommitBasePath() => "log-commits";
    }
}
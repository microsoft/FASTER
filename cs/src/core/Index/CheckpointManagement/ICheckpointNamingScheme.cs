// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.core
{

    /// <summary>
    /// Interface to provide paths and names for all checkpoint-related files
    /// </summary>
    public interface ICheckpointNamingScheme
    {
        /// <summary>
        /// Base (or container) name for all checkpoint files
        /// </summary>
        /// <returns></returns>
        public string BaseName();

        /// <summary>
        /// Hash table (including overflow buckets)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        FileDescriptor HashTable(Guid token);

        /// <summary>
        /// Index checkpoint metadata
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        FileDescriptor IndexCheckpointMetadata(Guid token);


        /// <summary>
        /// Hybrid log checkpoint metadata
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        FileDescriptor LogCheckpointMetadata(Guid token);

        /// <summary>
        /// Hybrid log snapshot
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        FileDescriptor LogSnapshot(Guid token);

        /// <summary>
        /// Object log snapshot
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        FileDescriptor ObjectLogSnapshot(Guid token);

        /// <summary>
        /// FasterLog commit metadata
        /// </summary>
        /// <returns></returns>
        FileDescriptor FasterLogCommitMetadata(long commitNumber);

        /// <summary>
        /// Token associated with given file descriptor
        /// </summary>
        /// <param name="fileDescriptor"></param>
        /// <returns></returns>
        Guid Token(FileDescriptor fileDescriptor);

        /// <summary>
        /// Commit number associated with given file descriptor
        /// </summary>
        /// <param name="fileDescriptor"></param>
        /// <returns></returns>
        long CommitNumber(FileDescriptor fileDescriptor);

        /// <summary>
        /// Get base path holding index checkpoints
        /// </summary>
        /// <returns></returns>
        string IndexCheckpointBasePath();

        /// <summary>
        /// Get base path holding log checkpoints
        /// </summary>
        /// <returns></returns>
        string LogCheckpointBasePath();

        /// <summary>
        /// Get base path holding FasterLog commits
        /// </summary>
        /// <returns></returns>
        string FasterLogCommitBasePath();
    }
}
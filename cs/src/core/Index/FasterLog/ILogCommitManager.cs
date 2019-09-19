// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Log commit manager
    /// </summary>
    public interface ILogCommitManager
    {
        /// <summary>
        /// Perform (synchronous) commit with specified metadata
        /// </summary>
        /// <param name="commitMetadata"></param>
        void Commit(byte[] commitMetadata);

        /// <summary>
        /// Return prior commit metadata during recovery
        /// </summary>
        /// <returns></returns>
        byte[] GetCommitMetadata();
    }
}
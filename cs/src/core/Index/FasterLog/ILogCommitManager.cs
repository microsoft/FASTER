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
        /// <param name="address">Address committed until (for information only, not necessary to persist)</param>
        /// <param name="commitMetadata">Commit metadata</param>
        void Commit(long address, byte[] commitMetadata);

        /// <summary>
        /// Return prior commit metadata during recovery
        /// </summary>
        /// <returns></returns>
        byte[] GetCommitMetadata();
    }
}
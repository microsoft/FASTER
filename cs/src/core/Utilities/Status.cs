// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Status result of operation on FASTER
    /// </summary>
    public enum Status
    {
        /// <summary>
        /// For Read and RMW, item being read was found, and
        /// the operation completed successfully
        /// For Upsert, item was upserted successfully
        /// </summary>
        OK,
        /// <summary>
        /// For Read and RMW, item being read was not found
        /// </summary>
        NOTFOUND,
        /// <summary>
        /// Operation went pending (async)
        /// </summary>
        PENDING,
        /// <summary>
        /// Operation resulted in some error
        /// </summary>
        ERROR
    }
}

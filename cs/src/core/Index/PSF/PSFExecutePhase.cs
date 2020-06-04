// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// The phase of PSF operations in which PSFs are being executed
    /// </summary>
    public enum PSFExecutePhase
    {
        /// <summary>
        /// Executing the PSFs to obtain new TPSFKeys to be inserted into the secondary FKV.
        /// </summary>
        Insert,

        /// <summary>
        /// Lookup in IPUCache or execute the PSFs to obtain TPSFKeys prior to update, to be compared to those 
        /// obtained after the update to modify the record's PSF membership in the secondary FKV.
        /// </summary>
        PreUpdate,

        /// <summary>
        /// Executing the PSFs to obtain TPSFKeys following an update, to be compared to those obtained before
        /// the update to modify the record's PSF membership in the secondary FKV.
        /// </summary>
        PostUpdate,

        /// <summary>
        /// Lookup in IPUCache to tombstone a record, or execute the PSFs to obtain TPSFKeys to place a new
        /// tombstoned record.
        /// </summary>
        Delete
    }
}

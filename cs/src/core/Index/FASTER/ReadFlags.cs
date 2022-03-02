// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Flags for the Read-by-address methods
    /// </summary>
    /// <remarks>Note: must be kept in sync with corresponding PendingContext k* values</remarks>
    [Flags]
    public enum ReadFlags
    {
        /// <summary>
        /// Use whatever was specified at the previous level, or None if at FasterKV level.
        /// </summary>
        /// <remarks>
        /// May be combined with other flags to add options.
        /// </remarks>
        Default = 0x0,

        /// <summary>
        /// Turn off all flags from higher levels. May be combined with other flags to specify an entirely new set of flags.
        /// </summary>
        None = 0x1,

        /// <summary>
        /// Do not update read cache with new data
        /// </summary>
        DisableReadCacheUpdates = 0x2,

        /// <summary>
        /// Skip read cache on reads
        /// </summary>
        DisableReadCacheReads = 0x4,

        /// <summary>
        /// Skip read cache on reads and updates
        /// </summary>
        DisableReadCache = 0x6,

        /// <summary>
        /// Copy reads to tail of main log from both read-only region and from <see cref="IDevice"/>.
        /// </summary>
        /// <remarks>
        /// This generally should not be used for reads by address, and especially not for versioned reads,
        /// because those would promote obsolete values to the tail of the log.
        /// </remarks>
        CopyReadsToTail = 0x8,

        /// <summary>
        /// Copy reads from <see cref="IDevice" /> only (do not copy from read-only region), to read cache or tail.
        /// </summary>
        CopyFromDeviceOnly = 0x10,
    }
}
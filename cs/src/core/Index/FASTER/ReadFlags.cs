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
        /// Default read operation
        /// </summary>
        None = 0,

        /// <summary>
        /// Skip the ReadCache when reading, including not inserting to ReadCache when pending reads are complete.
        /// May be used with ReadAtAddress, to avoid copying earlier versions.
        /// </summary>
        SkipReadCache = 0x00000001,

        /// <summary>
        /// The minimum address at which to resolve the Key; return <see cref="Status.Found"/> false if the key is not found at this address or higher
        /// </summary>
        MinAddress = 0x00000002,

        /// <summary>
        /// Force a copy to tail if we read from immutable or on-disk. If this and ReadCache are both specified, ReadCache wins.
        /// This avoids log pollution for read-mostly workloads. Used mostly in conjunction with 
        /// <see cref="LockableUnsafeContext{Key, Value, Input, Output, Context, Functions}"/> locking.
        /// </summary>
        CopyToTail = 0x00000004,

        /// <summary>
        /// Skip copying to tail even if the FasterKV constructore specifed it. May be used with ReadAtAddress, to avoid copying earlier versions.
        /// </summary>
        SkipCopyToTail = 0x00000008,

        /// <summary>
        /// Utility to combine these flags. May be used with ReadAtAddress, to avoid copying earlier versions.
        /// </summary>
        SkipCopyReads = SkipReadCache | SkipCopyToTail,
    }
}
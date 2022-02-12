// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// The reason a SingleWriter was performed
    /// </summary>
    public enum WriteReason
    {
        /// <summary>A new record appended by Upsert</summary>
        Upsert, 

        /// <summary>Copying a read from disk to the tail of the log</summary>
        CopyToTail,

        /// <summary>Copying a read from disk to the read cache</summary>
        CopyToReadCache,

        /// <summary>The user called Compact()</summary>
        Compaction
    }
}
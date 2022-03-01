// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Options for the read operation
    /// </summary>
    public struct ReadOptions
    {
        /// <summary>
        /// The address to start the read search at; if this is Constants.kInvalidAddress, the search starts with the key, as in other forms of Read.
        /// For ReadAtAddress it is the address to read at.
        /// Can be populated from <see cref="RecordMetadata.RecordInfo"/>.PreviousAddress for chained reads.
        /// </summary>
        public long StartAddress;

        /// <summary>
        /// Flags for controlling operations within the read, such as ReadCache interaction. When doing versioned reads, this should be set to <see cref="ReadFlags.SkipCopyReads"/>
        /// </summary>
        public ReadFlags ReadFlags;
    }
}

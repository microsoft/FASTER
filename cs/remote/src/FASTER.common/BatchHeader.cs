// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace FASTER.common
{
    /// <summary>
    /// Header for message batch
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct BatchHeader
    {
        /// <summary>
        /// Size
        /// </summary>
        public const int Size = 8;

        /// <summary>
        /// Sequence number for batch
        /// </summary>
        [FieldOffset(0)]
        public int seqNo;

        /// <summary>
        /// Number of messsages packed in batch
        /// </summary>
        [FieldOffset(4)]
        public int numMessages;
    }
}
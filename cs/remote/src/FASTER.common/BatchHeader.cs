// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.common
{
    /// <summary>
    /// Header for message batch (Little Endian server)
    /// [4 byte seqNo][1 byte protocol][3 byte numMessages]
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct BatchHeader
    {
        /// <summary>
        /// Size
        /// </summary>
        public const int Size = 8;

        /// <summary>
        /// Sequence number.
        /// </summary>
        [FieldOffset(0)]
        public int SeqNo;

        /// <summary>
        /// Lower-order 8 bits are protocol type, higher-order 24 bits are num messages.
        /// </summary>
        [FieldOffset(4)]
        private int numMessagesAndProtocolType;

        /// <summary>
        /// Number of messages packed in batch
        /// </summary>
        public int NumMessages
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)((uint)numMessagesAndProtocolType >> 8);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { numMessagesAndProtocolType = (value << 8) | (numMessagesAndProtocolType & 0xFF); }
        }
        
        /// <summary>
        /// Wire protocol this batch is written in
        /// </summary>
        public WireFormat Protocol
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (WireFormat)(numMessagesAndProtocolType & 0xFF);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { numMessagesAndProtocolType = (numMessagesAndProtocolType & ~0xFF) | ((int)value & 0xFF); }
        }
    }
}
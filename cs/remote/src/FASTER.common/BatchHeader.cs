// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.server;

namespace FASTER.common
{
    /// <summary>
    /// Header for message batch
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct BatchHeader
    {
        public BatchHeader(int seqNo, int numMessages, WireFormat protocol)
        {
            this.seqNo = seqNo;
            numMessagesAndProtocolType = (numMessages << sizeof(byte)) | (byte) protocol;
        }
        
        /// <summary>
        /// Size
        /// </summary>
        public const int Size = 8;

        /// <summary>
        /// Sequence number for batch
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetSeqNo() => seqNo;

        /// <summary>
        /// Number of messsages packed in batch
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNumMessages() => numMessagesAndProtocolType >> sizeof(byte);
        
        /// <summary>
        /// Byte value that denotes the wire protocol this batch is written in. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public WireFormat GetProtocol() => (WireFormat) (numMessagesAndProtocolType & 0xFF);
        
        
        [FieldOffset(0)]
        private int seqNo;
        [FieldOffset(4)]
        // Lower-order 8 bits are protocol type, higher-order 24 bits are num messages.
        private int numMessagesAndProtocolType;
    }
}
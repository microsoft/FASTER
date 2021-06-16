// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.common
{
    /// <summary>
    /// Header for message batch
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct BatchHeader
    {
        public BatchHeader(int seqNo, int numMessages, WireFormat protocol)
        {
            this.seqNo = seqNo;
            numMessagesAndProtocolType = (numMessages << 8) | (byte) protocol;
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
        /// Set sequence number for batch
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetSeqNo(int seqNo) => this.seqNo = seqNo;

        /// <summary>
        /// Number of messages packed in batch
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNumMessages() => (int) ((uint) numMessagesAndProtocolType >> 8);

        /// <summary>
        /// Set number of messages packed in batch
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNumMessages(int numMessages) => numMessagesAndProtocolType = (numMessages << 8) | (numMessagesAndProtocolType & 0xFF);
        
        /// <summary>
        /// Byte value that denotes the wire protocol this batch is written in. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public WireFormat GetProtocol() => (WireFormat) (numMessagesAndProtocolType & 0xFF);

        /// <summary>
        /// Sets the byte value that denotes the wire protocol this batch is written in. 
        /// </summary>
        public void SetProtocol(WireFormat protocol) => numMessagesAndProtocolType = (numMessagesAndProtocolType & ~0xFF) | ((int) protocol & 0xFF);
        
        
        [FieldOffset(0)]
        private int seqNo;
        [FieldOffset(4)]
        // Lower-order 8 bits are protocol type, higher-order 24 bits are num messages.
        private int numMessagesAndProtocolType;
    }
}
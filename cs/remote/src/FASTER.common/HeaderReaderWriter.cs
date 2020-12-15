// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.common
{
    /// <summary>
    /// Reader and writer for message headers
    /// </summary>
    public struct HeaderReaderWriter
    {
        /// <summary>
        /// Write status
        /// </summary>
        /// <param name="s">Status</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Length of destination</param>
        /// <returns>Whether write succeeded</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Write(Status s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = (byte)s;
            return true;
        }

        /// <summary>
        /// Read status
        /// </summary>
        /// <param name="dst">Source memory</param>
        /// <returns>Status</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe Status ReadStatus(ref byte* dst)
        {
            return (Status)(*dst++);
        }

        /// <summary>
        /// Read pending sequence number
        /// </summary>
        /// <param name="dst">Source memory</param>
        /// <returns>Pending sequence number</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int ReadPendingSeqNo(ref byte* dst)
        {
            var ret = *(int*)dst;
            dst += sizeof(int);
            return ret;
        }

        /// <summary>
        /// Write message type to memory
        /// </summary>
        /// <param name="s">Message type</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Length of destination</param>
        /// <returns>Whether write succeeded</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Write(MessageType s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = (byte)s;
            return true;
        }

        /// <summary>
        /// Read message type
        /// </summary>
        /// <param name="dst">Source memory</param>
        /// <returns>Message type</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe MessageType ReadMessageType(ref byte* dst)
        {
            return (MessageType)(*dst++);
        }
    }
}
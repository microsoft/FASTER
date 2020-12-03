// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.common
{
    public struct HeaderReaderWriter
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Write(Status s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = (byte)s;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe Status ReadStatus(ref byte* dst)
        {
            return (Status)(*dst++);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int ReadPendingSeqNo(ref byte* dst)
        {
            var ret = *(int*)dst;
            dst += sizeof(int);
            return ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Write(MessageType s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = (byte)s;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe MessageType ReadMessageType(ref byte* dst)
        {
            return (MessageType)(*dst++);
        }
    }
}
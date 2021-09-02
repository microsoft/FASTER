// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.common;
using System.Buffers;

namespace FASTER.client
{
    /// <summary>
    /// Serializer for SpanByte (can be used on client side)
    /// </summary>
    public unsafe class SpanByteClientSerializer : IClientSerializer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory>
    {

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteClientSerializer()
        {
        }

        ///// <inheritdoc />
        public SpanByteAndMemory ReadOutput(ref byte* src)
        {
            int length = *(int*)src;
            var sb = SpanByte.FromPointer(src + sizeof(int), length);
            src += length + sizeof(int);
            return new SpanByteAndMemory(sb);
        }
        
        /// <inheritdoc />
        public SpanByte ReadKey(ref byte* src)
        {
            int length = *(int*)src;
            return SpanByte.FromPointer(src, length);
        }

        /// <inheritdoc />
        public SpanByte ReadValue(ref byte* src)
        {
            int length = *(int*)src;
            return SpanByte.FromPointer(src, length);
        }

        /// <inheritdoc />
        public bool Write(ref SpanByte k, ref byte* dst, int length)
        {
            var len = k.TotalSize;
            if (length < len) return false;
            k.CopyTo(dst);
            dst += len;
            return true;
        }
    }
}
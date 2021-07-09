// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using FASTER.core;
using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace FASTER.client
{
    /// <summary>
    /// Serializer for SpanByte (can be used on client side)
    /// </summary>
    public unsafe class SpanByteClientSerializer : IClientSerializer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory>
    {
        readonly MemoryPool<byte> memoryPool;
        readonly SpanByteVarLenStruct settings;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteClientSerializer(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public SpanByteAndMemory ReadOutput(ref byte* src)
        {
            int length = *(int*)src;
            var mem = memoryPool.Rent(length);
            new ReadOnlySpan<byte>(src + sizeof(int), length).CopyTo(mem.Memory.Span);
            src += length + sizeof(int);
            return new SpanByteAndMemory(mem, length);
        }

        /// <inheritdoc />
        public bool Write(ref SpanByte k, ref byte* dst, int length)
        {
            var len = settings.GetLength(ref k);
            if (length < len) return false;
            k.CopyTo(dst);
            dst += len;
            return true;
        }
    }
}
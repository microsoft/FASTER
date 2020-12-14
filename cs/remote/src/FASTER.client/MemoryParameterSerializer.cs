// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace FASTER.client
{
    /// <summary>
    /// Serializer for Memory
    /// </summary>
    /// <typeparam name="T">Type of memory</typeparam>
    public unsafe class MemoryParameterSerializer<T> : IClientSerializer<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int)>
        where T : unmanaged
    {
        readonly MemoryPool<T> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool">Memory pool</param>
        public MemoryParameterSerializer(MemoryPool<T> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<T>.Shared;
        }

        /// <inheritdoc />
        public (IMemoryOwner<T>, int) ReadOutput(ref byte* src)
        {
            var len = (*(int*)src) / sizeof(T);
            var mem = memoryPool.Rent(len);
            new ReadOnlySpan<byte>(src + sizeof(int), (*(int*)src)).CopyTo(
               MemoryMarshal.Cast<T, byte>(mem.Memory.Span));
            src += sizeof(int) + (*(int*)src);
            return (mem, len);
        }

        /// <inheritdoc />
        public bool Write(ref ReadOnlyMemory<T> k, ref byte* dst, int length)
        {
            int payloadLength = k.Length * sizeof(T);
            if (payloadLength + sizeof(int) > length) return false;
            *(int*)dst = payloadLength;

            MemoryMarshal.Cast<T, byte>(k.Span).CopyTo(new Span<byte>(dst + sizeof(int), payloadLength));
            dst += payloadLength + sizeof(int);
            return true;
        }
    }
}
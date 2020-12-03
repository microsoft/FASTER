// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using FASTER.core;

namespace FASTER.common
{
    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctionsForServer<Context> : SpanByteFunctions<SpanByte, SpanByteAndMemory, Context>
    {
        readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctionsForServer(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public unsafe override void SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            CopyWithHeaderTo(ref value, ref dst, memoryPool);
        }

        /// <inheritdoc />
        public unsafe override void ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            CopyWithHeaderTo(ref value, ref dst, memoryPool);
        }

        /// <summary>
        /// Copy to given SpanByteAndMemory (header length and payload copied to actual span/memory)
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        /// <param name="memoryPool"></param>
        private unsafe void CopyWithHeaderTo(ref SpanByte src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (dst.Length >= src.TotalSize)
                {
                    dst.Length = src.TotalSize;
                    var span = dst.SpanByte.AsSpan();
                    fixed (byte* ptr = span)
                        *(int*)ptr = src.Length;
                    src.AsReadOnlySpan().CopyTo(span.Slice(sizeof(int)));
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Length = src.TotalSize;
            dst.Memory = memoryPool.Rent(src.TotalSize);
            dst.Length = src.TotalSize;
            fixed (byte* ptr = dst.Memory.Memory.Span)
                *(int*)ptr = src.Length;
            src.AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span.Slice(sizeof(int)));
        }
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Buffers;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctionsForServer<Context> : SpanByteFunctions<SpanByte, SpanByteAndMemory, Context>
    {
        readonly WireFormat wireFormat;
        readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="wireFormat"></param>
        /// <param name="memoryPool"></param>
        public SpanByteFunctionsForServer(WireFormat wireFormat = default, MemoryPool<byte> memoryPool = default) : base(true)
        {
            this.wireFormat = wireFormat;
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public unsafe override void SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            if (wireFormat == WireFormat.Binary)
                CopyWithHeaderTo(ref value, ref dst, memoryPool);
        }

        /// <inheritdoc />
        public unsafe override void ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            if (wireFormat == WireFormat.Binary)
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
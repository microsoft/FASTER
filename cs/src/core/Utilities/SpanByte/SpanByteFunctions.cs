// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// Callback functions for SpanByte key, value
    /// </summary>
    public class SpanByteFunctions<Input, Output, Context> : FunctionsBase<SpanByte, SpanByte, Input, Output, Context>
    {
        /// <inheritdoc />
        public override void SingleWriter(ref SpanByte key, ref SpanByte src, ref SpanByte dst)
        {
            src.CopyTo(ref dst);
        }

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte src, ref SpanByte dst)
        {
            return src.TryCopyTo(ref dst);
        }
    }

    /// <summary>
    /// Callback functions for SpanByte key, value, input
    /// </summary>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class SpanByteFunctions<Output, Context> : SpanByteFunctions<SpanByte, Output, Context>
    {
    }

    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctions<Context> : SpanByteFunctions<SpanByteAndMemory, Context>
    {
        readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctions(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public unsafe override void SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            value.CopyTo(ref dst, memoryPool);
        }

        /// <inheritdoc />
        public unsafe override void ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            value.CopyTo(ref dst, memoryPool);
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Buffers;

namespace FASTER.stress
{
    internal class SpanByteValueFunctions<TKey> : SpanByteFunctions<TKey, SpanByteAndMemory, Empty>
    {
        private readonly MemoryPool<byte> memoryPool;

        public SpanByteValueFunctions(MemoryPool<byte> memoryPool = default) => this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;

        public override bool SingleReader(ref TKey key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            => ConcurrentReader(ref key, ref input, ref value, ref output, ref readInfo);

        public override bool ConcurrentReader(ref TKey key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            value.CopyTo(ref output, memoryPool);
            return true;
        }

        public override bool SingleWriter(ref TKey key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (dst.Length < input.Length)
                throw new ApplicationException("SingleWriter should have allocated enough space");
            return ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);
        }

        public override bool ConcurrentWriter(ref TKey key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (dst.Length < src.Length)
                return false;
            src.CopyTo(ref dst);
            // TODO: we're not using output yet for Upsert, so this would leak: src.CopyTo(ref output, memoryPool);
            return true;
        }

        public override bool InitialUpdater(ref TKey key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            if (value.Length < input.Length)
                throw new ApplicationException("InitialUpdater should have allocated enough space");
            input.CopyTo(ref value);
            input.CopyTo(ref output, memoryPool);
            return true;
        }

        bool copyInput;

        public override bool CopyUpdater(ref TKey key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            if (newValue.Length < input.Length)
                throw new ApplicationException("CopyUpdater should have allocated enough space");

            // For our purposes, input and oldValue are the same. To ensure we're reading correctly, we'll toggle between input and oldValue being copied to output.
            if (copyInput)
            {
                input.CopyTo(ref newValue);
                input.CopyTo(ref output, memoryPool);
            }
            else
            {
                oldValue.CopyTo(ref newValue);
                oldValue.CopyTo(ref output, memoryPool);
            }
            copyInput = !copyInput;
            return true;
        }

        public override bool InPlaceUpdater(ref TKey key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            if (value.Length < input.Length)
                return false;
            input.CopyTo(ref value);
            input.CopyTo(ref output, memoryPool);
            return true;
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// Callback functions for SpanByte key, value
    /// </summary>
    public class SpanByteFunctions<Key, Output, Context> : FunctionsBase<Key, SpanByte, SpanByte, Output, Context>
    {
        /// <inheritdoc />
        public override void SingleWriter(WriteReason reason, ref Key key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref Output output, ref RecordInfo recordInfo, long address)
        {
            src.CopyTo(ref dst);
        }

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref Key key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref Output output, ref RecordInfo recordInfo, long address)
        {
            if (dst.Length < src.Length)
            {
                return false;
            }

            // Option 1: write the source data, leaving the destination size unchanged. You will need
            // to mange the actual space used by the value if you stop here.
            src.CopyTo(ref dst);

            // We can adjust the length header on the serialized log, if we wish.
            // This method will also zero out the extra space to retain log scan correctness.
            dst.ShrinkSerializedLength(src.Length);

            return true;
        }

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref SpanByte input, ref SpanByte value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            input.CopyTo(ref value);
        }

        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref Output output, ref RecordInfo recordInfo, long address)
        {
            oldValue.CopyTo(ref newValue);
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref SpanByte input, ref SpanByte value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            // The default implementation of IPU simply writes input to destination, if there is space
            return ConcurrentWriter(ref key, ref input, ref input, ref value, ref output, ref recordInfo, address);
        }
    }

    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctions<Context> : SpanByteFunctions<SpanByte, SpanByteAndMemory, Context>
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
        public unsafe override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref RecordInfo recordInfo, long address)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public unsafe override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref RecordInfo recordInfo, long address)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }
    }

    /// <summary>
    /// Callback functions for SpanByte with byte[] output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctions_ByteArrayOutput<Context> : SpanByteFunctions<SpanByte, byte[], Context>
    {
        /// <inheritdoc />
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref RecordInfo recordInfo, long address)
        {
            dst = value.ToByteArray();
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref RecordInfo recordInfo, long address)
        {
            dst = value.ToByteArray();
            return true;
        }
    }
}

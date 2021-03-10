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
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="locking"></param>
        public SpanByteFunctions(bool locking = false) : base(locking)
        {
        }

        /// <inheritdoc />
        public override void SingleWriter(ref Key key, ref SpanByte src, ref SpanByte dst)
        {
            src.CopyTo(ref dst);
        }

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref Key key, ref SpanByte src, ref SpanByte dst)
        {
            if (locking) dst.SpinLock();

            // We can write the source (src) data to the existing destination (dst) in-place, 
            // only if there is sufficient space
            if (dst.Length < src.Length || dst.IsMarkedReadOnly())
            {
                dst.MarkReadOnly();
                if (locking) dst.Unlock();
                return false;
            }

            // Option 1: write the source data, leaving the destination size unchanged. You will need
            // to mange the actual space used by the value if you stop here.
            src.CopyTo(ref dst);

            // We can adjust the length header on the serialized log, if we wish.
            // This method will also zero out the extra space to retain log scan correctness.
            dst.ShrinkSerializedLength(src.Length);

            if (locking) dst.Unlock();
            return true;
        }

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref SpanByte input, ref SpanByte value)
        {
            input.CopyTo(ref value);
        }

        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue)
        {
            oldValue.CopyTo(ref newValue);
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref SpanByte input, ref SpanByte value)
        {
            // The default implementation of IPU simply writes input to destination, if there is space
            return ConcurrentWriter(ref key, ref input, ref value);
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
        /// <param name="locking"></param>
        public SpanByteFunctions(MemoryPool<byte> memoryPool = default, bool locking = false) : base(locking)
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
            if (locking) value.SpinLock();
            value.CopyTo(ref dst, memoryPool);
            if (locking) value.Unlock();
        }
    }

    /// <summary>
    /// Callback functions for SpanByte with byte[] output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctions_ByteArrayOutput<Context> : SpanByteFunctions<SpanByte, byte[], Context>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="locking"></param>
        public SpanByteFunctions_ByteArrayOutput(bool locking = false) : base(locking) { }

        /// <inheritdoc />
        public override void SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst)
        {
            dst = value.ToByteArray();
        }

        /// <inheritdoc />
        public override void ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst)
        {
            value.SpinLock();
            dst = value.ToByteArray();
            value.Unlock();
        }
    }
}

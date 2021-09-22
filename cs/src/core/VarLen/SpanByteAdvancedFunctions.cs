// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// Callback functions for SpanByte key, value
    /// </summary>
    public class SpanByteAdvancedFunctions<Key, Output, Context> : AdvancedFunctionsBase<Key, SpanByte, SpanByte, Output, Context>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="locking"></param>
        public SpanByteAdvancedFunctions(bool locking = false) : base(locking) { }

        /// <inheritdoc />
        public override void SingleWriter(ref Key key, ref SpanByte src, ref SpanByte dst)
        {
            src.CopyTo(ref dst);
        }

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref Key key, ref SpanByte src, ref SpanByte dst, ref RecordInfo recordInfo, long address)
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
        public override void InitialUpdater(ref Key key, ref SpanByte input, ref SpanByte value, ref Output output)
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
            return ConcurrentWriter(ref key, ref input, ref value, ref recordInfo, address);
        }
    }

    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteAdvancedFunctions<Context> : SpanByteAdvancedFunctions<SpanByte, SpanByteAndMemory, Context>
    {
        readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        /// <param name="locking"></param>
        public SpanByteAdvancedFunctions(MemoryPool<byte> memoryPool = default, bool locking = false) : base(locking)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public unsafe override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, long address)
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

        /// <inheritdoc />
        public override bool SupportsLocking => locking;

        /// <inheritdoc />
        public override void Lock(ref RecordInfo recordInfo, ref SpanByte key, ref SpanByte value, LockType lockType, ref long lockContext)
        {
            value.SpinLock();
        }

        /// <inheritdoc />
        public override bool Unlock(ref RecordInfo recordInfo, ref SpanByte key, ref SpanByte value, LockType lockType, long lockContext)
        {
            value.Unlock();
            return true;
        }
    }

    /// <summary>
    /// Callback functions for SpanByte with byte[] output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteAdvancedFunctions_ByteArrayOutput<Context> : SpanByteAdvancedFunctions<SpanByte, byte[], Context>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="locking"></param>
        public SpanByteAdvancedFunctions_ByteArrayOutput(bool locking = false) : base(locking) { }

        /// <inheritdoc />
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, long address)
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

        /// <inheritdoc />
        public override bool SupportsLocking => locking;

        /// <inheritdoc />
        public override void Lock(ref RecordInfo recordInfo, ref SpanByte key, ref SpanByte value, LockType lockType, ref long lockContext)
        {
            value.SpinLock();
        }

        /// <inheritdoc />
        public override bool Unlock(ref RecordInfo recordInfo, ref SpanByte key, ref SpanByte value, LockType lockType, long lockContext)
        {
            value.Unlock();
            return true;
        }
    }
}

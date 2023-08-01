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
        public override bool SingleWriter(ref Key key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            => DoSafeCopy(ref src, ref dst, ref upsertInfo);

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref Key key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref Output output, ref UpsertInfo upsertInfo) 
            => DoSafeCopy(ref src, ref dst, ref upsertInfo);

        private static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
        {
            // First get the full record length and clear it from the extra value space (if there is any). 
            // This ensures all bytes after the used value space are 0, which retains log-scan correctness.

            // For non-in-place operations, the new record may have been revivified, so standard copying procedure must be done;
            // For SpanByte we don't implement DisposeForRevivification, so any previous value is still there, and thus we must
            // zero unused value space to ensure log-scan correctness, just like in in-place updates.

            // IMPORTANT: usedValueLength and fullValueLength use .TotalSize, not .Length, to account for extra int.
            //            This is also consistent with SpanByteVarLenStruct.Length.
            upsertInfo.ClearExtraValueLength(ref dst, dst.TotalSize);

            // We want to set the used and extra lengths and Filler whether we succeed (to the new length) or fail (to the original length).
            var result = src.TrySafeCopyTo(ref dst, upsertInfo.FullValueLength);
            upsertInfo.SetUsedValueLength(ref dst, dst.TotalSize);
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref Key key, ref SpanByte input, ref SpanByte value, ref Output output, ref RMWInfo rmwInfo)
            => DoSafeCopy(ref input, ref value, ref rmwInfo);

        /// <inheritdoc/>
        public override bool CopyUpdater(ref Key key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref Output output, ref RMWInfo rmwInfo)
            => DoSafeCopy(ref oldValue, ref newValue, ref rmwInfo);

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref SpanByte input, ref SpanByte value, ref Output output, ref RMWInfo rmwInfo)
            // The default implementation of IPU simply writes input to destination, if there is space
            => DoSafeCopy(ref input, ref value, ref rmwInfo);

        private static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref RMWInfo rmwInfo)
        {
            // See comments in upsertInfo overload of this function.
            rmwInfo.ClearExtraValueLength(ref dst, dst.TotalSize);
            var result = src.TrySafeCopyTo(ref dst, rmwInfo.FullValueLength);
            rmwInfo.SetUsedValueLength(ref dst, dst.TotalSize);
            return result;
        }

        /// <inheritdoc/>
        /// <remarks>Avoids the "value = default" for added tombstone record, which do not have space for the payload</remarks>
        public override bool SingleDeleter(ref Key key, ref SpanByte value, ref DeleteInfo deleteInfo) => true;
    }

    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctions<Context> : SpanByteFunctions<SpanByte, SpanByteAndMemory, Context>
    {
        private protected readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctions(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public unsafe override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public unsafe override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
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
        public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            return true;
        }
    }
}

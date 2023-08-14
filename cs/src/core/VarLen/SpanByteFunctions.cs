// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Buffers;
using System.Runtime.CompilerServices;
using static FASTER.core.Utility;

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

        /// <summary>
        /// Utility function for SpanByte copying, Upsert version.
        /// </summary>
        public static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
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

        /// <summary>
        /// Utility function for SpanByte copying, RMW version.
        /// </summary>
        public static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref RMWInfo rmwInfo)
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

        /// <inheritdoc />
        public override void DisposeForRevivification(ref Key key, ref SpanByte value, int newKeySize)
        {
            // If the Key type requires disposal, or can shrink in size, the caller must override this to handle that.
        }
    }

    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for SpanByte key, value, input
    /// </summary>
    public class SpanByteFunctions<Context> : SpanByteFunctions<SpanByte, Context>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public SpanByteFunctions(MemoryPool<byte> memoryPool = default) : base(memoryPool)
        {
        }

        /// <inheritdoc />
        public override void DisposeForRevivification(ref SpanByte key, ref SpanByte value, int newKeySize)
            => ClearForRevivification(ref key, ref value, newKeySize);

        internal unsafe static void ClearForRevivification(ref SpanByte key, ref SpanByte value, int newKeySize)
        {
            // Our math here uses record alignment of keys as in the allocator, and assumes this will always be at least int alignment.
            newKeySize = RoundUp(newKeySize, VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment);
            var oldKeySize = RoundUp(key.TotalSize, VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment);

            // We don't have to do anything with the Value unless the new key requires adjusting the key length.
            int keySizeChange = newKeySize < 0 ? 0 : newKeySize - oldKeySize;
            if (keySizeChange != 0)
                return;

            // We are growing or shrinking. We don't care (here or in SingleWriter, InitialUpdater, CopyUpdater) what is inside the Key and Value,
            // as long as we don't leave nonzero bytes after the used value space. So we just need to make sure the Value space starts immediately
            // after the new key size. SingleWriter et al. will do the ShrinkSerializedLength on Value as needed.
            if (keySizeChange < 0)
            {
                // We are shrinking the key; the Value of the new record will start after key + newKeySize, so set the new value length there.
                *(int*)((byte*)Unsafe.AsPointer(ref key) + newKeySize) = value.Length - keySizeChange; // minus negative => plus positive
            }
            else
            { 
                // We are growing the key; the Value of the new record will start somewhere in the middle of where the old Value was, so set the new value length there.
                *(int*)((byte*)Unsafe.AsPointer(ref value) + keySizeChange) = value.Length - keySizeChange;
            }

            // NewKeySize is (newKey).TotalSize.
            key.Length = newKeySize - sizeof(int);
        }
    }

    /// <summary>
    /// Callback functions using SpanByteAndMemory output, for some Key type with SpanByte value and input
    /// </summary>
    public class SpanByteFunctions<Key, Context> : SpanByteFunctions<Key, SpanByteAndMemory, Context>
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
        public unsafe override bool SingleReader(ref Key key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public unsafe override bool ConcurrentReader(ref Key key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            value.CopyTo(ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public override void DisposeForRevivification(ref Key key, ref SpanByte value, int newKeySize)
        {
            // If the Key type requires disposal, or can shrink in size, the caller must override this to handle that.
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

        /// <inheritdoc />
        public override void DisposeForRevivification(ref SpanByte key, ref SpanByte value, int newKeySize)
            => SpanByteFunctions<Context>.ClearForRevivification(ref key, ref value, newKeySize);
    }

    /// <summary>
    /// Callback functions for SpanByte key with some other simple type for value, input, output
    /// </summary>
    public class SpanByteFunctions_SimpleValue<Value, Context> : SimpleFunctions<SpanByte, Value, Context>
    {
        /// <inheritdoc />
        public override bool SingleReader(ref SpanByte key, ref Value input, ref Value value, ref Value dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentReader(ref SpanByte key, ref Value input, ref Value value, ref Value dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc />
        public unsafe override void DisposeForRevivification(ref SpanByte key, ref Value value, int newKeySize)
        {
            // Store off the current value and zeroinit the value space. If this simple copy does not work for the Value type, this method must be overridden.
            var tempValue = value;
            value = default;

            // Zeroinit the now-unused key space. NewKeySize is (newKey).TotalSize.
            key.ShrinkSerializedLength(newKeySize - sizeof(int));
            key.Length = newKeySize - sizeof(int);

            // Our math here uses record alignment of keys as in the allocator, and assumes this will always be at least int alignment.
            // Do this after setting the new key length.
            newKeySize = RoundUp(newKeySize, VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment);

            // Restore the value.
            Unsafe.AsRef<Value>((byte*)Unsafe.AsPointer(ref key) + newKeySize) = tempValue;
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    using static Utility;

    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        private bool IsFixedLengthReviv => valueLengthStruct is null;
        private IVariableLengthStruct<Value> valueLengthStruct;
        internal VariableLengthBlittableAllocator<Key, Value> varLenAllocator;

        private bool InitializeRevivification(RevivificationSettings settings)
        {
            // Set these first in case revivification is not enabled; they still tell us not to expect fixed-length.
            valueLengthStruct = (this.hlog as VariableLengthBlittableAllocator<Key, Value>)?.ValueLength;

            if (settings is null) 
                return false;
            settings.Verify(IsFixedLengthReviv);
            if (!settings.EnableRevivification)
                return false;
            if (settings.FreeListBins?.Length > 0)
                this.FreeRecordPool = new FreeRecordPool<Key, Value>(this, settings, IsFixedLengthReviv ? hlog.GetAverageRecordSize() : -1);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long MinFreeRecordAddress(HashBucketEntry entry) => entry.Address >= this.hlog.ReadOnlyAddress ? entry.Address : this.hlog.ReadOnlyAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetValueOffset(long physicalAddress, ref Value recordValue) => (int)((long)Unsafe.AsPointer(ref recordValue) - physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe int* GetExtraValueLengthPointer(ref Value value, int usedValueLength)
        {
            Debug.Assert(RoundUp(usedValueLength, sizeof(int)) == usedValueLength, "GetLiveFullValueLengthPointer: usedValueLength should have int-aligned length");
            return (int*)((long)Unsafe.AsPointer(ref value) + usedValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetExtraValueLength(ref Value recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            if (IsFixedLengthReviv)
                recordInfo.Filler = false;
            else
                SetExtraVarLenValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetExtraVarLenValueLength(ref Value recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            usedValueLength = RoundUp(usedValueLength, sizeof(int));
            Debug.Assert(fullValueLength >= usedValueLength, $"SetFullValueLength: usedValueLength {usedValueLength} cannot be > fullValueLength {fullValueLength}");
            if (fullValueLength - usedValueLength >= sizeof(int))
            {
                *GetExtraValueLengthPointer(ref recordValue, usedValueLength) = fullValueLength - usedValueLength;
                recordInfo.Filler = true;
                return;
            }
            recordInfo.Filler = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int usedValueLength, int fullValueLength, int fullRecordLength) GetRecordLengths(long physicalAddress, ref Value recordValue, ref RecordInfo recordInfo)
        {
            // FixedLen may be GenericAllocator which does not point physicalAddress to the actual record location, so calculate fullRecordLength via GetAverageRecordSize().
            if (IsFixedLengthReviv)
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length, hlog.GetAverageRecordSize());

            int usedValueLength, fullValueLength, allocatedSize, valueOffset = GetValueOffset(physicalAddress, ref recordValue);
            if (recordInfo.Filler)
            {
                usedValueLength = valueLengthStruct.GetLength(ref recordValue);
                var alignedUsedValueLength = RoundUp(usedValueLength, sizeof(int));
                fullValueLength = alignedUsedValueLength + *GetExtraValueLengthPointer(ref recordValue, alignedUsedValueLength);
                Debug.Assert(fullValueLength >= usedValueLength, $"GetLengthsFromFiller: fullValueLength {fullValueLength} should be >= usedValueLength {usedValueLength}");
                allocatedSize = valueOffset + fullValueLength;
            }
            else
            {
                // Live varlen record with no stored sizes; we always have a default(Key) and default(Value). Return the full record length (including key), not just the value length.
                (int actualSize, allocatedSize) = hlog.GetRecordSize(physicalAddress);
                usedValueLength = actualSize - valueOffset;
                fullValueLength = allocatedSize - valueOffset;
            }

            Debug.Assert(usedValueLength >= 0, $"GetLiveRecordLengths: usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"GetLiveRecordLengths: fullValueLength {fullValueLength}");
            Debug.Assert(allocatedSize >= 0, $"GetLiveRecordLengths: fullRecordLength {allocatedSize}");
            return (usedValueLength, fullValueLength, allocatedSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int usedValueLength, int fullValueLength) GetNewValueLengths(int actualSize, int allocatedSize, long newPhysicalAddress, ref Value recordValue)
        {
            // Called after a new record is allocated
            if (IsFixedLengthReviv)
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length);

            int valueOffset = GetValueOffset(newPhysicalAddress, ref recordValue);
            int usedValueLength = actualSize - valueOffset;
            int fullValueLength = allocatedSize - valueOffset;
            Debug.Assert(usedValueLength >= 0, $"GetNewValueLengths: usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"GetNewValueLengths: fullValueLength {fullValueLength}");

            return (usedValueLength, fullValueLength);
        }

        // A "free record" is one on the FreeList.
        #region FreeRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo, int allocatedSize)
        {
            // Set default Key and Value, and store the full value length
            hlog.GetKey(physicalAddress) = default;
            ref Value recordValue = ref hlog.GetValue(physicalAddress);
            recordValue = default;

            // Skip the valuelength calls if we are not varlen.
            if (IsFixedLengthReviv)
            {
                recordInfo.Filler = false;
                return;
            }

            int usedValueLength = valueLengthStruct.GetLength(ref recordValue);
            int fullValueLength = allocatedSize - GetValueOffset(physicalAddress, ref recordValue);
            SetExtraVarLenValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo) 
            => IsFixedLengthReviv
                ? hlog.GetAverageRecordSize()
                : GetRecordLengths(physicalAddress, ref hlog.GetValue(physicalAddress), ref recordInfo).fullRecordLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryTakeFreeRecord(ref int allocatedSize, HashBucketEntry entry, out long logicalAddress, out long physicalAddress)
        {
            if (FreeRecordPoolHasSafeRecords && FreeRecordPool.TryTake(allocatedSize, MinFreeRecordAddress(entry), out logicalAddress))
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                Debug.Assert(recordInfo.IsSealed, "TryDequeueFreeRecord: recordInfo should still have the revivification Seal");

                // If IsFixedLengthReviv, the allocatedSize will be unchanged
                if (!IsFixedLengthReviv)
                {
                    Debug.Assert(recordInfo.Filler, "TryDequeueFreeRecord: recordInfo should have the Filler bit set for varlen");
                    var freeAllocatedSize = GetFreeRecordSize(physicalAddress, ref recordInfo);
                    recordInfo.Filler = false;
                    Debug.Assert(freeAllocatedSize >= allocatedSize, $"TryDequeueFreeRecord: freeAllocatedSize {freeAllocatedSize} should be >= allocatedSize {allocatedSize}");
                    allocatedSize = freeAllocatedSize;
                }

                // Now we can unseal; epoch management guarantees nobody is still executing who saw this record before it went into the free record pool.
                recordInfo.Unseal();
                return true;
            }

            // No free record available.
            logicalAddress = physicalAddress = default;
            return false;
        }

        #endregion FreeRecords

        // TombstonedRecords are in the tag chain with the tombstone bit set (they are not in the freelist). They preserve the key (they mark that key as deleted,
        // which is important if there is a subsequent record for that key), and store the full Value length after the used value data (if there is room).
        #region TombstonedRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetTombstoneAndFullValueLength(ref Value recordValue, ref RecordInfo recordInfo, int fullValueLength)
        {
            recordValue = default;
            recordInfo.Tombstone = true;
            if (IsFixedLengthReviv)
            {
                recordInfo.Filler = false;
                return;
            }

            var usedValueLength = RoundUp(valueLengthStruct.GetLength(ref recordValue), sizeof(int));
            SetExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool ok, int usedValueLength) TryReinitializeTombstonedValue(long physicalAddress, int actualSize, ref RecordInfo srcRecordInfo, ref Value recordValue, int fullValueLength)
        {
            // Don't change Filler if we don't have enough room to use the record; we need it to remain there for record traversal.
            var recordLength = GetValueOffset(physicalAddress, ref recordValue) + fullValueLength;
            if (recordLength < actualSize)
                return (false, 0);

            srcRecordInfo.Filler = false;
            hlog.GetAndInitializeValue(physicalAddress, physicalAddress + actualSize);
            return (true, valueLengthStruct.GetLength(ref recordValue));
        }

        #endregion TombstonedRecords
    }
}

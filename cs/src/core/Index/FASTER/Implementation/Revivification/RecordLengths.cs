// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    using static Utility;

    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        private bool IsFixedLengthReviv => varLenValueOnlyLengthStruct is null;
        private IVariableLengthStruct<Value> varLenValueOnlyLengthStruct;
        internal VariableLengthBlittableAllocator<Key, Value> varLenAllocator;

        private bool InitializeRevivification(RevivificationSettings settings, IVariableLengthStruct<Value> varLenStruct, bool fixedRecordLength)
        {
            if (settings is null) 
                return false;
            settings.Verify(fixedRecordLength);
            if (!settings.EnableRevivification)
                return false;
            varLenValueOnlyLengthStruct = varLenStruct;
            varLenAllocator = this.hlog as VariableLengthBlittableAllocator<Key, Value>;
            if (settings.FreeListBins?.Length > 0)
                this.FreeRecordPool = new FreeRecordPool<Key, Value>(this, settings, fixedRecordLength ? hlog.GetAverageRecordSize() : -1);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long MinFreeRecordAddress(HashBucketEntry entry) => entry.Address >= this.hlog.ReadOnlyAddress ? entry.Address : this.hlog.ReadOnlyAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetValueOffset(long physicalAddress, ref Value recordValue) => (int)((long)Unsafe.AsPointer(ref recordValue) - physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe int* GetLiveFullValueLengthPointer(ref Value value, int usedValueLength)
        {
            Debug.Assert(RoundUp(usedValueLength, sizeof(int)) == usedValueLength, "GetLiveFullValueLengthPointer: usedValueLength should have int-aligned length");
            return (int*)((long)Unsafe.AsPointer(ref value) + usedValueLength);
        }

        // LiveRecords are in a tag chain (not the FreeList) and not tombstoned
        #region LiveRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetLiveFullValueLength(long physicalAddress, ref Value value, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            if (IsFixedLengthReviv)
                return;
            usedValueLength = RoundUp(usedValueLength, sizeof(int));
            Debug.Assert(fullValueLength >= usedValueLength, $"SetLiveFullValueLength: usedValueLength {usedValueLength}, fullValueLength {fullValueLength}");
            int availableLength = fullValueLength - usedValueLength;
            Debug.Assert(availableLength >= 0, $"SetLiveFullValueLength: availableLength {availableLength}");
            if (availableLength >= sizeof(int))
            {
                *GetLiveFullValueLengthPointer(ref value, usedValueLength) = fullValueLength;
                recordInfo.Filler = true;
                return;
            }
            recordInfo.Filler = false;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int usedValueLength, int fullValueLength) GetLiveLengthsFromFiller<Input, Output, Context, FasterSession>(long physicalAddress, ref Value value, ref RecordInfo recordInfo, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(!IsFixedLengthReviv, "GetLiveLengthsFromFiller: Callers should have handled IsFixedLengthReviv");
            Debug.Assert(!recordInfo.Tombstone, "GetLiveLengthsFromFiller: Callers should have handled recordInfo.Tombstone");
            Debug.Assert(recordInfo.Filler, "GetLiveLengthsFromFiller: Callers should have ensured recordInfo.Filler");

            int usedValueLength = varLenValueOnlyLengthStruct.GetLength(ref value);
            int fullValueLength = *GetLiveFullValueLengthPointer(ref value, RoundUp(usedValueLength, sizeof(int))); // Get the length from the Value space after usedValueLength
            Debug.Assert(fullValueLength >= 0, $"GetLiveLengthsFromFiller: fullValueLength {fullValueLength}");
            Debug.Assert(fullValueLength >= usedValueLength, $"GetLiveLengthsFromFiller: usedValueLength {usedValueLength}, fullValueLength {fullValueLength}");
            return (usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int usedValueLength, int fullValueLength, int fullRecordLength) GetLiveRecordLengths<Input, Output, Context, FasterSession>(long physicalAddress, ref Value recordValue, ref RecordInfo recordInfo, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // FixedLen may be GenericAllocator which does not point physicalAddress to the actual record location.
            if (IsFixedLengthReviv)
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length, hlog.GetAverageRecordSize());

            int usedValueLength, fullValueLength, allocatedSize, valueOffset = GetValueOffset(physicalAddress, ref recordValue);
            if (recordInfo.Tombstone)
            {
                (usedValueLength, fullValueLength) = (0, GetFreeRecordSize(physicalAddress, ref recordInfo));
                allocatedSize = valueOffset + fullValueLength;
            }
            else if (recordInfo.Filler)
            {
                (usedValueLength, fullValueLength) = GetLiveLengthsFromFiller<Input, Output, Context, FasterSession>(physicalAddress, ref recordValue, ref recordInfo, fasterSession);
                allocatedSize = valueOffset + fullValueLength;
            }
            else
            {
                // Live varlen record with no stored sizes; get the full record length (including key), not just the value length.
                (int actualSize, allocatedSize) = hlog.GetRecordSize(physicalAddress);
                usedValueLength = actualSize - valueOffset;
                fullValueLength = allocatedSize - valueOffset;
            }
            Debug.Assert(usedValueLength >= 0, $"GetLiveRecordLengths: usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"GetLiveRecordLengths: fullValueLength {fullValueLength}");
            Debug.Assert(allocatedSize >= 0, $"GetLiveRecordLengths: fullRecordLength {allocatedSize}");
            return (usedValueLength, fullValueLength, allocatedSize);
        }

        #endregion LiveRecords

        // A "free record" is one on the FreeList; it does NOT preserve the key, so we store the full Value length in the key space.
        #region FreeRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo, int allocatedSize)
        {
            if (IsFixedLengthReviv || allocatedSize < RecordInfo.GetLength() + sizeof(int))
                return;
            Debug.Assert(RoundUp(allocatedSize, sizeof(int)) == allocatedSize, "SetFreeRecordSize: VarLen GetRecordSize() should have ensured nonzero int-aligned length");
            *GetFreeRecordSizePointer(physicalAddress) = allocatedSize;
            recordInfo.Filler = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo)
        {
            Debug.Assert(IsFixedLengthReviv || recordInfo.Filler, "GetFreeRecordSize: Varlen should have filler set");
            return IsFixedLengthReviv ? hlog.GetAverageRecordSize() : *GetFreeRecordSizePointer(physicalAddress);
        }

        // Use Key space for the value as FreeRecords do not preserve the key.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe int* GetFreeRecordSizePointer(long physicalAddress) => (int*)Unsafe.AsPointer(ref hlog.GetKey(physicalAddress));

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
                    *GetFreeRecordSizePointer(physicalAddress) = 0;
                    recordInfo.Filler = false;
                    Debug.Assert(freeAllocatedSize >= allocatedSize, $"TryDequeueFreeRecord: freeAllocatedSize {freeAllocatedSize} should be >= allocatedSize {allocatedSize}");
                    allocatedSize = freeAllocatedSize;
                }

                // Now we can unseal; epoch management guarantees nobody is still executing who saw this record before it went into the free record pool.
                recordInfo.Unseal();
                return true;
            }
            logicalAddress = physicalAddress = default;
            return false;
        }

        #endregion FreeRecords

        // TombstonedRecords are in the tag chain with the tombstone bit set (they are not in the freelist). They preserve the key (they mark that key as deleted,
        // which is important if there is a subsequent record for that key), and thus stores the full Value length after the used value data (if there is room).
        #region TombstonedRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetTombstonedValueLength(ref Value recordValue, ref RecordInfo recordInfo, int fullValueLength)
        {
            if (IsFixedLengthReviv || fullValueLength < sizeof(int))
            {
                recordInfo.Filler = false;
                return;
            }

            Debug.Assert(fullValueLength >= sizeof(long) && RoundUp(fullValueLength, sizeof(int)) == fullValueLength, "SetTombstonedValueLength: VarLen GetRecordSize() should have ensured nonzero int-aligned length");
            *(int*)Unsafe.AsPointer(ref recordValue) = fullValueLength;
            recordInfo.Filler = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetTombstonedValueLength(long physicalAddress, ref RecordInfo recordInfo)
        {
            Debug.Assert(recordInfo.Filler, "GetTombstonedValueLength: Filler should be set");
            return IsFixedLengthReviv ? FixedLengthStruct<Value>.Length : *GetTombstonedValueLengthPointer(physicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe int* GetTombstonedValueLengthPointer(long physicalAddress) => (int*)Unsafe.AsPointer(ref hlog.GetValue(physicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool ok, int usedValueLength) TryReinitializeTombstonedValue(long physicalAddress, int actualSize, ref RecordInfo srcRecordInfo, ref Value recordValue, int fullValueLength)
        {
            srcRecordInfo.Filler = false;
            var recordLength = GetValueOffset(physicalAddress, ref recordValue) + fullValueLength;
            if (recordLength < actualSize)
                return (false, 0);

            hlog.GetAndInitializeValue(physicalAddress, physicalAddress + actualSize);
            return (true, varLenValueOnlyLengthStruct.GetLength(ref recordValue));
        }

        #endregion TombstonedRecords
    }
}

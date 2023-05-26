// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        private bool IsFixedLengthReviv => varLenValueOnlyLengthStruct is null;
        private IVariableLengthStruct<Value> varLenValueOnlyLengthStruct;
        internal VariableLengthBlittableAllocator<Key, Value> varLenAllocator;

        private void InitializeRevivification(IVariableLengthStruct<Value> varLenStruct, int maxFreeRecordsInBin, bool fixedRecordLength)
        {
            varLenValueOnlyLengthStruct = varLenStruct;
            varLenAllocator = this.hlog as VariableLengthBlittableAllocator<Key, Value>;
            if (maxFreeRecordsInBin > 0)
                this.FreeRecordPool = new FreeRecordPool(maxFreeRecordsInBin, fixedRecordLength ? hlog.GetAverageRecordSize() : -1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int RoundupLengthToInt(int length) => (length + sizeof(int) - 1) & (~(sizeof(int) - 1));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long MinFreeRecordAddress(HashBucketEntry entry) => entry.Address > this.hlog.ReadOnlyAddress ? entry.Address : this.hlog.ReadOnlyAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int* GetValueFullLengthPointer(long physicalAddress, int usedValueLength)
        {
            Debug.Assert(RoundupLengthToInt(usedValueLength) == usedValueLength, "usedValueLength should have int-aligned length");
            return (int*)((byte*)physicalAddress + GetValueOffset(physicalAddress) + usedValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetValueOffset(long physicalAddress) => (int)(varLenAllocator.ValueOffset(physicalAddress) - physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetValueOffset(long physicalAddress, ref Value recordValue) => (int)((long)Unsafe.AsPointer(ref recordValue) - physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetRecordLength(long physicalAddress, ref Value recordValue, int fullValueLength) => (int)((long)Unsafe.AsPointer(ref recordValue) - physicalAddress) + fullValueLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetLengths(long physicalAddress, ref Value value, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            if (IsFixedLengthReviv)
                return;
            usedValueLength = RoundupLengthToInt(usedValueLength);
            Debug.Assert(fullValueLength >= usedValueLength, $"usedValueLength {usedValueLength}, fullValueLength {fullValueLength}");
            int availableLength = fullValueLength - usedValueLength;
            Debug.Assert(availableLength >= 0, $"availableLength {availableLength}");
            if (availableLength >= sizeof(int))
            {
                *GetValueFullLengthPointer(physicalAddress, usedValueLength) = fullValueLength;
                recordInfo.Filler = true;
                return;
            }
            recordInfo.Filler = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int usedValueLength, int fullValueLength) GetLengths(int actualSize, int allocatedSize, long newPhysicalAddress)
        {
            if (IsFixedLengthReviv)
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length);

            int valueOffset = GetValueOffset(newPhysicalAddress);
            int actualValueLength = actualSize - valueOffset;
            int fullValueLength = allocatedSize - valueOffset;
            Debug.Assert(actualValueLength >= 0, $"actualValueLength {actualValueLength}");
            Debug.Assert(fullValueLength >= 0, $"fullValueLength {fullValueLength}");

            return (actualValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int usedValueLength, int fullValueLength) GetLengthsFromFiller<Input, Output, Context, FasterSession>(long physicalAddress, ref Value value, ref RecordInfo recordInfo, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(!IsFixedLengthReviv, "Callers should have handled IsFixedLengthReviv");
            Debug.Assert(!recordInfo.Tombstone, "Callers should have handled recordInfo.Tombstone");
            Debug.Assert(recordInfo.Filler, "Callers should have ensured recordInfo.Filler");

            int actualValueLength = varLenValueOnlyLengthStruct.GetLength(ref value);
            int usedValueLength = RoundupLengthToInt(actualValueLength);
            int fullValueLength = *GetValueFullLengthPointer(physicalAddress, usedValueLength); // Get the length from the Value space after usedValueLength
            Debug.Assert(fullValueLength >= 0, $"fullValueLength {fullValueLength}");
            Debug.Assert(fullValueLength >= usedValueLength, $"usedValueLength {usedValueLength}, fullValueLength {fullValueLength}");
            return (actualValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int usedValueLength, int fullValueLength) GetValueLengths<Input, Output, Context, FasterSession>(long physicalAddress, ref Value recordValue, ref RecordInfo recordInfo, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (IsFixedLengthReviv)
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length);
            if (recordInfo.Tombstone)
                return (0, GetFreeRecordSize(physicalAddress, ref recordInfo));

            // Only get valueOffset if we need it.
            if (recordInfo.Filler)
                return GetLengthsFromFiller<Input, Output, Context, FasterSession>(physicalAddress, ref recordValue, ref recordInfo, fasterSession);

            // Get the full record length (including key), not just the value length.
            var (actualRecordLength, fullRecordLength) = hlog.GetRecordSize(physicalAddress);
            var valueOffset = GetValueOffset(physicalAddress);
            var usedValueLength = actualRecordLength - valueOffset;
            var fullValueLength = fullRecordLength - valueOffset;
            Debug.Assert(usedValueLength >= 0, $"usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"fullValueLength {fullValueLength}");
            return (usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (int usedValueLength, int fullValueLength, int fullRecordLength) GetRecordLengths<Input, Output, Context, FasterSession>(long physicalAddress, ref Value recordValue, ref RecordInfo recordInfo, FasterSession fasterSession)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            int fullRecordLength;
            if (IsFixedLengthReviv)
            {
                (_, fullRecordLength) = hlog.GetRecordSize(physicalAddress);
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length, fullRecordLength);
            }

            int valueOffset = GetValueOffset(physicalAddress);
            int usedValueLength, fullValueLength;
            if (recordInfo.Tombstone)
            {
                (usedValueLength, fullValueLength) = (0, GetFreeRecordSize(physicalAddress, ref recordInfo));
                return (usedValueLength, fullValueLength, valueOffset + fullValueLength);
            }

            if (recordInfo.Filler)
            {
                (usedValueLength, fullValueLength) = GetLengthsFromFiller<Input, Output, Context, FasterSession>(physicalAddress, ref recordValue, ref recordInfo, fasterSession);
                fullRecordLength = valueOffset + fullValueLength;
            }
            else
            {
                // Get the full record length (including key), not just the value length.
                int actualSize;
                (actualSize, fullRecordLength) = hlog.GetRecordSize(physicalAddress);
                usedValueLength = actualSize - valueOffset;
                fullValueLength = fullRecordLength - valueOffset;
            }
            Debug.Assert(usedValueLength >= 0, $"usedValueLength {usedValueLength}");
            Debug.Assert(fullValueLength >= 0, $"fullValueLength {fullValueLength}");
            Debug.Assert(fullRecordLength >= 0, $"fullRecordLength {fullRecordLength}");
            return (usedValueLength, fullValueLength, fullRecordLength);
        }

        // A "free record" is one on the FreeList; it does NOT preserve the key.
        #region FreeRecords
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo, int allocatedSize)
        {
            if (!IsFixedLengthReviv)
            {
                Debug.Assert(allocatedSize >= sizeof(long) && RoundupLengthToInt(allocatedSize) == allocatedSize, "VarLen GetRecordSize() should have ensured nonzero int-aligned length");
                *GetFreeRecordSizePointer(physicalAddress) = allocatedSize;
                recordInfo.Filler = true;
            }
        }

        // A "free record" is one on the FreeList; it does NOT preserve the key.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo)
        {
            Debug.Assert(recordInfo.Filler, "Should have filler set");
            return IsFixedLengthReviv ? hlog.GetAverageRecordSize() : *GetFreeRecordSizePointer(physicalAddress);
        }

        // Use Key space for the value as FreeRecords do not preserve the key.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe int* GetFreeRecordSizePointer(long physicalAddress) => (int*)Unsafe.AsPointer(ref hlog.GetKey(physicalAddress));
        #endregion FreeRecords

        // TombstonedRecords are in the tag chain with the tombstone bit set (they are not in the freelist). They preserve the key (they mark that key as deleted,
        // which is important if there is a subsequent record for that key).
        #region TombstonedRecords
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetTombstonedValueLength(long physicalAddress, ref RecordInfo recordInfo, int fullValueLength)
        {
            if (!IsFixedLengthReviv)
            {
                Debug.Assert(fullValueLength >= sizeof(long) && RoundupLengthToInt(fullValueLength) == fullValueLength, "VarLen GetRecordSize() should have ensured nonzero int-aligned length");
                *GetTombstonedValueLengthPointer(physicalAddress) = fullValueLength;
                recordInfo.Filler = true;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetTombstonedValueLength(long physicalAddress, ref RecordInfo recordInfo)
        {
            Debug.Assert(recordInfo.Filler, "Should have filler set");
            return IsFixedLengthReviv ? FixedLengthStruct<Value>.Length : *GetTombstonedValueLengthPointer(physicalAddress);
        }

        // Use Value space as TombstonedRecords preserve the key.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe int* GetTombstonedValueLengthPointer(long physicalAddress) => (int*)Unsafe.AsPointer(ref hlog.GetValue(physicalAddress));
        #endregion TombstonedRecords

        bool TryDequeueFreeRecord(ref int allocatedSize, HashBucketEntry entry, out long logicalAddress, out long physicalAddress)
        {
            if (FreeRecordPoolHasRecords && FreeRecordPool.Dequeue(allocatedSize, MinFreeRecordAddress(entry), this, out logicalAddress))
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                Debug.Assert(recordInfo.IsSealed, "recordInfo should still have the revivification Seal");

                // If IsFixedLengthReviv, the allocatedSize will be unchanged
                if (!IsFixedLengthReviv)
                {
                    Debug.Assert(recordInfo.Filler, "recordInfo should have the Filler bit set for varlen");
                    var freeAllocatedSize = GetFreeRecordSize(physicalAddress, ref recordInfo);
                    *GetFreeRecordSizePointer(physicalAddress) = 0;
                    recordInfo.Filler = false;
                    Debug.Assert(freeAllocatedSize >= allocatedSize, $"freeAllocatedSize {freeAllocatedSize} should be >= allocatedSize {allocatedSize}"); 
                    allocatedSize = freeAllocatedSize;
                }

                // We are finally safe to unseal, since epoch management guarantees nobody is still executing who could
                // have seen this record before it went into the free record pool.
                recordInfo.Unseal();
                return true;
            }
            logicalAddress = physicalAddress = default;
            return false;
        }

        private int ReInitializeValue(long physicalAddress, int actualSize, ref RecordInfo srcRecordInfo, ref Value recordValue)
        {
            byte* valuePointer = (byte*)Unsafe.AsPointer(ref recordValue);
            srcRecordInfo.Filler = false;
            hlog.GetAndInitializeValue(physicalAddress, physicalAddress + actualSize);
            return (int)(physicalAddress + actualSize - (long)valuePointer);
        }
    }
}

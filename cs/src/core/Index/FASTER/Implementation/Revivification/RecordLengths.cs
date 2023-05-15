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
        internal static int RoundupLength(int length) => (length + sizeof(int) - 1) & (~(sizeof(int) - 1));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long MinFreeRecordAddress(HashBucketEntry entry) => entry.Address > this.hlog.ReadOnlyAddress ? entry.Address : this.hlog.ReadOnlyAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int* GetValueLengthPointer(long physicalAddress, int usedValueLength)
        {
            Debug.Assert(RoundupLength(usedValueLength) == usedValueLength, "usedValueLength should have int-aligned length");
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
            usedValueLength = RoundupLength(usedValueLength);
            Debug.Assert(fullValueLength >= usedValueLength, $"usedValueLength {usedValueLength}, fullValueLength {fullValueLength}");
            int availableLength = fullValueLength - usedValueLength;
            Debug.Assert(availableLength >= 0, $"availableLength {availableLength}");
            if (availableLength >= sizeof(int))
            {
                *GetValueLengthPointer(physicalAddress, usedValueLength) = fullValueLength;
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
            int usedValueLength = RoundupLength(actualValueLength);
            int fullValueLength = *GetValueLengthPointer(physicalAddress, usedValueLength); // Get the length from the Value space after usedValueLength
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
                return (GetDeletedValueLengths(physicalAddress, ref recordInfo));

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
                (usedValueLength, fullValueLength) = GetDeletedValueLengths(physicalAddress, ref recordInfo);
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

        // Deleted value invariant: the full value length starts at the beginning of the value offset.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void SetDeletedValueLengths(long physicalAddress, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            if (!IsFixedLengthReviv)
            {
                Debug.Assert(usedValueLength >= 0, $"usedValueLength {usedValueLength}");
                Debug.Assert(fullValueLength >= sizeof(long) && RoundupLength(fullValueLength) == fullValueLength, "VarLen GetRecordSize() should have ensured nonzero record-aligned length");
                int* ptr = GetValueLengthPointer(physicalAddress, 0);
                *ptr = fullValueLength;
                *(ptr + 1) = usedValueLength;
                recordInfo.Filler = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int usedValueLength, int fullValueLength) GetDeletedValueLengths(long physicalAddress, ref RecordInfo recordInfo)
        {
            Debug.Assert(recordInfo.Filler, "Should have filler set");
            if (IsFixedLengthReviv)
                return (FixedLengthStruct<Value>.Length, FixedLengthStruct<Value>.Length);
            int* ptr = GetValueLengthPointer(physicalAddress, 0);
            return (*(ptr + 1), *ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetDeletedRecordLength(long physicalAddress, ref RecordInfo recordInfo)
        {
            int valueOffset = GetValueOffset(physicalAddress);
            return valueOffset + GetDeletedValueLengths(physicalAddress, ref recordInfo).fullValueLength;
        }

        bool TryDequeueFreeRecord(ref int allocatedSize, HashBucketEntry entry, out long logicalAddress, out long physicalAddress)
        {
            if (FreeRecordPoolHasRecords && FreeRecordPool.Dequeue(allocatedSize, MinFreeRecordAddress(entry), this, out logicalAddress))
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                if (IsFixedLengthReviv)
                {
                    allocatedSize = FixedLengthStruct<Value>.Length;
                }
                else
                {
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    int valueOffset = GetValueOffset(physicalAddress);
                    allocatedSize = valueOffset + GetDeletedValueLengths(physicalAddress, ref recordInfo).fullValueLength;
                }
                return true;
            }
            logicalAddress = physicalAddress = default;
            return false;
        }

        private int ReInitialize(long physicalAddress, int actualSize, int fullValueLength, RecordInfo recordInfo, ref Value recordValue)
        {
            // Zero memory, call GetValue to re-initialize, and remove Filler
            byte* valuePointer = (byte*)Unsafe.AsPointer(ref recordValue);
            Native32.ZeroMemory(valuePointer, fullValueLength);
            hlog.GetAndInitializeValue(physicalAddress, physicalAddress + actualSize);
            recordInfo.Filler = false;
            return (int)(physicalAddress + actualSize - (long)valuePointer);
        }
    }
}

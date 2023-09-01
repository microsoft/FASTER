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

        // This has to be here instead of in the FreeRecordPool because it's also used for in-chain revivification and the FreeRecordPool may be null.
        internal double mutableRevivificationMultiplier;

        private bool InitializeRevivification(RevivificationSettings settings)
        {
            // Set these first in case revivification is not enabled; they still tell us not to expect fixed-length.
            valueLengthStruct = (this.hlog as VariableLengthBlittableAllocator<Key, Value>)?.ValueLength;

            this.mutableRevivificationMultiplier = settings is null ? 1.0 : settings.MutablePercent / 100.0;

            if (settings is null) 
                return false;
            settings.Verify(IsFixedLengthReviv);
            if (!settings.EnableRevivification)
                return false;
            if (settings.FreeRecordBins?.Length > 0)
                this.FreeRecordPool = new FreeRecordPool<Key, Value>(this, settings, IsFixedLengthReviv ? hlog.GetAverageRecordSize() : -1);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetMinRevivificationAddress()
        {
            var readOnlyAddress = hlog.ReadOnlyAddress;
            var tailAddress = hlog.GetTailAddress();
            return tailAddress - (long)((tailAddress - readOnlyAddress) * this.mutableRevivificationMultiplier);
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
                SetVarLenExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void SetVarLenExtraValueLength(ref Value recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            usedValueLength = RoundUp(usedValueLength, sizeof(int));
            Debug.Assert(fullValueLength >= usedValueLength, $"SetFullValueLength: usedValueLength {usedValueLength} cannot be > fullValueLength {fullValueLength}");
            int extraValueLength = fullValueLength - usedValueLength;
            if (extraValueLength >= sizeof(int))
            {
                var extraValueLengthPtr = GetExtraValueLengthPointer(ref recordValue, usedValueLength);
                Debug.Assert(*extraValueLengthPtr == 0 || *extraValueLengthPtr == extraValueLength, "existing ExtraValueLength should be 0 or the same value");

                // We always store the "extra" as the difference between the aligned usedValueLength and the fullValueLength.
                // However, the UpdateInfo structures use the unaligned usedValueLength; aligned usedValueLength is not visible to the user.
                *extraValueLengthPtr = extraValueLength;
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
                // Live varlen record with no stored sizes; we always have a Key and Value (even if defaults). Return the full record length (including recordInfo and Key).
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
            Debug.Assert(fullValueLength >= RoundUp(usedValueLength, sizeof(int)), $"GetNewValueLengths: usedValueLength {usedValueLength} cannot be > fullValueLength {fullValueLength}");

            return (usedValueLength, fullValueLength);
        }

        // A "free record" is one on the FreeList.
        #region FreeRecords

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo, int allocatedSize)
        {
            // Skip the valuelength calls if we are not varlen.
            if (IsFixedLengthReviv)
            {
                recordInfo.Filler = false;
                return;
            }

            // Store the full value length. Defer clearing the Key until the record is revivified (it may never be).
            ref Value recordValue = ref hlog.GetValue(physicalAddress);
            int usedValueLength = valueLengthStruct.GetLength(ref recordValue);
            int fullValueLength = allocatedSize - GetValueOffset(physicalAddress, ref recordValue);
            SetVarLenExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetFreeRecordSize(long physicalAddress, ref RecordInfo recordInfo) 
            => IsFixedLengthReviv
                ? hlog.GetAverageRecordSize()
                : GetRecordLengths(physicalAddress, ref hlog.GetValue(physicalAddress), ref recordInfo).fullRecordLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ClearExtraValueSpace(ref RecordInfo recordInfo, ref Value recordValue, int usedValueLength, int fullValueLength)
        {
            // SpanByte's implementation of GetAndInitializeValue does not clear the space after usedValueLength. This may be
            // considerably less than the previous value length, so we clear it here before DisposeForRevivification. This space
            // includes the extra value length if Filler is set, so we must clear the space before clearing the Filler bit so
            // log-scan traversal does not see nonzero values past Value (it's fine if we see the Filler and extra length is 0).
            int extraValueLength = fullValueLength - usedValueLength;   // do not round up usedValueLength; we must clear all extra bytes
            if (extraValueLength > 0)
                SpanByte.Clear((byte*)Unsafe.AsPointer(ref recordValue) + usedValueLength, extraValueLength);
            recordInfo.Filler = false;
        } 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryTakeFreeRecord<Input, Output, Context, FasterSession>(FasterSession fasterSession, int requiredSize, ref int allocatedSize, int newKeySize, HashBucketEntry entry, 
                    out long logicalAddress, out long physicalAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (FreeRecordPoolHasSafeRecords && FreeRecordPool.TryTake(allocatedSize, MinFreeRecordAddress(entry), out logicalAddress))
            {
                physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                Debug.Assert(recordInfo.IsSealed, "TryTakeFreeRecord: recordInfo should still have the revivification Seal");

                // If IsFixedLengthReviv, the allocatedSize will be unchanged
                if (!IsFixedLengthReviv)
                {
                    var (usedValueLength, fullValueLength, fullRecordLength) = GetRecordLengths(physicalAddress, ref hlog.GetValue(physicalAddress), ref recordInfo);

                    // Zero the end of the value space between required and full value lengths and clear the Filler.
                    var valueOffset = fullRecordLength - fullValueLength;
                    var requiredValueLength = requiredSize - valueOffset;
                    var minValueLength = requiredValueLength < usedValueLength ? requiredValueLength : usedValueLength;
                    ref var recordValue = ref hlog.GetValue(physicalAddress);
                    Debug.Assert(valueOffset == (long)Unsafe.AsPointer(ref recordValue) - physicalAddress);
                    ClearExtraValueSpace(ref recordInfo, ref recordValue, minValueLength, fullValueLength);
                    
                    // Dispose any existing key and value. We defer this to here, at record retrieval time, because that is when we know it is needed
                    // (the record may never be Taken before it falls below ReadOnlyAddress, for example). We don't want the app to know about Filler
                    // (except for non-SpanByte varlens, which will have to copy the logic in SpanByteFunctions and UpsertInfo to manage shrinking lengths),
                    // so we've cleared out any extraValueLength entry to ensure the space beyond usedValueLength is zero'd for log-scan correctness.
                    fasterSession.DisposeForRevivification(ref hlog.GetKey(physicalAddress), ref recordValue, newKeySize, ref recordInfo);

                    Debug.Assert(fullRecordLength >= allocatedSize, $"TryTakeFreeRecord: fullRecordLength {fullRecordLength} should be >= allocatedSize {allocatedSize}");
                    allocatedSize = fullRecordLength;
                }

                // Preserve the Sealed bit due to checkpoint/recovery; see RecordInfo.WriteInfo.
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
        internal void SetTombstoneAndExtraValueLength(ref Value recordValue, ref RecordInfo recordInfo, int usedValueLength, int fullValueLength)
        {
            recordInfo.Tombstone = true;
            if (IsFixedLengthReviv)
            {
                recordInfo.Filler = false;
                return;
            }

            Debug.Assert(usedValueLength == valueLengthStruct.GetLength(ref recordValue));
            SetVarLenExtraValueLength(ref recordValue, ref recordInfo, usedValueLength, fullValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool ok, int usedValueLength) TryReinitializeTombstonedValue<Input, Output, Context, FasterSession>(FasterSession fasterSession, 
                ref RecordInfo srcRecordInfo, ref Key key, ref Value recordValue, int requiredSize, (int usedValueLength, int fullValueLength, int allocatedSize) recordLengths)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (IsFixedLengthReviv || recordLengths.allocatedSize < requiredSize)
                return (false, recordLengths.usedValueLength);

            // Zero the end of the value space between required and full value lengths and clear the Filler.
            var valueOffset = recordLengths.allocatedSize - recordLengths.fullValueLength;
            var requiredValueLength = requiredSize - valueOffset;
            var minValueLength = requiredValueLength < recordLengths.usedValueLength ? requiredValueLength : recordLengths.usedValueLength;
            ClearExtraValueSpace(ref srcRecordInfo, ref recordValue, minValueLength, recordLengths.fullValueLength);

            fasterSession.DisposeForRevivification(ref key, ref recordValue, newKeySize: -1, ref srcRecordInfo);
            srcRecordInfo.Tombstone = false;

            SetExtraValueLength(ref recordValue, ref srcRecordInfo, recordLengths.usedValueLength, recordLengths.fullValueLength);
            return (true, valueLengthStruct.GetLength(ref recordValue));
        }

        #endregion TombstonedRecords
    }
}

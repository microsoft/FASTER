// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static FASTER.core.Utility;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = sizeof(long) * 2)]
    internal struct FreeRecord
    {
        internal const int kSizeBits = 64 - RecordInfo.kPreviousAddressBits;
        const int kSizeShiftInWord = RecordInfo.kPreviousAddressBits;

        const long kSizeMask = RevivificationBin.MaxInlineRecordSize - 1;
        const long kSizeMaskInWord = kSizeMask << kSizeShiftInWord;

        // This is the "latch" on the FreeRecord; a thread that is Set()ing or Take()ing will CAS to this to "own" the record
        // and do the update. No other thread is allowed to touch the record while the epoch is this value.
        const long Epoch_Latched = -1;

        // Marks the record as empty. LightEpoch.CurrentEpoch is initialized to 1, so this is not a valid epoch value (and is the default value for long).
        const long Epoch_Empty = 0;

        #region Instance data
        // 'word' contains the reclaimable logicalAddress and the size of the record at that address.
        [FieldOffset(0)]
        private long word;

        // The epoch in which this record was Added; it cannot be Taken until all threads are past that epoch.
        // It may also be one of the Epoch_* constants, for concurrency control.
        [FieldOffset(8)]
        internal long addedEpoch;

        internal const int StructSize = sizeof(long) * 2;
        #endregion Instance data

        public long Address
        { 
            readonly get => word & RecordInfo.kPreviousAddressMaskInWord;
            set => word = (word & ~RecordInfo.kPreviousAddressMaskInWord) | (value & RecordInfo.kPreviousAddressMaskInWord);
        }

        public int Size => (int)((word & kSizeMaskInWord) >> kSizeShiftInWord);

        /// <inheritdoc/>
        public override string ToString()
        {
            string epochStr = this.addedEpoch switch
            {
                Epoch_Latched => "Latched",
                Epoch_Empty => "Empty",
                _ => this.addedEpoch.ToString()
            };
            return $"epoch {epochStr}, address {this.Address}, size {this.Size}";
        }

        internal readonly bool IsSet => IsSetEpoch(this.addedEpoch);
        internal static bool IsSetEpoch(long epoch) => epoch > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryLatch(long epoch) => Interlocked.CompareExchange(ref this.addedEpoch, Epoch_Latched, epoch) == epoch;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Set<Key, Value>(long address, long recordSize, FasterKV<Key, Value> fkv)
        {
            // If the record is empty or the address is below mutable, set the new address into it.
            var epoch = this.addedEpoch;
            if ((epoch == Epoch_Empty || (IsSetEpoch(epoch) && this.Address < fkv.hlog.ReadOnlyAddress)) && TryLatch(epoch))
            {
                // Doublecheck in case another thread in the same epoch wrote into it with a later address than before.
                if (this.Address >= fkv.hlog.ReadOnlyAddress)
                {
                    this.addedEpoch = epoch;   // Unlatches
                    return false;
                }

                // Ignore overflow due to oversize here--we check for that on Take()
                this.word = (recordSize << kSizeShiftInWord) | (address & RecordInfo.kPreviousAddressMaskInWord);
                this.addedEpoch = fkv.epoch.CurrentEpoch;   // Unlatches
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetEmpty()
        {
            this.word = 0;  // Must be first, so we retain Epoch_Latched until we're ready to reset it to Epoch_Empty
            this.addedEpoch = Epoch_Empty;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SetEmptyAtomic()
        {
            var epoch = this.addedEpoch;
            if (IsSetEpoch(epoch) && TryLatch(epoch))
                SetEmpty();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryPeek<Key, Value>(long recordSize, FasterKV<Key, Value> fkv, bool oversize, long minAddress, ref int bestFitSize, ref long lowestFailedEpoch)
        {
            FreeRecord oldRecord = this;
            if (!oldRecord.IsSet)
                return false;
            if (oldRecord.Address < minAddress)
            {
                if (oldRecord.Address < fkv.hlog.ReadOnlyAddress)
                    SetEmptyAtomic();
                else
                { }
                return false;
            }
            var thisSize = oversize ? GetRecordSize(fkv, oldRecord.Address) : oldRecord.Size;
            if (thisSize < recordSize)
                return false;
            if (oldRecord.addedEpoch > fkv.epoch.SafeToReclaimEpoch)
            {
                if (oldRecord.addedEpoch < lowestFailedEpoch)
                    lowestFailedEpoch = oldRecord.addedEpoch;
                return false;
            }

            if (bestFitSize > thisSize)
                bestFitSize = thisSize;
            return thisSize == recordSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTake<Key, Value>(int recordSize, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            address = 0;

            // This is subject to extremely unlikely ABA--someone else could Set() a new address with the same epoch.
            // If so, it doesn't matter; we just return the address that's there.
            var epoch = this.addedEpoch;
            FreeRecord oldRecord = this;
            if (!IsSetEpoch(epoch) || epoch > fkv.epoch.SafeToReclaimEpoch || oldRecord.Size < recordSize || oldRecord.Address < minAddress
                || !TryLatch(epoch))
            {
                // Not set, or failed to CAS the epoch; leave 'word' unchanged
                return false;
            }

            // At this point we must unlatch. Recheck 'word' after the latch.
            if (this.Size >= recordSize && this.Address >= minAddress)
            {
                address = this.Address;
                SetEmpty();
                return true;
            }

            if (this.Address < fkv.hlog.ReadOnlyAddress)
                SetEmpty();
            else
                this.addedEpoch = epoch;    // Restore for the next caller to try
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetRecordSize<Key, Value>(FasterKV<Key, Value> fkv, long logicalAddress)
        {
            // Because this is oversize, we need hlog to get the length out of the record's value (it won't fit in FreeRecord.kSizeBits)
            long physicalAddress = fkv.hlog.GetPhysicalAddress(logicalAddress);
            return fkv.GetFreeRecordSize(physicalAddress, ref fkv.hlog.GetInfo(physicalAddress));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryTakeOversize<Key, Value>(long recordSize, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            address = 0;

            // The difference in this oversize version is that we delay checking size until after the CAS, because we have
            // go go the slow route of getting the physical address.
            var epoch = this.addedEpoch;
            if (!IsSetEpoch(epoch) || epoch > fkv.epoch.SafeToReclaimEpoch || this.Address < minAddress || !TryLatch(epoch))
            {
                // Not set, or failed to CAS the epoch; leave 'word' unchanged
                return false;
            }

            // At this point we must unlatch. Recheck 'word' after the latch.
            if (this.Address >= minAddress)
            { 
                long thisSize = GetRecordSize(fkv, this.Address);
                if (thisSize >= recordSize)
                {
                    address = this.Address;
                    SetEmpty();
                    return true;
                }
            }

            if (this.Address < fkv.hlog.ReadOnlyAddress)
                SetEmpty();
            else
                this.addedEpoch = epoch;    // Restore for the next caller to try
            return false;
        }
    }

    internal unsafe class FreeRecordBin : IDisposable
    {
        internal const int MinRecordsPerBin = 8;    // Make sure we have enough to be useful
        internal const int MinSegmentSize = MinRecordsPerBin;

        private readonly FreeRecord[] recordsArray;
        internal readonly int maxRecordSize, recordCount;
        private readonly int minRecordSize, segmentSize, segmentCount, segmentRecordSizeIncrement;

        internal readonly FreeRecord* records;
#if !NET5_0_OR_GREATER
        private readonly GCHandle recordsHandle;
#endif

        readonly int bestFitScanLimit;

        /// <inheritdoc/>
        public override string ToString()
        {
            string scanStr = this.bestFitScanLimit switch
            {
                RevivificationBin.BestFitScanAll => "ScanAll",
                RevivificationBin.UseFirstFit => "FirstFit",
                _ => this.bestFitScanLimit.ToString()
            };
            return $"recSizes {minRecordSize}..{maxRecordSize}, recSizeInc {segmentRecordSizeIncrement}, #recs {recordCount}; segments: segSize {segmentSize}, #segs {segmentCount}; scanLimit {scanStr}";
        }

        internal FreeRecordBin(ref RevivificationBin binDef, int prevBinRecordSize, bool isFixedLength = false)
        {
            // If the size range is too much for the number of records in the bin, we must allow multiple sizes per segment.
            // prevBinRecordSize is already verified to be a multiple of 8.
            var bindefRecordSize = RoundUp(binDef.RecordSize, 8);
            if (isFixedLength || bindefRecordSize == prevBinRecordSize + 8)
            {
                this.bestFitScanLimit = RevivificationBin.UseFirstFit;

                this.segmentSize = RoundUp(binDef.NumberOfRecords, MinSegmentSize);
                this.segmentCount = 1;
                this.segmentRecordSizeIncrement = 1;  // For the division and multiplication in GetSegmentStart
                this.minRecordSize = this.maxRecordSize = isFixedLength ? prevBinRecordSize : bindefRecordSize;
            }
            else
            {
                this.bestFitScanLimit = binDef.BestFitScanLimit;

                // minRecordSize is already verified to be a multiple of 8.
                var sizeRange = bindefRecordSize - prevBinRecordSize;

                this.segmentCount = sizeRange / 8;
                this.segmentSize = (int)Math.Ceiling(binDef.NumberOfRecords / (double)this.segmentCount);

                if (this.segmentSize >= MinSegmentSize)
                    this.segmentSize = RoundUp(this.segmentSize, MinSegmentSize);
                else
                {
                    this.segmentSize = MinSegmentSize;
                    this.segmentCount = (int)Math.Ceiling(binDef.NumberOfRecords / (double)this.segmentSize);
                }

                this.segmentRecordSizeIncrement = RoundUp(sizeRange / this.segmentCount, 8);
                this.maxRecordSize = prevBinRecordSize + this.segmentRecordSizeIncrement * this.segmentCount;
                this.minRecordSize = prevBinRecordSize + this.segmentRecordSizeIncrement;
            }
            this.recordCount = this.segmentSize * this.segmentCount;

            // Overallocate the GCHandle by one cache line so we have room to offset the returned pointer to make it cache-aligned.
#if NET5_0_OR_GREATER
            this.recordsArray = GC.AllocateArray<FreeRecord>(recordCount + Constants.kCacheLineBytes / FreeRecord.StructSize, pinned: true);
            long p = (long)Unsafe.AsPointer(ref recordsArray[0]);
#else
            this.recordsArray = new FreeRecord[recordCount + Constants.kCacheLineBytes / FreeRecord.StructSize];
            this.recordsHandle = GCHandle.Alloc(this.recordsArray, GCHandleType.Pinned);
            long p = (long)this.recordsHandle.AddrOfPinnedObject();
#endif

            // Force the pointer to align to cache boundary.
            this.records = (FreeRecord*)RoundUp(p, Constants.kCacheLineBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetSegmentStart(int recordSize)
        {
            // recordSize and segmentSizeIncrement are rounded up to 8, unless IsFixedLength in which case segmentSizeIncrement is 1.
            // sizeOffset will be negative if we are searching the next-highest bin.
            var sizeOffset = recordSize - this.minRecordSize;
            if (sizeOffset < 0)
                sizeOffset = 0;
            var segmentIndex = sizeOffset / this.segmentRecordSizeIncrement;
            Debug.Assert(segmentIndex >= 0 && segmentIndex < this.segmentCount, $"Internal error: Segment index ({segmentIndex}) must be >= 0 && < segmentCount ({this.segmentCount})");
            return this.segmentSize * segmentIndex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private FreeRecord* GetRecord(int recordIndex) => records + (recordIndex >= recordCount ? recordIndex - recordCount : recordIndex);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd<Key, Value>(long address, int recordSize, FasterKV<Key, Value> fkv) 
            => TryAdd(address, recordSize, fkv, GetSegmentStart(recordSize));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd<Key, Value>(long address, int recordSize, FasterKV<Key, Value> fkv, int segmentStart)
        {
            for (var ii = 0; ii < this.recordCount; ++ii)
            {
                FreeRecord* record = GetRecord(segmentStart + ii);
                if (record->Set(address, recordSize, fkv))
                    return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake<Key, Value>(int recordSize, long minAddress, FasterKV<Key, Value> fkv, out long address) 
            => TryTake(recordSize, minAddress, fkv, oversize: false, out address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake<Key, Value>(int recordSize, long minAddress, FasterKV<Key, Value> fkv, bool oversize, out long address) 
            => (this.bestFitScanLimit == RevivificationBin.UseFirstFit)
                ? TryTakeFirstFit(recordSize, minAddress, fkv, oversize, out address)
                : TryTakeBestFit(recordSize, minAddress, fkv, oversize, out address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTakeFirstFit<Key, Value>(int recordSize, long minAddress, FasterKV<Key, Value> fkv, bool oversize, out long address)
        {
            var segmentStart = GetSegmentStart(recordSize);
            for (var ii = 0; ii < this.recordCount; ++ii)
            {
                FreeRecord* record = GetRecord(segmentStart + ii);
                if (oversize ? record->TryTakeOversize(recordSize, minAddress, fkv, out address) : record->TryTake(recordSize, minAddress, fkv, out address))
                    return true;
            }

            address = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTakeBestFit<Key, Value>(int recordSize, long minAddress, FasterKV<Key, Value> fkv, bool oversize, out long address)
        {
            // Retry as long as we find a candidate, but reduce the best fit scan limit each retry.
            int localBestFitScanLimit = this.bestFitScanLimit;
            var segmentStart = GetSegmentStart(recordSize);

            while (true)
            { 
                int bestFitSize = int.MaxValue;         // Comparison is "if record.Size < bestFitSize", hence initialized to int.MaxValue
                int bestFitIndex = -1;                  // Will be compared to >= 0 on exit from the best-fit scan loop
                int firstFitIndex = int.MaxValue;       // Subtracted from loop control var and tested for >= bestFitScanLimit; int.MaxValue produces a negative result

                FreeRecord* record;
                long prevFailedEpoch = long.MaxValue, lowestFailedEpoch = long.MaxValue, safeEpoch = fkv.epoch.SafeToReclaimEpoch;
                for (var ii = 0; ii < this.recordCount; ++ii)
                {
                    int saveBestSize = bestFitSize;
                    record = GetRecord(segmentStart + ii);

                    // For best-fit we must peek first without taking.
                    if (record->TryPeek(recordSize, fkv, oversize, minAddress, ref bestFitSize, ref lowestFailedEpoch))
                    { 
                        bestFitIndex = ii;      // Found exact match
                        break;
                    }

                    if (bestFitSize != saveBestSize)
                    {
                        bestFitIndex = ii;      // We have a better fit.
                        if (firstFitIndex == int.MaxValue)
                            firstFitIndex = ii;
                    }
                    if (ii - firstFitIndex >= localBestFitScanLimit)
                        break;
                }

                if (bestFitIndex < 0)
                {
                    if (lowestFailedEpoch < prevFailedEpoch && lowestFailedEpoch > safeEpoch)
                    {
                        var newSafeEpoch = fkv.epoch.ComputeNewSafeToReclaimEpoch();
                        if (newSafeEpoch > lowestFailedEpoch)
                        {
                            prevFailedEpoch = lowestFailedEpoch;
                            safeEpoch = newSafeEpoch;
                            localBestFitScanLimit = this.bestFitScanLimit;
                            continue;
                        }
                    }
                    address = 0;    // No candidate found
                    return false;
                }

                record = GetRecord(segmentStart + bestFitIndex);
                if (oversize ? record->TryTakeOversize(recordSize, minAddress, fkv, out address) : record->TryTake(recordSize, minAddress, fkv, out address))
                    return true;

                // We found a candidate but CAS failed or epoch was not safe. Reduce the best fit scan length and continue.
                localBestFitScanLimit /= 2;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ScanForBumpOrEmpty(long currentEpoch, long safeEpoch, int maxCountForBump, out int countNeedingBump, ref long lowestUnsafeEpoch)
        {
            var hasSafeRecords = false;
            countNeedingBump = 0;
            for (var ii = 0; ii < this.recordCount; ++ii)
            {
                FreeRecord localRecord = *(records + ii);
                if (localRecord.IsSet)
                {
                    if (localRecord.addedEpoch <= safeEpoch)
                        hasSafeRecords = true;
                    else if (localRecord.addedEpoch <= currentEpoch)
                    {
                        if (localRecord.addedEpoch == currentEpoch)
                        { 
                            if (++countNeedingBump >= maxCountForBump)
                                break;
                        }
                        else if (localRecord.addedEpoch < lowestUnsafeEpoch)
                        {
                            // This epoch is not safe but is below currentEpoch, so a bump of CurrentEpoch itself is not needed; track the lowest unsafe epoch.
                            lowestUnsafeEpoch = localRecord.addedEpoch;
                        }
                    }
                }
            }
            return hasSafeRecords;
        }

        public void Dispose()
        {
#if !NET5_0_OR_GREATER
            if (this.recordsHandle.IsAllocated)
                this.recordsHandle.Free();
#endif
        }
    }

    internal unsafe class FreeRecordPool<Key, Value> : IDisposable
    {
        internal readonly FasterKV<Key, Value> fkv;
        internal readonly FreeRecordBin[] bins;

        internal bool SearchNextHigherBin;
        internal bool IsFixedLength;
        internal bool HasSafeRecords;

        internal readonly int[] sizeIndexArray;
        private readonly int* sizeIndex;
        private readonly int numBins;

        internal readonly BumpEpochWorker<Key, Value> bumpEpochWorker;

#if !NET5_0_OR_GREATER
        private readonly GCHandle sizeIndexHandle;
#endif

        /// <inheritdoc/>
        public override string ToString() 
            => $"isFixedLen {IsFixedLength}, hasSafeRec {HasSafeRecords}, numBins {numBins}, searchNextBin {SearchNextHigherBin}, bumpEpochWorker: {bumpEpochWorker}";

        internal FreeRecordPool(FasterKV<Key, Value> fkv, RevivificationSettings settings, int fixedRecordLength)
        {
            this.fkv = fkv;
            this.IsFixedLength = fixedRecordLength > 0;
            settings.Verify(this.IsFixedLength);

            bumpEpochWorker = new(this);

            if (this.IsFixedLength)
            {
                this.numBins = 1;
                this.bins = new[] { new FreeRecordBin(ref settings.FreeListBins[0], fixedRecordLength, isFixedLength: true) };
                return;
            }

            // First create the "size index": a cache-aligned vector of int bin sizes. This way searching for the bin
            // for a record size will stay in a single cache line (unless there are more than 16 bins).
            var sizeIndexCount = RoundUp(settings.FreeListBins.Length * sizeof(int), Constants.kCacheLineBytes) / sizeof(int);

            // Overallocate the GCHandle by one cache line so we have room to offset the returned pointer to make it cache-aligned.
#if NET5_0_OR_GREATER
            this.sizeIndexArray = GC.AllocateArray<int>(sizeIndexCount + Constants.kCacheLineBytes / sizeof(int), pinned: true);
            long p = (long)Unsafe.AsPointer(ref sizeIndexArray[0]);
#else
            this.sizeIndexArray = new int[sizeIndexCount + Constants.kCacheLineBytes / sizeof(int)];
            this.sizeIndexHandle = GCHandle.Alloc(this.sizeIndexArray, GCHandleType.Pinned);
            long p = (long)sizeIndexHandle.AddrOfPinnedObject();
#endif

            // Force the pointer to align to cache boundary.
            long p2 = RoundUp(p, Constants.kCacheLineBytes);
            this.sizeIndex = (int*)p2;

            // Create the bins.
            List<FreeRecordBin> binList = new();
            int prevBinRecordSize = RevivificationBin.MinRecordSize - 8;      // The minimum record size increment is 8, so the first bin will set this to MinRecordSize or more
            for (var ii = 0; ii < settings.FreeListBins.Length; ++ii)
            {
                if (prevBinRecordSize >= settings.FreeListBins[ii].RecordSize)
                    continue;
                FreeRecordBin bin = new(ref settings.FreeListBins[ii], prevBinRecordSize);
                sizeIndex[binList.Count] = bin.maxRecordSize;
                binList.Add(bin);
                prevBinRecordSize = bin.maxRecordSize;
            }
            this.bins = binList.ToArray();
            this.numBins = this.bins.Length;
            this.SearchNextHigherBin = settings.SearchNextHigherBin;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetBinIndex(int size, out int binIndex)
        {
            Debug.Assert(!this.IsFixedLength, "Should only search bins if !IsFixedLength");

            // Sequential search in the sizeIndex for the requested size.
            for (var ii = 0; ii < this.numBins; ++ii)
            {
                if (sizeIndex[ii] >= size)
                {
                    binIndex = ii;
                    return true;
                }
            }
            binIndex = -1;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long address, int size)
        {
            int binIndex = 0;
            if (!this.IsFixedLength && !GetBinIndex(size, out binIndex))
                return false;
            if (!bins[binIndex].TryAdd(address, size, this.fkv))
                return false;
            
            bumpEpochWorker.Start(fromAdd: true);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(long logicalAddress, long physicalAddress, int allocatedSize)
        {
            if (logicalAddress < fkv.hlog.ReadOnlyAddress)
                return false;
            var recordInfo = fkv.hlog.GetInfo(physicalAddress);
            recordInfo.TrySeal();
            fkv.SetFreeRecordSize(physicalAddress, ref recordInfo, allocatedSize);
            return this.TryAdd(logicalAddress, allocatedSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake(int recordSize, long minAddress, out long address)
        {
            address = 0;

            bool result = false;
            if (this.IsFixedLength)
                result = bins[0].TryTake(recordSize, minAddress, this.fkv, out address);
            else if (GetBinIndex(recordSize, out int index))
            {
                // Try to Take from the inital bin and if unsuccessful, try the next-highest bin if requested.
                result = bins[index].TryTake(recordSize, minAddress, this.fkv, oversize: this.sizeIndex[index] > RevivificationBin.MaxInlineRecordSize, out address);
                if (!result && this.SearchNextHigherBin && index < this.numBins - 1)
                    result = bins[++index].TryTake(recordSize, minAddress, this.fkv, oversize: this.sizeIndex[index] > RevivificationBin.MaxInlineRecordSize, out address);
            }

            if (result)
                bumpEpochWorker.Start(fromAdd: false);
            return result;
        }

        internal bool ScanForBumpOrEmpty(int maxCountForBump, out int totalCountNeedingBump, ref long lowestUnsafeEpoch)
        {
            bool hasSafeRecords;
            for (var currentEpoch = this.fkv.epoch.CurrentEpoch ; ;)
            {
                hasSafeRecords = false;
                totalCountNeedingBump = 0;
                foreach (var bin in this.bins)
                {
                    if (this.bumpEpochWorker.YieldToAnotherThread())
                        return false;
                    if (currentEpoch != this.fkv.epoch.CurrentEpoch)
                        goto RedoEpoch; // Epoch was bumped since we started

                    bool binHasSafeRecords = bin.ScanForBumpOrEmpty(currentEpoch, fkv.epoch.SafeToReclaimEpoch, maxCountForBump, out int countNeedingBump, ref lowestUnsafeEpoch);
                    if (binHasSafeRecords)
                    {
                        if (!hasSafeRecords)    // Set this.HasRecords as soon as we know we have some, but only write to it once during this loop
                            this.HasSafeRecords = hasSafeRecords = true;
                    }

                    totalCountNeedingBump += countNeedingBump;
                    if (totalCountNeedingBump >= maxCountForBump)
                        break;
                }

                // Completed all bins
                break;

            RedoEpoch:
                currentEpoch = this.fkv.epoch.CurrentEpoch;
            }

            if (!hasSafeRecords)
                this.HasSafeRecords = false;
            return totalCountNeedingBump > 0;
        }

        public void Dispose()
        {
            foreach (var bin in this.bins)
                bin.Dispose();
            bumpEpochWorker.Dispose();
#if !NET5_0_OR_GREATER
            if (this.sizeIndexHandle.IsAllocated)
                this.sizeIndexHandle.Free();
#endif
        }
    }
}

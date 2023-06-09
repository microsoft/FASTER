// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
#if !NET5_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using System.Threading;
using static FASTER.core.Utility;

namespace FASTER.core
{
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
        private long word;

        // The epoch in which this record was freed; it cannot be reused until all threads are past that epoch.
        // It may also be one of the Epoch_* constants, for concurrency control.
        private long freedEpoch;

        internal const int StructSize = sizeof(long) * 2;
        #endregion Instance data

        private FreeRecord(long word) => this.word = word;

        public long Address
        {
            get => word & RecordInfo.kPreviousAddressMaskInWord;
            set
            {
                word &= ~RecordInfo.kPreviousAddressMaskInWord;
                word |= value & RecordInfo.kPreviousAddressMaskInWord;
            }
        }

        public long Size
        {
            get => (word & kSizeMaskInWord) >> kSizeShiftInWord;
            set
            {
                word &= ~kSizeMaskInWord;
                word |= value & RecordInfo.kPreviousAddressBits;
            }
        }

        internal bool IsSet => IsSetEpoch(this.freedEpoch);
        internal static bool IsSetEpoch(long epoch) => epoch > 0;

        private static bool IsLatchedEpoch(long epoch) => epoch == Epoch_Latched;

        internal bool Set(long freedEpoch, long address, long size)
        {
            // It may be set; that means we have wrapped around and found an entry that has a record that failed Take() before.
            // In that case, just overwrite it.
            var epoch = this.freedEpoch;
            if (IsLatchedEpoch(epoch) || Interlocked.CompareExchange(ref this.freedEpoch, Epoch_Latched, epoch) != epoch)
                return false;

            // Ignore oversize here--we check for that on Take()
            this.word = (size << kSizeShiftInWord) | (address & RecordInfo.kPreviousAddressMaskInWord);
            this.freedEpoch = freedEpoch;
            return true;
        }

        void SetEmpty()
        {
            this.word = 0;  // Must be first
            this.freedEpoch = Epoch_Empty;
        }

        internal bool Peek(long safeEpoch)
        {
            if (!IsSet)
                return false; // Enqueue has incremented 'write' but not yet written to it
            long epoch;
            while (IsLatchedEpoch(epoch = this.freedEpoch))
                Thread.Yield();
            return epoch <= safeEpoch;
        }

        internal bool Take(long safeEpoch, long size, long minAddress, out long address)
        {
            address = 0;

            // This is subject to extremely unlikely ABA--someone else could Set() a new address with the same epoch.
            // If so, it doesn't matter; we just return the address that's there.
            var epoch = this.freedEpoch;
            FreeRecord oldRecord = new(this.word);
            if (!IsSetEpoch(epoch) || epoch > safeEpoch || oldRecord.Size < size || oldRecord.Address < minAddress
                || Interlocked.CompareExchange(ref this.freedEpoch, Epoch_Latched, epoch) != epoch)
            {
                // Failed to CAS the epoch; leave 'word' unchanged
                return false;
            }

            // At this point we must unlatch. Recheck 'word' after the latch.
            if (this.Size >= size && this.Address >= minAddress)
            {
                address = this.Address;
                SetEmpty();
                return true;
            }

            SetEmpty();
            return false;
        }

        internal unsafe bool Take<Key, Value>(long safeEpoch, long size, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            address = 0;

            var epoch = this.freedEpoch;
            FreeRecord oldRecord = new(this.word);
            if (!IsSetEpoch(epoch) || epoch > safeEpoch || oldRecord.Address < minAddress
                || Interlocked.CompareExchange(ref this.freedEpoch, Epoch_Latched, epoch) != epoch)
            {
                // Failed to CAS the epoch; leave 'word' unchanged
                return false;
            }

            // At this point we must unlatch. Recheck 'word' after the latch.
            if (this.Address >= minAddress)
            { 
                // Because this is oversize, we need hlog to get the length out of the record's value (it won't fit in FreeRecord.kSizeBits)
                long physicalAddress = fkv.hlog.GetPhysicalAddress(this.Address);
                long recordSize = fkv.GetFreeRecordSize(physicalAddress, ref fkv.hlog.GetInfo(physicalAddress));
                if (recordSize >= size)
                {
                    address = this.Address;
                    SetEmpty();
                    return true;
                }
            }

            SetEmpty();
            return false;
        }
    }

    internal unsafe class FreeRecordBin : IDisposable
    {
        internal const int MinRecordSize = 16;      // RecordInfo + int key/value
        internal const int MinPartitionSize = 4;    // Make sure we have enough for the initial read/write pointers as well as enough to be useful

        private readonly FreeRecord[] recordsArray;
        internal readonly int partitionCount;
        internal readonly int partitionSize;
        internal readonly int numberOfPartitionsToTraverse;

        private readonly FreeRecord* records;
#if !NET5_0_OR_GREATER
        private readonly GCHandle recordsHandle;
#endif

        // Used by test also
        internal static int GetRecordCount(RevivificationBin binDef, out int partitionCount, out int partitionSize)
        {
            // Round up to align partitions to cache boundary.
            var partitionBytes = RoundUp(binDef.NumberOfRecordsPerPartition * FreeRecord.StructSize, Constants.kCacheLineBytes);
            partitionCount = binDef.NumberOfPartitions;
            partitionSize = partitionBytes / FreeRecord.StructSize;

            // FreeRecord.StructSize is a power of two
            return RoundUp(partitionBytes * partitionCount, FreeRecord.StructSize) / FreeRecord.StructSize;
        }

        internal FreeRecordBin(ref RevivificationBin binDef)
        {
            this.numberOfPartitionsToTraverse = binDef.NumberOfPartitionsToTraverse > 0 ? binDef.NumberOfPartitionsToTraverse : binDef.NumberOfPartitions;

            var recordCount = GetRecordCount(binDef, out this.partitionCount, out this.partitionSize);

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
            long p2 = RoundUp(p, Constants.kCacheLineBytes);
            this.records = (FreeRecord*)p2;

            // Initialize read and write pointers to 1, as we will store them as the first items in the partition.
            for (var ii = 0; ii < partitionCount; ++ii)
            {
                // Don't use GetReadPos/GetWritePos here; they assert the value is already > 0.
                int* partitionStart = (int*)GetPartitionStart(ii);
                *partitionStart = *(partitionStart + 1) = 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetInitialPartitionIndex()
        {
            // Taken from LightEpoch
            var threadId = Environment.CurrentManagedThreadId;
            var partitionId = (uint)Murmur3(threadId) % partitionCount;
            return (int)partitionId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Increment(int pointer)
        {
            var next = pointer + 1;
            return next == partitionSize ? 1 : next;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe FreeRecord* GetPartitionStart(int partitionIndex) => records + partitionSize * (partitionIndex % partitionCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref int GetReadPos(FreeRecord* partitionStart) => ref GetPos(partitionStart, 0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref int GetWritePos(FreeRecord* partitionStart) => ref GetPos(partitionStart, 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static ref int GetPos(FreeRecord* partitionStart, int offset)
        {
            ref int pos = ref Unsafe.AsRef<int>((int*)partitionStart + offset);
            Debug.Assert(pos > 0, "Read or write position must be > 0, because they start at 1");
            return ref pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue<Key, Value>(long address, int size, FasterKV<Key, Value> fkv) => Enqueue(address, size, fkv, GetInitialPartitionIndex());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue<Key, Value>(long address, int size, FasterKV<Key, Value> fkv, int initialPartitionIndex)
        {
            for (var iPart = 0; iPart < this.numberOfPartitionsToTraverse; ++iPart)
            {
                FreeRecord* partitionStart = GetPartitionStart(initialPartitionIndex + iPart);
                ref int read = ref GetReadPos(partitionStart);
                ref int write = ref GetWritePos(partitionStart);

                while (true)
                {
                    var currWrite = write;
                    var nextWrite = Increment(currWrite);
                    if (nextWrite == read)
                        break; // The partition is full
                    if (Interlocked.CompareExchange(ref write, nextWrite, currWrite) != currWrite)
                        continue;
                    if ((partitionStart + nextWrite)->Set(fkv.epoch.CurrentEpoch, address, size))
                        return true;
                }
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Dequeue<Key, Value>(int size, long minAddress, FasterKV<Key, Value> fkv, out long address) => Dequeue(size, minAddress, fkv, oversize: false, out address);

        public bool Dequeue<Key, Value>(int size, long minAddress, FasterKV<Key, Value> fkv, bool oversize, out long address)
        {
            // We skip over records that do not meet the minAddress requirement.
            // If this is the oversize bin, we may also skip over records that fail to satisfy the size requirement.
            var initialPartitionIndex = GetInitialPartitionIndex();

            for (var iPart = 0; iPart < this.numberOfPartitionsToTraverse; ++iPart)
            {
                FreeRecord* partitionStart = GetPartitionStart(initialPartitionIndex + iPart);
                ref int read = ref GetReadPos(partitionStart);
                ref int write = ref GetWritePos(partitionStart);

                while (true)
                {
                    var currRead = read;
                    if (currRead == write)
                        break; // The partition is empty
                    var nextRead = Increment(currRead);
                    FreeRecord* record = partitionStart + nextRead;

                    // First peek to see if the first epoch is reclaimable. These being circular buffers, we know the epoch will
                    // be monotonically non-decreasing; if the first one is too high, all the others in that partition will be.
                    // We do not want to skip over these.
                    long safeToReclaimEpoch = fkv.epoch.SafeToReclaimEpoch;
                    if (!record->Peek(safeToReclaimEpoch))
                        break; // Skip this partition

                    if (Interlocked.CompareExchange(ref read, nextRead, currRead) != currRead)
                        continue;

                    if (oversize ? record->Take(safeToReclaimEpoch, size, minAddress, fkv, out address) : record->Take(safeToReclaimEpoch, size, minAddress, out address))
                        return true;
                }
            }

            address = 0;
            return false;
        }

        public void Dispose()
        {
#if !NET5_0_OR_GREATER
            if (this.recordsHandle.IsAllocated)
                this.recordsHandle.Free();
#endif
        }
    }

    internal class FreeRecordPool
    {
        internal const int DefaultBumpIntervalMs = 1000;
    }

    internal unsafe class FreeRecordPool<Key, Value> : FreeRecordPool, IDisposable
    {
        private readonly FasterKV<Key, Value> fkv;
        internal readonly FreeRecordBin[] bins;

        internal bool IsFixedLength;
        internal bool SearchNextHighestBin;

        private long numberOfRecords = 0;
        internal bool HasRecords => numberOfRecords > 0;
        internal long NumberOfRecords => numberOfRecords;

        internal readonly int[] indexArray;
        private readonly int* index;
        private readonly int numBins;

        readonly System.Timers.Timer timer = new(DefaultBumpIntervalMs);
        long lastFreedEpoch = 0;        // 0 is invalid epoch
        long NumberOfRecordsAtLastFreedEpoch;

#if !NET5_0_OR_GREATER
        private readonly GCHandle indexHandle;
#endif

        internal FreeRecordPool(FasterKV<Key, Value> fkv, RevivificationSettings settings, int fixedRecordLength)
        {
            this.fkv = fkv;
            this.IsFixedLength = fixedRecordLength > 0;
            settings.Verify(this.IsFixedLength);

            if (this.IsFixedLength)
            {
                this.numBins = 1;
                this.bins = new[] { new FreeRecordBin(ref settings.FreeListBins[0]) };
                return;
            }

            // First create the "size index": a cache-aligned vector of int bin sizes. This way searching for the bin
            // for a record size will stay in a single cache line (unless there are more than 16 bins).
            var indexCount = RoundUp(settings.FreeListBins.Length * sizeof(int), Constants.kCacheLineBytes) / sizeof(int);

            // Overallocate the GCHandle by one cache line so we have room to offset the returned pointer to make it cache-aligned.
#if NET5_0_OR_GREATER
            this.indexArray = GC.AllocateArray<int>(indexCount + Constants.kCacheLineBytes / sizeof(int), pinned: true);
            long p = (long)Unsafe.AsPointer(ref indexArray[0]);
#else
            this.indexArray = new int[indexCount + Constants.kCacheLineBytes / sizeof(int)];
            this.indexHandle = GCHandle.Alloc(this.indexArray, GCHandleType.Pinned);
            long p = (long)indexHandle.AddrOfPinnedObject();
#endif

            // Force the pointer to align to cache boundary.
            long p2 = RoundUp(p, Constants.kCacheLineBytes);
            this.index = (int*)p2;

            // Initialize the size index.
            this.numBins = settings.FreeListBins.Length;
            for (var ii = 0; ii < this.numBins; ++ii)
                index[ii] = settings.FreeListBins[ii].RecordSize;

            // Create the bins.
            List<FreeRecordBin> binList = new();
            for (var ii = 0; ii < this.numBins; ++ii)
                binList.Add(new FreeRecordBin(ref settings.FreeListBins[ii]));
            this.bins = binList.ToArray();
            this.SearchNextHighestBin = settings.SearchNextHighestBin;

            // Timer will be started manually each time needed.
            timer.AutoReset = false;
            timer.Elapsed += (object source, System.Timers.ElapsedEventArgs e) =>
            {
                fkv.BumpCurrentEpoch();
                this.NumberOfRecordsAtLastFreedEpoch = this.NumberOfRecords;
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetBinIndex(int size, out int binIndex)
        {
            Debug.Assert(!this.IsFixedLength, "Should only search bins if !IsFixedLength");

            // Sequential search in the bin for now. TODO: Profile; consider a binary search if we have enough bins.
            for (var ii = 0; ii < this.numBins; ++ii)
            {
                if (index[ii] >= size)
                {
                    binIndex = ii;
                    return true;
                }
            }
            binIndex = -1;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size)
        {
            if (this.IsFixedLength)
            {
                if (!bins[0].Enqueue(address, size, this.fkv))
                    return false;
            }
            else
            { 
                if (!GetBinIndex(size, out int index) || !bins[index].Enqueue(address, size, this.fkv))
                    return false;
            }

            var numRec = Interlocked.Increment(ref this.numberOfRecords);

            if (MonotonicUpdate(ref lastFreedEpoch, fkv.epoch.CurrentEpoch, out _))
            {
                // We just enqueued the first record of a new epoch; start the timer.
                this.NumberOfRecordsAtLastFreedEpoch = numRec;
                this.timer.Interval = DefaultBumpIntervalMs;
                this.timer.Enabled = true;
            }
            else
            {
                // See if we have enough records to shorten the interval. Default timer resolution is about 15 ms.
                var elapsedNumRec = this.numberOfRecords - this.NumberOfRecordsAtLastFreedEpoch;
                if (elapsedNumRec > 20 && elapsedNumRec % 10 == 0)
                {
                    // This could have a race that overwrites a lower value with a greater one, but it's not worth more
                    // interlocks to avoid it before we get to 1 (0 is invalid) and stop setting it.
                    var interval = elapsedNumRec >= 100 ? 1 : (100 - elapsedNumRec);
                    if (timer.Interval > interval)
                        timer.Interval = interval;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Dequeue(int size, long minAddress, out long address)
        {
            address = 0;

            if (this.IsFixedLength)
            { 
                // We only have one bin, so pass a minimal record size to make it effectively ignored.
                if (!bins[0].Dequeue(RecordInfo.GetLength(), minAddress, this.fkv, out address))
                    return false;
            }
            else
            { 
                if (!GetBinIndex(size, out int index))
                    return false;

                // If the bin's max size can't be stored "inline" within the FreeRecord's size storage, then it is oversize and uses the fkv to get the record's length.
                var result = bins[index].Dequeue(size, minAddress, this.fkv, oversize: this.index[index] > RevivificationBin.MaxInlineRecordSize, out address);

                // If unsuccessful, try the next-highest bin if requested.
                if (!result && this.SearchNextHighestBin && index < this.numBins - 1)
                { 
                    ++index;
                    result = bins[index].Dequeue(size, minAddress, this.fkv, oversize: this.index[index] > RevivificationBin.MaxInlineRecordSize, out address);
                }

                if (!result)
                    return false;
            }

            Interlocked.Decrement(ref this.numberOfRecords);
            return true;
        }

        public void Dispose()
        {
            foreach (var bin in this.bins)
                bin.Dispose();
            timer.Dispose();
#if !NET5_0_OR_GREATER
            if (this.indexHandle.IsAllocated)
                this.indexHandle.Free();
#endif
        }
    }
}

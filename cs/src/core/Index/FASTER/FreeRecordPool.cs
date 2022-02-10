// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    internal struct FreeRecord
    {
        const int kSizeBits = 64 - RecordInfo.kPreviousAddressBits;
        const int kSizeShiftInWord = RecordInfo.kPreviousAddressBits;

        internal const int kMaxSize = 1 << kSizeBits;
        const long kSizeMask = kMaxSize - 1;
        const long kSizeMaskInWord = kSizeMask << kSizeShiftInWord;

        private long word;
        private const long emptyWord = 0;

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

        private FreeRecord(long word) => this.word = word;

        internal bool Set(long address, long size)
        {
            // Don't replace a higher address with a lower one.
            FreeRecord old_record = new(this.word);
            while (old_record.Address < address)
            {
                long newWord = (size << kSizeShiftInWord) | (address & RecordInfo.kPreviousAddressMaskInWord);
                if (Interlocked.CompareExchange(ref word, newWord, old_record.word) == old_record.word)
                    return true;
                old_record = new(this.word);
            }
            return false;
        }

        internal bool Take(long size, long minAddress, out long address)
        {
            FreeRecord old_record = new(this.word);
            if (old_record.Size >= size && old_record.Address >= minAddress)
            {
                if (Interlocked.CompareExchange(ref word, emptyWord, old_record.word) == old_record.word)
                {
                    address = old_record.Address;
                    return true;
                }
            }
            address = emptyWord;
            return false;
        }

        internal unsafe bool Take<Key, Value>(long size, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            FreeRecord old_record = new(this.word);
            if (old_record.Address >= minAddress)
            {
                // Because this is oversize, we need hlog to get the length out of the record's value (it won't fit in FreeRecord.kSizeBits)
                long physicalAddress = fkv.hlog.GetPhysicalAddress(old_record.Address);
                long recordSize = fkv.GetDeletedRecordLength(physicalAddress, ref fkv.hlog.GetInfo(physicalAddress));

                if (recordSize >= size)
                {
                    if (Interlocked.CompareExchange(ref word, emptyWord, old_record.word) == old_record.word)
                    {
                        address = old_record.Address;
                        return true;
                    }
                }
            }
            address = emptyWord;
            return false;
        }

        internal bool IsSet => word != emptyWord;
    }

    internal unsafe class FreeRecordBin
    {
        private readonly FreeRecord[] recordsArray;
        internal readonly int maxSize;
        private readonly int recordCount;
        private readonly int partitionCount;
        private readonly int partitionSize;

        private readonly GCHandle handle;
        private readonly FreeRecord* records;

        // Used by test also
        internal static void GetPartitionSizes(int maxRecs, out int partitionCount, out int partitionSize, out int recordCount)
        {
            partitionCount = Environment.ProcessorCount / 2;

            if (maxRecs < partitionCount)
                partitionCount = 1;

            // Round up to align partitions to cache boundary.
            var pad = Constants.kCacheLineBytes / sizeof(long) - 1;
            partitionSize = ((((maxRecs + pad) / partitionCount) * sizeof(long) + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1)) / sizeof(long);

            // Overallocate to allow space for cache-aligned start
            recordCount = partitionSize * partitionCount;
        }

        internal FreeRecordBin(int maxRecs, int maxSize)
        {
            this.maxSize = maxSize;

            GetPartitionSizes(maxRecs, out this.partitionCount, out this.partitionSize, out this.recordCount);

            this.recordsArray = new FreeRecord[this.recordCount + Constants.kCacheLineBytes / sizeof(long)];

            // Allocate the GCHandle so we can create a cache-aligned pointer.
            handle = GCHandle.Alloc(this.recordsArray, GCHandleType.Pinned);
            long p = (long)handle.AddrOfPinnedObject();

            // Force the pointer to align to cache boundary.
            long p2 = (p + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1);
            this.records = (FreeRecord*)p2;

            // Initialize head and tail to 1, as we will store head and tail as the first items in the partition.
            for (var ii = 0; ii < partitionCount; ++ii)
            {
                int* head = (int*)(records + ii * partitionSize);
                *head = 1;
                *(head + 1) = 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetInitialPartitionIndex()
        {
            // Taken from LightEpoch
            var threadId = Environment.OSVersion.Platform == PlatformID.Win32NT ? (int)Native32.GetCurrentThreadId() : Thread.CurrentThread.ManagedThreadId;
            var partitionId = Utility.Murmur3(threadId) % partitionCount;
            return partitionId >= 0 ? partitionId : -partitionId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Increment(int pointer)
        {
            var next = pointer + 1;
            if (next == partitionSize)
                next = 1;   // The first "FreeRecord" in the partition is the head/tail popinters
            return next;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe FreeRecord* GetPartitionStart(int partitionIndex) => records + partitionSize * (partitionIndex % partitionCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size)
        {
            var initialPartitionIndex = GetInitialPartitionIndex();

            for (var iPart = 0; iPart < this.partitionCount; ++iPart)
            {
                FreeRecord* partitionStart = GetPartitionStart(initialPartitionIndex + iPart);
                ref int head = ref Unsafe.AsRef<int>((int*)partitionStart);
                ref int tail = ref Unsafe.AsRef<int>((int*)partitionStart + 1);
                Debug.Assert(head > 0);
                Debug.Assert(tail > 0);

                // Start at 1 because head/tail are in the first element
                for (var iRec = 1; iRec < this.recordCount; ++iRec)
                {
                    var prev = tail;
                    var next = Increment(prev);
                    if (next == head)
                        break; // The bin is full
                    if (Interlocked.CompareExchange(ref tail, next, prev) != prev)
                        continue;
                    if ((partitionStart + prev)->Set(address, size))
                        return true;
                }
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Dequeue<Key, Value>(int size, long minAddress, out long address) 
            => Dequeue<Key, Value>(size, minAddress, null, out address);

        public bool Dequeue<Key, Value>(int size, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            // We skip over records that do not meet the minAddress requirement.
            // If this is the oversize bin, we may also skip over records that fail to satisfy the size requirement.
            var initialPartitionIndex = GetInitialPartitionIndex();

            for (var iPart = 0; iPart < this.partitionCount; ++iPart)
            {
                FreeRecord* partitionStart = GetPartitionStart(initialPartitionIndex + iPart);
                ref int head = ref Unsafe.AsRef<int>((int*)partitionStart);
                ref int tail = ref Unsafe.AsRef<int>((int*)partitionStart + 1);
                Debug.Assert(head > 0);
                Debug.Assert(tail > 0);

                // Start at 1 because head/tail are in the first element
                for (var iRec = 1; iRec < this.partitionSize; ++iRec)
                {
                    var prev = head;
                    if (prev == tail)
                        break; // The bin is empty
                    var next = Increment(prev);
                    if (Interlocked.CompareExchange(ref head, next, prev) != prev)
                        continue;

                    var success = fkv is null
                        ? (partitionStart + prev)->Take(size, minAddress, out address)
                        : (partitionStart + prev)->Take(size, minAddress, fkv, out address);
                    if (success)
                        return true;
                }
            }

            address = 0;
            return false;
        }

        internal void Dispose()
        {
            if (this.recordsArray is not null)
                handle.Free();
        }
    }

    internal class FreeRecordPool
    {
        internal const int InitialBinSize = 16;  // RecordInfo + int key/value
        internal readonly FreeRecordBin[] bins;
        internal readonly FreeRecordBin overSizeBin;

        internal bool IsFixedLength => overSizeBin is null;

        private long numberOfRecords = 0;
        internal bool HasRecords => numberOfRecords > 0;
        internal long NumberOfRecords => numberOfRecords;

        internal FreeRecordPool(int maxRecsPerBin, int fixedRecordLength)
        {
            if (maxRecsPerBin <= 0)
                throw new FasterException($"Invalid number of records per FreeRecordBin {maxRecsPerBin}; must be > 0");

            if (fixedRecordLength > 0)
            {
                this.bins = new[] { new FreeRecordBin(maxRecsPerBin, fixedRecordLength) };
                return;
            }

            List<FreeRecordBin> binList = new();
            for (var size = InitialBinSize; size <= FreeRecord.kMaxSize; size *= 2)
                binList.Add(new FreeRecordBin(maxRecsPerBin, size));
            this.bins = binList.ToArray();

            this.overSizeBin = new FreeRecordBin(maxRecsPerBin, int.MaxValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetEnqueueBinIndex(int size, out int index)
        {
            if (IsFixedLength)
            {
                index = 0;
                return true;
            }

            // Enqueue into the highest bin whose maxSize is <= size;
            var binSize = InitialBinSize / 2;
            for (var r = 0; r < bins.Length; ++r)
            {
                binSize <<= 1;
                if (size > binSize)
                    continue;
                index = r;
                return true;
            }

            // Oversize
            index = -1;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetDequeueBinIndex(int size, out int index)
        {
            // Modified from SectorAlignedBufferPool.Position()
            if (IsFixedLength)
            {
                index = 0;
                return true;
            }

            // Stored records' lengths are *between* the lowest and highest sizes of a bin, *not* the highest size of the bin,
            // so we have to retrieve from the next-highest bin; e.g. 48 will come from the [64-127] bin because the [32-63]
            // bin might not have anything larger than 42, so we can't satisfy the request.
            // We only store records of size >= InitialBinSize, so the first bin is always a fit.
            // The second bin is only retrieved from as a Dequeue overflow bin from the first (see Dequeue), all sizes that
            // are less than the first bin's max size will stay in the first bin.
            if (size <= InitialBinSize)
            {
                index = 0;
                return true;
            }
            
            var binSize = InitialBinSize / 2;

            // r will be lg(v) - lg(InitialBinSize / 2)
            for (int r = 0; r < bins.Length - 1; ++r)        // unroll for more speed...
            {
                binSize <<= 1;
                if (size > binSize)
                    continue;

                index = r + 1;
                return true;
            }

            // Oversize
            index = -1;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size)
        {
            var result = GetEnqueueBinIndex(size, out int index)
                ? bins[index].Enqueue(address, size)
                : overSizeBin.Enqueue(address, size);

            // If unsuccessful, try the next-highest bin if possible.
            if (!result && index < bins.Length - 1)
                result = bins[index + 1].Enqueue(address, size);

            if (result)
                Interlocked.Increment(ref this.numberOfRecords);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Dequeue<Key, Value>(int size, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            var result = GetDequeueBinIndex(size, out int index)
                ? bins[index].Dequeue<Key, Value>(size, minAddress, out address)
                : overSizeBin.Dequeue(size, minAddress, fkv, out address);

            // If unsuccessful, try the next-highest bin if possible.
            if (!result && index < bins.Length - 1)
                result = bins[index + 1].Dequeue<Key, Value>(size, minAddress, out address);

            if (result)
                Interlocked.Decrement(ref this.numberOfRecords);
            return result;
        }

        internal void Dispose()
        {
            foreach (var bin in this.bins)
                bin.Dispose();
        }
    }
}

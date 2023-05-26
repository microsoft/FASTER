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

        // This is the empty word we replace the current word with on Reads.
        private const long emptyWord = 0;

        #region Start of instance data
        // 'word' contains the reclaimable logicalAddress and the size of the record at that address.
        private long word;

        internal const int StructSize = sizeof(long);
        #endregion Start of instance data

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
            // If it's set, that means someone else won a race to set it (we will only be here on the first lap
            // of the circular buffer, or on a subsequent lap after Read has read/cleared the record).
            if (this.IsSet)
                return false;

            long newWord = (size << kSizeShiftInWord) | (address & RecordInfo.kPreviousAddressMaskInWord);
            return Interlocked.CompareExchange(ref word, newWord, emptyWord) == emptyWord;
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
                Debug.Assert(this.word == emptyWord, "CAS should only fail if another thread did Take() first, which would leave word==emptyWord");
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
                long recordSize = fkv.GetFreeRecordSize(physicalAddress, ref fkv.hlog.GetInfo(physicalAddress));

                if (recordSize >= size)
                {
                    if (Interlocked.CompareExchange(ref word, emptyWord, old_record.word) == old_record.word)
                    {
                        address = old_record.Address;
                        return true;
                    }
                    Debug.Assert(this.word == emptyWord, "CAS should only fail if another thread did Take() first, which would leave word==emptyWord");
                }
            }

            address = emptyWord;
            return false;
        }

        internal bool IsSet => word != emptyWord;
    }

    internal unsafe class FreeRecordBin : IDisposable
    {
        private readonly FreeRecord[] recordsArray;
        internal readonly int maxSize;
        private readonly int recordCount;
        internal readonly int partitionCount;
        internal readonly int partitionSize;

        private readonly FreeRecord* records;
#if !NET5_0_OR_GREATER
        private readonly GCHandle handle;
#endif

        // Used by test also
        internal static void GetPartitionSizes(int maxRecs, out int partitionCount, out int partitionSize, out int recordCount)
        {
            partitionCount = Environment.ProcessorCount / 2;

            // If we don't have enough records to make partitions worthwhile, don't use them
            if (maxRecs <= partitionCount * 4)
                partitionCount = 1;

            // Round up to align partitions to cache boundary.
            partitionSize = (((maxRecs / partitionCount) * FreeRecord.StructSize + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1)) / FreeRecord.StructSize;

            // Overallocate to allow space for cache-aligned start
            recordCount = partitionSize * partitionCount;
        }

        internal FreeRecordBin(int maxRecs, int maxSize)
        {
            this.maxSize = maxSize;

            GetPartitionSizes(maxRecs, out this.partitionCount, out this.partitionSize, out this.recordCount);

            // Allocate the GCHandle so we can create a cache-aligned pointer.
#if NET5_0_OR_GREATER
            this.recordsArray = GC.AllocateArray<FreeRecord>(this.recordCount + Constants.kCacheLineBytes / FreeRecord.StructSize, pinned: true);
            long p = (long)Unsafe.AsPointer(ref recordsArray[0]);
#else
            this.recordsArray = new FreeRecord[this.recordCount + Constants.kCacheLineBytes / FreeRecord.StructSize];
            handle = GCHandle.Alloc(this.recordsArray, GCHandleType.Pinned);
            long p = (long)handle.AddrOfPinnedObject();
#endif

            // Force the pointer to align to cache boundary.
            long p2 = (p + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1);
            this.records = (FreeRecord*)p2;

            // Initialize read and write pointers to 1, as we will store them as the first items in the partition.
            for (var ii = 0; ii < partitionCount; ++ii)
            {
                // Don't use GetReadPos/GetWritePos here; they assert the value is already > 0.
                int* partitionStart = (int*)GetPartitionStart(ii);
                *partitionStart = 1;
                *(partitionStart + 1) = 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetInitialPartitionIndex()
        {
            // Taken from LightEpoch
            var threadId = Environment.CurrentManagedThreadId;
            var partitionId = (uint)Utility.Murmur3(threadId) % partitionCount;
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
            Debug.Assert(pos > 0);
            return ref pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size) => Enqueue(address, size, GetInitialPartitionIndex());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size, int initialPartitionIndex)
        {
            for (var iPart = 0; iPart < this.partitionCount; ++iPart)
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
                    if ((partitionStart + nextWrite)->Set(address, size))
                        return true;
                }
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Dequeue<Key, Value>(int size, long minAddress, out long address) => Dequeue<Key, Value>(size, minAddress, null, out address);

        public bool Dequeue<Key, Value>(int size, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            // We skip over records that do not meet the minAddress requirement.
            // If this is the oversize bin, we may also skip over records that fail to satisfy the size requirement.
            var initialPartitionIndex = GetInitialPartitionIndex();

            for (var iPart = 0; iPart < this.partitionCount; ++iPart)
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
                    if (nextRead == write && !(partitionStart + nextRead)->IsSet)
                        break; // Enqueue has incremented 'write' but not yet written to it.
                    if (Interlocked.CompareExchange(ref read, nextRead, currRead) != currRead)
                        continue;

                    FreeRecord* record = partitionStart + nextRead;
                    if (fkv is null ? record->Take(size, minAddress, out address) : record->Take(size, minAddress, fkv, out address))
                        return true;
                }
            }

            address = 0;
            return false;
        }

        public void Dispose()
        {
#if !NET5_0_OR_GREATER
            if (handle.IsAllocated)
                handle.Free();
#endif
        }
    }

    internal class FreeRecordPool : IDisposable
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

        public void Dispose()
        {
            foreach (var bin in this.bins)
                bin.Dispose();
        }
    }
}

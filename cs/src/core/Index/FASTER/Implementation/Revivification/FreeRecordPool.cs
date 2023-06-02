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
        internal const int MinimumPartitionSize = 4;    // Make sure we have enough for the initial read/write pointers as well as enough to be useful

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
            Debug.Assert(pos > 0);
            return ref pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size) => Enqueue(address, size, GetInitialPartitionIndex());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Enqueue(long address, int size, int initialPartitionIndex)
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
            if (this.recordsHandle.IsAllocated)
                this.recordsHandle.Free();
#endif
        }
    }

    internal unsafe class FreeRecordPool : IDisposable
    {
        internal const int MinRecordSize = 16;  // RecordInfo + int key/value

        internal readonly FreeRecordBin[] bins;

        internal bool IsFixedLength;
        internal bool SearchNextHighestBin;

        private long numberOfRecords = 0;
        internal bool HasRecords => numberOfRecords > 0;
        internal long NumberOfRecords => numberOfRecords;

        internal readonly int[] indexArray;
        private readonly int* index;
        private readonly int numBins;
#if !NET5_0_OR_GREATER
        private readonly GCHandle indexHandle;
#endif

        internal FreeRecordPool(RevivificationSettings settings, int fixedRecordLength)
        {
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
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetBinIndex(int size, out int binIndex)
        {
            if (this.IsFixedLength)
            { 
                binIndex = 0;
                return true;
            }

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
            if (!GetBinIndex(size, out int index))
                return false;
            if (!bins[index].Enqueue(address, size))
                return false;
            Interlocked.Increment(ref this.numberOfRecords);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Dequeue<Key, Value>(int size, long minAddress, FasterKV<Key, Value> fkv, out long address)
        {
            address = 0;

            if (this.IsFixedLength)
            { 
                if (!bins[0].Dequeue<Key, Value>(size, minAddress, out address))
                    return false;
            }
            else
            { 
                if (!GetBinIndex(size, out int index))
                    return false;

                // If the bin's max size can't be stored "inline" within the FreeRecord's size storage, then we must pass
                // the fkv to retrieve the record length.
                var result = (this.index[index] < RevivificationBin.MaxInlineRecordSize)
                    ? bins[index].Dequeue<Key, Value>(size, minAddress, out address)
                    : bins[index].Dequeue(size, minAddress, fkv, out address);

                // If unsuccessful, try the next-highest bin if requested.
                if (!result && this.SearchNextHighestBin && index < this.numBins - 1)
                { 
                    ++index;
                    result = (this.index[index] < RevivificationBin.MaxInlineRecordSize)
                        ? bins[index].Dequeue<Key, Value>(size, minAddress, out address)
                        : bins[index].Dequeue(size, minAddress, fkv, out address);
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
#if !NET5_0_OR_GREATER
            if (this.indexHandle.IsAllocated)
                this.indexHandle.Free();
#endif
        }
    }
}

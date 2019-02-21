// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    internal static class Constants
    {
        /// Size of cache line in bytes
        public const int kCacheLineBytes = 64;

        public const bool kFineGrainedHandoverRecord = false;

        public const bool kFineGrainedHandoverBucket = true;

        /// Number of entries per bucket (assuming 8-byte entries to fill a cacheline)
        /// Number of bits per bucket (assuming 8-byte entries to fill a cacheline)
        public const int kBitsPerBucket = 3;

        public const int kEntriesPerBucket = 1 << kBitsPerBucket;

        // Position of fields in hash-table entry
        public const int kTentativeBitShift = 63;

        public const long kTentativeBitMask = (1L << kTentativeBitShift);

        public const int kPendingBitShift = 62;

        public const long kPendingBitMask = (1L << kPendingBitShift);

        public const int kTagSize = 14;

        public const int kTagShift = 62 - kTagSize;

        public const long kTagMask = (1L << kTagSize) - 1;

        public const long kTagPositionMask = (kTagMask << kTagShift);

        public const long kAddressMask = (1L << 48) - 1;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagSize;


        /// Invalid entry value
        public const int kInvalidEntrySlot = kEntriesPerBucket;

        /// Location of the special bucket entry
        public const long kOverflowBucketIndex = kEntriesPerBucket - 1;

        /// Invalid value in the hash table
        public const long kInvalidEntry = 0;

        /// Number of times to retry a compare-and-swap before failure
        public const long kRetryThreshold = 1000000;

        /// Number of merge/split chunks.
        public const int kNumMergeChunkBits = 8;
        public const int kNumMergeChunks = 1 << kNumMergeChunkBits;

        // Size of chunks for garbage collection
        public const int kSizeofChunkBits = 14;
        public const int kSizeofChunk = 1 << 14;

        public const long kInvalidAddress = 0;
        public const long kTempInvalidAddress = 1;
        public const int kFirstValidAddress = 64;
    }

    [StructLayout(LayoutKind.Explicit, Size = Constants.kEntriesPerBucket * 8)]
    internal unsafe struct HashBucket
    {

        public const long kPinConstant = (1L << 48);

        public const long kExclusiveLatchBitMask = (1L << 63);

        [FieldOffset(0)]
        public fixed long bucket_entries[Constants.kEntriesPerBucket];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireSharedLatch(HashBucket* bucket)
        {
            return Interlocked.Add(ref bucket->bucket_entries[Constants.kOverflowBucketIndex],
                                   kPinConstant) > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseSharedLatch(HashBucket* bucket)
        {
            Interlocked.Add(ref bucket->bucket_entries[Constants.kOverflowBucketIndex],
                            -kPinConstant);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireExclusiveLatch(HashBucket* bucket)
        {
            long expected_word = bucket->bucket_entries[Constants.kOverflowBucketIndex];
            if ((expected_word & ~Constants.kAddressMask) == 0)
            {
                long desired_word = expected_word | kExclusiveLatchBitMask;
                var found_word = Interlocked.CompareExchange(
                                    ref bucket->bucket_entries[Constants.kOverflowBucketIndex],
                                    desired_word,
                                    expected_word);
                return found_word == expected_word;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseExclusiveLatch(HashBucket* bucket)
        {
            long expected_word = bucket->bucket_entries[Constants.kOverflowBucketIndex];
            long desired_word = expected_word & Constants.kAddressMask;
            var found_word = Interlocked.Exchange(
                                ref bucket->bucket_entries[Constants.kOverflowBucketIndex],
                                desired_word);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool NoSharedLatches(HashBucket* bucket)
        {
            long word = bucket->bucket_entries[Constants.kOverflowBucketIndex];
            return (word & ~Constants.kAddressMask) == 0;
        }
    }

    // Long value layout: [1-bit tentative][15-bit TAG][48-bit address]
    // Physical little endian memory layout: [48-bit address][15-bit TAG][1-bit tentative]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct HashBucketEntry
    {
        [FieldOffset(0)]
        public long word;

        public long Address
        {
            get
            {
                return word & Constants.kAddressMask;
            }

            set
            {
                word &= ~Constants.kAddressMask;
                word |= (value & Constants.kAddressMask);
            }
        }

        public ushort Tag
        {
            get
            {
                return (ushort)((word & Constants.kTagPositionMask) >> Constants.kTagShift);
            }

            set
            {
                word &= ~Constants.kTagPositionMask;
                word |= ((long)value << Constants.kTagShift);
            }
        }

        public bool Pending
        {
            get
            {
                return (word & Constants.kPendingBitMask) != 0;
            }

            set
            {
                if (value)
                {
                    word |= Constants.kPendingBitMask;
                }
                else
                {
                    word &= ~Constants.kPendingBitMask;
                }
            }
        }

        public bool Tentative
        {
            get
            {
                return (word & Constants.kTentativeBitMask) != 0;
            }

            set
            {
                if (value)
                {
                    word |= Constants.kTentativeBitMask;
                }
                else
                {
                    word &= ~Constants.kTentativeBitMask;
                }
            }
        }
    }

    internal unsafe struct InternalHashTable
    {
        public long size;
        public long size_mask;
        public int size_bits;
        public HashBucket[] tableRaw;
        public GCHandle tableHandle;
        public HashBucket* tableAligned;
    }

    public unsafe partial class FasterBase
    {
        // Initial size of the table
        internal long minTableSize = 16;

        // Allocator for the hash buckets
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>();

        // An array of size two, that contains the old and new versions of the hash-table
        internal InternalHashTable[] state = new InternalHashTable[2];

        // Array used to denote if a specific chunk is merged or not
        internal long[] splitStatus;

        // Used as an atomic counter to check if resizing is complete
        internal long numPendingChunksToBeSplit;

        // Epoch set for resizing
        internal int resizeEpoch;

        internal LightEpoch epoch;

        internal ResizeInfo resizeInfo;

        internal long currentSize;

        /// <summary>
        /// Constructor
        /// </summary>
        public FasterBase()
        {
            epoch = LightEpoch.Instance;
        }

        internal Status Free()
        {
            Free(0);
            Free(1);
            overflowBucketsAllocator.Dispose();
            return Status.OK;
        }

        private Status Free(int version)
        {
            if (state[version].tableHandle.IsAllocated)
                state[version].tableHandle.Free();

            state[version].tableRaw = null;
            state[version].tableAligned = null;
            return Status.OK;
        }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        public void Initialize(long size, int sector_size)
        {
            if (!Utility.IsPowerOfTwo(size))
            {
                throw new ArgumentException("Size {0} is not a power of 2");
            }
            if (!Utility.Is32Bit(size))
            {
                throw new ArgumentException("Size {0} is not 32-bit");
            }

            minTableSize = size;
            resizeInfo = default(ResizeInfo);
            resizeInfo.status = ResizeOperationStatus.DONE;
            resizeInfo.version = 0;
            Initialize(resizeInfo.version, size, sector_size);
        }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="version"></param>
        /// <param name="size"></param>
        /// <param name="sector_size"></param>
        protected void Initialize(int version, long size, int sector_size)
        {
            long size_bytes = size * sizeof(HashBucket);
            long aligned_size_bytes = sector_size +
                ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));

            //Over-allocate and align the table to the cacheline
            state[version].size = size;
            state[version].size_mask = size - 1;
            state[version].size_bits = Utility.GetLogBase2((int)size);

            state[version].tableRaw = new HashBucket[aligned_size_bytes / Constants.kCacheLineBytes];
            state[version].tableHandle = GCHandle.Alloc(state[version].tableRaw, GCHandleType.Pinned);
            long sectorAlignedPointer = ((long)state[version].tableHandle.AddrOfPinnedObject() + (sector_size - 1)) & ~(sector_size - 1);
            state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
        }

        /// <summary>
        /// A helper function that is used to find the slot corresponding to a
        /// key in the specified version of the hash table
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="tag"></param>
        /// <param name="bucket"></param>
        /// <param name="slot"></param>
        /// <param name="entry"></param>
        /// <returns>true if such a slot exists, false otherwise</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool FindTag(long hash, ushort tag, ref HashBucket* bucket, ref int slot, ref HashBucketEntry entry)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);
            var version = resizeInfo.version;
            var masked_entry_word = hash & state[version].size_mask;
            bucket = state[version].tableAligned + masked_entry_word;
            slot = Constants.kInvalidEntrySlot;

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved
                // for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                    {
                        continue;
                    }

                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        slot = index;

                        // If (final key, return immediately)
                        //if ((entry.tag & ~Constants.kTagMask) == 0) -- Guna: Is this correct?
                        if (!entry.Tentative)
                            return true;
                    }
                }

                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                // Go to next bucket in the chain


                if (target_entry_word == 0)
                {
                    entry = default(HashBucketEntry);
                    return false;
                }
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FindOrCreateTag(long hash, ushort tag, ref HashBucket* bucket, ref int slot, ref HashBucketEntry entry, long BeginAddress)
        {
            var version = resizeInfo.version;
            var masked_entry_word = hash & state[version].size_mask;

            while (true)
            {
                bucket = state[version].tableAligned + masked_entry_word;
                slot = Constants.kInvalidEntrySlot;

                if (FindTagOrFreeInternal(hash, tag, ref bucket, ref slot, ref entry, BeginAddress))
                    return;


                // Install tentative tag in free slot
                entry = default(HashBucketEntry);
                entry.Tag = tag;
                entry.Address = Constants.kTempInvalidAddress;
                entry.Pending = false;
                entry.Tentative = true;

                if (0 == Interlocked.CompareExchange(ref bucket->bucket_entries[slot], entry.word, 0))
                {
                    var orig_bucket = state[version].tableAligned + masked_entry_word;
                    var orig_slot = Constants.kInvalidEntrySlot;

                    if (FindOtherTagMaybeTentativeInternal(hash, tag, ref orig_bucket, ref orig_slot, bucket, slot))
                    {
                        bucket->bucket_entries[slot] = 0;
                    }
                    else
                    {
                        entry.Tentative = false;
                        *((long*)bucket + slot) = entry.word;
                        break;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagInternal(long hash, ushort tag, ref HashBucket* bucket, ref int slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved
                // for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                    {
                        continue;
                    }

                    HashBucketEntry entry = default(HashBucketEntry);
                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        slot = index;

                        // If (final key, return immediately)
                        //if ((entry.tag & ~Constants.kTagMask) == 0) -- Guna: Is this correct?
                        if (!entry.Tentative)
                            return true;
                    }
                }

                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                // Go to next bucket in the chain


                if (target_entry_word == 0)
                {
                    return false;
                }
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagMaybeTentativeInternal(long hash, ushort tag, ref HashBucket* bucket, ref int slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved
                // for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                    {
                        continue;
                    }

                    HashBucketEntry entry = default(HashBucketEntry);
                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        slot = index;
                        return true;
                    }
                }

                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                // Go to next bucket in the chain


                if (target_entry_word == 0)
                {
                    return false;
                }
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        /// <summary>
        /// Find existing entry (non-tenative)
        /// If not found, return pointer to some empty slot
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="tag"></param>
        /// <param name="bucket"></param>
        /// <param name="slot"></param>
        /// <param name="entry"></param>
        /// <param name="BeginAddress"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagOrFreeInternal(long hash, ushort tag, ref HashBucket* bucket, ref int slot, ref HashBucketEntry entry, long BeginAddress = 0)
        {
            var target_entry_word = default(long);
            var recordExists = false;
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved
                // for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                    {
                        if (slot == Constants.kInvalidEntrySlot)
                        {
                            slot = index;
                            entry_slot_bucket = bucket;
                        }
                        continue;
                    }

                    entry.word = target_entry_word;
                    if (entry.Address < BeginAddress)
                    {
                        if (entry.word == Interlocked.CompareExchange(ref bucket->bucket_entries[index], Constants.kInvalidAddress, target_entry_word))
                        {
                            if (slot == Constants.kInvalidEntrySlot)
                            {
                                slot = index;
                                entry_slot_bucket = bucket;
                            }
                            continue;
                        }
                    }
                    if (tag == entry.Tag && !entry.Tentative)
                    {
                        slot = index;
                        recordExists = true;
                        return recordExists;
                    }
                }

                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex);
                // Go to next bucket in the chain


                if ((target_entry_word & Constants.kAddressMask) == 0)
                {
                    if (slot == Constants.kInvalidEntrySlot)
                    {
                        // Allocate new bucket
                        var logicalBucketAddress = overflowBucketsAllocator.Allocate();
                        var physicalBucketAddress = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(logicalBucketAddress);
                        long compare_word = target_entry_word;
                        target_entry_word = logicalBucketAddress;
                        target_entry_word |= (compare_word & ~Constants.kAddressMask);

                        long result_word = Interlocked.CompareExchange(
                            ref bucket->bucket_entries[Constants.kOverflowBucketIndex],
                            target_entry_word,
                            compare_word);

                        if (compare_word != result_word)
                        {
                            // Install failed, undo allocation; use the winner's entry
                            overflowBucketsAllocator.FreeAtEpoch(logicalBucketAddress, 0);
                            target_entry_word = result_word;
                        }
                        else
                        {
                            // Install succeeded
                            bucket = physicalBucketAddress;
                            slot = 0;
                            entry = default(HashBucketEntry);
                            return recordExists;
                        }
                    }
                    else
                    {
                        if (!recordExists)
                        {
                            bucket = entry_slot_bucket;
                        }
                        entry = default(HashBucketEntry);
                        break;
                    }
                }

                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word & Constants.kAddressMask);
            } while (true);

            return recordExists;
        }


        /// <summary>
        /// Find existing entry (tenative or otherwise) other than the specified "exception" slot
        /// If not found, return false. Does not return a free slot.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="tag"></param>
        /// <param name="bucket"></param>
        /// <param name="slot"></param>
        /// <param name="except_bucket"></param>
        /// <param name="except_entry_slot"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOtherTagMaybeTentativeInternal(long hash, ushort tag, ref HashBucket* bucket, ref int slot, HashBucket* except_bucket, int except_entry_slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved
                // for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                    {
                        continue;
                    }

                    HashBucketEntry entry = default(HashBucketEntry);
                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        if ((except_entry_slot == index) && (except_bucket == bucket))
                            continue;

                        slot = index;
                        return true;
                    }
                }

                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                // Go to next bucket in the chain


                if (target_entry_word == 0)
                {
                    return false;
                }
                bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }


        /// <summary>
        /// Helper function used to update the slot atomically with the
        /// new offset value using the CAS operation
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="entrySlot"></param>
        /// <param name="expected"></param>
        /// <param name="desired"></param>
        /// <param name="found"></param>
        /// <returns>If atomic update was successful</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool UpdateSlot(HashBucket* bucket, int entrySlot, long expected, long desired, out long found)
        {
            found = Interlocked.CompareExchange(
                ref bucket->bucket_entries[entrySlot],
                desired,
                expected);

            return (found == expected);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected virtual long GetEntryCount()
        {
            var version = resizeInfo.version;
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_entry_count = 0;

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                        if (0 != b.bucket_entries[bucket_entry])
                            ++total_entry_count;
                    if (b.bucket_entries[Constants.kOverflowBucketIndex] == 0) break;
                    b = *((HashBucket*)overflowBucketsAllocator.GetPhysicalAddress((b.bucket_entries[Constants.kOverflowBucketIndex])));
                }
            }
            return total_entry_count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="version"></param>
        protected virtual void _DumpDistribution(int version)
        {
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_record_count = 0;
            long[] histogram = new long[14];

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                int cnt = 0;
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                    {
                        if (0 != b.bucket_entries[bucket_entry])
                        {
                            ++cnt;
                            ++total_record_count;
                        }
                    }
                    if (b.bucket_entries[Constants.kOverflowBucketIndex] == 0) break;
                    b = *((HashBucket*)overflowBucketsAllocator.GetPhysicalAddress((b.bucket_entries[Constants.kOverflowBucketIndex])));
                }
                if (cnt < 14)
                    histogram[cnt]++;
            }

            Console.WriteLine("Number of hash buckets: {0}", table_size_);
            Console.WriteLine("Total distinct hash-table entry count: {0}", total_record_count);
            Console.WriteLine("Histogram of #entries per bucket: ");
            for (int i=0; i<14; i++)
            {
                Console.WriteLine(i.ToString() + ": " + histogram[i].ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Dumps the distribution of each non-empty bucket in the hash table.
        /// </summary>
        public void DumpDistribution()
        {
            _DumpDistribution(resizeInfo.version);
        }

    }

}

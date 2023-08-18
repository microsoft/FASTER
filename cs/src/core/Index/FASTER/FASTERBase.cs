// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.core;
using Microsoft.Extensions.Logging;

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

        public const int kReadCacheBitShift = 47;
        public const long kReadCacheBitMask = (1L << kReadCacheBitShift);

        public const int kTagSize = 14;
        public const int kTagShift = 62 - kTagSize;
        public const long kTagMask = (1L << kTagSize) - 1;
        public const long kTagPositionMask = (kTagMask << kTagShift);
        public const int kAddressBits = 48;
        public const long kAddressMask = (1L << kAddressBits) - 1;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagSize;

        // Default number of entries in the lock table.
        public const int kDefaultLockTableSize = 16 * 1024;

        public const int kMaxLockSpins = 10;   // TODO verify these
        public const int kMaxReaderLockDrainSpins = kMaxLockSpins * 10;

        /// Invalid entry value
        public const int kInvalidEntrySlot = kEntriesPerBucket;

        /// Location of the special bucket entry
        public const long kOverflowBucketIndex = kEntriesPerBucket - 1;

        /// Invalid value in the hash table
        public const long kInvalidEntry = 0;

        /// Number of times to retry a compare-and-swap before failure
        public const long kRetryThreshold = 1000000;    // TODO unused

        /// Number of times to spin before awaiting or Waiting for a Flush Task.
        public const long kFlushSpinCount = 10;         // TODO verify this number

        /// Number of merge/split chunks.
        public const int kNumMergeChunkBits = 8;
        public const int kNumMergeChunks = 1 << kNumMergeChunkBits;

        // Size of chunks for garbage collection
        public const int kSizeofChunkBits = 14;
        public const int kSizeofChunk = 1 << 14;

        public const long kInvalidAddress = 0;
        public const long kTempInvalidAddress = 1;
        public const long kUnknownAddress = 2;
        public const int kFirstValidAddress = 64;
    }

    [StructLayout(LayoutKind.Explicit, Size = Constants.kEntriesPerBucket * 8)]
    internal unsafe struct HashBucket
    {
        // We use the first overflow bucket for latching, reusing all bits after the address.
        const int kSharedLatchBits = 63 - Constants.kAddressBits;
        const int kExclusiveLatchBits = 1;

        // Shift positions of latches in word
        const int kSharedLatchBitOffset = Constants.kAddressBits;
        const int kExclusiveLatchBitOffset = kSharedLatchBitOffset + kSharedLatchBits;

        // Shared latch constants
        const long kSharedLatchBitMask = ((1L << kSharedLatchBits) - 1) << kSharedLatchBitOffset;
        const long kSharedLatchIncrement = 1L << kSharedLatchBitOffset;

        // Exclusive latch constants
        const long kExclusiveLatchBitMask = 1L << kExclusiveLatchBitOffset;

        // Comnbined mask
        const long kLatchBitMask = kSharedLatchBitMask | kExclusiveLatchBitMask;

        [FieldOffset(0)]
        public fixed long bucket_entries[Constants.kEntriesPerBucket];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireSharedLatch(ref HashEntryInfo hei) => TryAcquireSharedLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireSharedLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];
            int spinCount = Constants.kMaxLockSpins;

            for (; ; Thread.Yield())
            {
                long expected_word = entry_word;
                if (((expected_word & kExclusiveLatchBitMask) == 0) // not exclusively locked
                    && (expected_word & kSharedLatchBitMask) != kSharedLatchBitMask) // shared lock is not full
                {
                    if (expected_word == Interlocked.CompareExchange(ref entry_word, expected_word + kSharedLatchIncrement, expected_word))
                        return true;
                }
                if (spinCount > 0 && --spinCount <= 0)
                    return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseSharedLatch(ref HashEntryInfo hei) => ReleaseSharedLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseSharedLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];
            // X and S latches means an X latch is still trying to drain readers, like this one.
            Debug.Assert((entry_word & kLatchBitMask) != kExclusiveLatchBitMask, "Trying to S unlatch an X-only latched record");
            Debug.Assert((entry_word & kSharedLatchBitMask) != 0, "Trying to S unlatch an unlatched record");
            Interlocked.Add(ref entry_word, -kSharedLatchIncrement);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireExclusiveLatch(ref HashEntryInfo hei) => TryAcquireExclusiveLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireExclusiveLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];
            int spinCount = Constants.kMaxLockSpins;

            // Acquire exclusive lock (readers may still be present; we'll drain them later)
            for (; ; Thread.Yield())
            {
                long expected_word = entry_word;
                if ((expected_word & kExclusiveLatchBitMask) == 0)
                {
                    if (expected_word == Interlocked.CompareExchange(ref entry_word, expected_word | kExclusiveLatchBitMask, expected_word))
                        break;
                }
                if (spinCount > 0 && --spinCount <= 0)
                    return false;
            }

            // Wait for readers to drain. Another session may hold an SLock on this record and need an epoch refresh to unlock, so limit this to avoid deadlock.
            for (var ii = 0; ii < Constants.kMaxReaderLockDrainSpins; ++ii)
            {
                if ((entry_word & kSharedLatchBitMask) == 0)
                    return true;
                Thread.Yield();
            }

            // Release the exclusive bit and return false so the caller will retry the operation. Since we still have readers, we must CAS.
            for (; ; Thread.Yield())
            {
                long expected_word = entry_word;
                if (Interlocked.CompareExchange(ref entry_word, expected_word & ~kExclusiveLatchBitMask, expected_word) == expected_word)
                    break;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseExclusiveLatch(ref HashEntryInfo hei) => ReleaseExclusiveLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseExclusiveLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];

            // We should not be calling this method unless we have successfully acquired the latch (all existing readers were drained).
            Debug.Assert((entry_word & kSharedLatchBitMask) == 0, "Trying to X unlatch an S latched record");
            Debug.Assert((entry_word & kExclusiveLatchBitMask) != 0, "Trying to X unlatch an unlatched record");

            // The address in the overflow bucket may change from unassigned to assigned, so retry
            for (; ; Thread.Yield())
            {
                long expected_word = entry_word;
                if (expected_word == Interlocked.CompareExchange(ref entry_word, expected_word & ~kExclusiveLatchBitMask, expected_word))
                    break;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort NumLatchedShared(HashBucket* bucket)
            => (ushort)((bucket->bucket_entries[Constants.kOverflowBucketIndex] & kSharedLatchBitMask) >> kSharedLatchBitOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLatchedExclusive(HashBucket* bucket)
            => (bucket->bucket_entries[Constants.kOverflowBucketIndex] & kExclusiveLatchBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLatched(HashBucket* bucket)
            => (bucket->bucket_entries[Constants.kOverflowBucketIndex] & kLatchBitMask) != 0;

        public static string ToString(HashBucket* bucket)
        {
            var locks = $"{(IsLatchedExclusive(bucket) ? "x" : string.Empty)}{NumLatchedShared(bucket)}";
            return $"locks {locks}";
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
            readonly get => word & Constants.kAddressMask;
            set
            {
                word &= ~Constants.kAddressMask;
                word |= (value & Constants.kAddressMask);
            }
        }

        public long AbsoluteAddress => Utility.AbsoluteAddress(this.Address);

        public ushort Tag
        {
            readonly get => (ushort)((word & Constants.kTagPositionMask) >> Constants.kTagShift);
            set
            {
                word &= ~Constants.kTagPositionMask;
                word |= ((long)value << Constants.kTagShift);
            }
        }

        public bool Pending
        {
            readonly get => (word & Constants.kPendingBitMask) != 0;
            set
            {
                if (value)
                    word |= Constants.kPendingBitMask;
                else
                    word &= ~Constants.kPendingBitMask;
            }
        }

        public bool Tentative
        {
            readonly get => (word & Constants.kTentativeBitMask) != 0;
            set
            {
                if (value)
                    word |= Constants.kTentativeBitMask;
                else
                    word &= ~Constants.kTentativeBitMask;
            }
        }

        public bool ReadCache
        {
            readonly get => (word & Constants.kReadCacheBitMask) != 0;
            set
            {
                if (value)
                    word |= Constants.kReadCacheBitMask;
                else
                    word &= ~Constants.kReadCacheBitMask;
            }
        }

        public override string ToString()
        {
            var addrRC = this.ReadCache ? "(rc)" : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            return $"addr {this.AbsoluteAddress}{addrRC}, tag {Tag}, tent {bstr(Tentative)}, pend {bstr(Pending)}";
        }
    }

    internal unsafe struct InternalHashTable
    {
        public long size;
        public long size_mask;
        public int size_bits;
        public HashBucket[] tableRaw;
        public HashBucket* tableAligned;
#if !NET5_0_OR_GREATER
        public GCHandle tableHandle;
#endif
    }

    public unsafe partial class FasterBase
    {
        // Initial size of the table
        internal long minTableSize = 16;

        // Allocator for the hash buckets
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocator;
        internal MallocFixedPageSize<HashBucket> overflowBucketsAllocatorResize;

        // An array of size two, that contains the old and new versions of the hash-table
        internal InternalHashTable[] state = new InternalHashTable[2];

        // Array used to denote if a specific chunk is merged or not
        internal long[] splitStatus;

        // Used as an atomic counter to check if resizing is complete
        internal long numPendingChunksToBeSplit;

        internal LightEpoch epoch;

        internal ResizeInfo resizeInfo;

        /// <summary>
        /// LoggerFactory
        /// </summary>
        protected ILoggerFactory loggerFactory;

        /// <summary>
        /// Logger
        /// </summary>
        protected ILogger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        public FasterBase()
        {
            epoch = new LightEpoch();
            overflowBucketsAllocator = new MallocFixedPageSize<HashBucket>();
        }

        internal void Free()
        {
            Free(0);
            Free(1);
            epoch.Dispose();
            overflowBucketsAllocator.Dispose();
        }

        private void Free(int version)
        {
#if !NET5_0_OR_GREATER
            if (state[version].tableHandle.IsAllocated)
                state[version].tableHandle.Free();
#endif
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
            resizeInfo = default;
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
        internal void Initialize(int version, long size, int sector_size)
        {
            long size_bytes = size * sizeof(HashBucket);
            long aligned_size_bytes = sector_size +
                ((size_bytes + (sector_size - 1)) & ~(sector_size - 1));

            //Over-allocate and align the table to the cacheline
            state[version].size = size;
            state[version].size_mask = size - 1;
            state[version].size_bits = Utility.GetLogBase2((int)size);

#if NET5_0_OR_GREATER
            state[version].tableRaw = GC.AllocateArray<HashBucket>((int)(aligned_size_bytes / Constants.kCacheLineBytes), true);
            long sectorAlignedPointer = ((long)Unsafe.AsPointer(ref state[version].tableRaw[0]) + (sector_size - 1)) & ~(sector_size - 1);
            state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
#else
            state[version].tableRaw = new HashBucket[aligned_size_bytes / Constants.kCacheLineBytes];
            state[version].tableHandle = GCHandle.Alloc(state[version].tableRaw, GCHandleType.Pinned);
            long sectorAlignedPointer = ((long)state[version].tableHandle.AddrOfPinnedObject() + (sector_size - 1)) & ~(sector_size - 1);
            state[version].tableAligned = (HashBucket*)sectorAlignedPointer;
#endif
        }

        /// <summary>
        /// A helper function that is used to find the slot corresponding to a
        /// key in the specified version of the hash table
        /// </summary>
        /// <returns>true if such a slot exists, and populates <paramref name="hei"/>, else returns false</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool FindTag(ref HashEntryInfo hei)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);
            var version = resizeInfo.version;
            var masked_entry_word = hei.hash & state[version].size_mask;
            hei.firstBucket = hei.bucket = state[version].tableAligned + masked_entry_word;
            hei.slot = Constants.kInvalidEntrySlot;
            hei.entry = default;
#if DEBUG
            hei.LockCode = masked_entry_word;
#endif // DEBUG

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                        continue;

                    hei.entry.word = target_entry_word;
                    if (hei.tag == hei.entry.Tag && !hei.entry.Tentative)
                    {
                        hei.slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation)
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                {
                    // We lock the firstBucket, so it can't be cleared.
                    hei.bucket = default;
                    hei.entry = default;
                    return false;
                }
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word);
            } while (true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FindOrCreateTag(ref HashEntryInfo hei, long BeginAddress)
        {
            var version = resizeInfo.version;
            var masked_entry_word = hei.hash & state[version].size_mask;
#if DEBUG
            hei.LockCode = masked_entry_word;
#endif // DEBUG

            while (true)
            {
                hei.firstBucket = hei.bucket = state[version].tableAligned + masked_entry_word;
                hei.slot = Constants.kInvalidEntrySlot;

                if (FindTagOrFreeInternal(ref hei, BeginAddress))
                    return;

                // Install tentative tag in free slot
                hei.entry = default;
                hei.entry.Tag = hei.tag;
                hei.entry.Address = Constants.kTempInvalidAddress;
                hei.entry.Pending = false;
                hei.entry.Tentative = true;

                // Insert the tag into this slot. Failure means another session inserted a key into that slot, so continue the loop to find another free slot.
                if (0 == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[hei.slot], hei.entry.word, 0))
                {
                    // Make sure this tag isn't in a different slot already; if it is, make this slot 'available' and continue the search loop.
                    var orig_bucket = state[version].tableAligned + masked_entry_word;  // TODO local var not used; use or change to byval param
                    var orig_slot = Constants.kInvalidEntrySlot;                        // TODO local var not used; use or change to byval param

                    if (FindOtherSlotForThisTagMaybeTentativeInternal(hei.tag, ref orig_bucket, ref orig_slot, hei.bucket, hei.slot))
                    {
                        // We own the slot per CAS above, so it is OK to non-CAS the 0 back in
                        hei.bucket->bucket_entries[hei.slot] = 0;
                        // TODO: Why not return orig_bucket and orig_slot if it's not Tentative?
                    }
                    else
                    {
                        hei.entry.Tentative = false;
                        *((long*)hei.bucket + hei.slot) = hei.entry.word;
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Find existing entry (non-tentative) entry.
        /// </summary>
        /// <returns>If found, return the slot it is in, else return a pointer to some empty slot (which we may have allocated)</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagOrFreeInternal(ref HashEntryInfo hei, long BeginAddress = 0)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)hei.bucket) + index);
                    if (0 == target_entry_word)
                    {
                        if (hei.slot == Constants.kInvalidEntrySlot)
                        {
                            // Record the free slot and continue to search for the key
                            hei.slot = index;
                            entry_slot_bucket = hei.bucket;
                        }
                        continue;
                    }

                    // If the entry points to an address that has been truncated, it's free; try to reclaim it by setting its word to 0.
                    hei.entry.word = target_entry_word;
                    if (hei.entry.Address < BeginAddress && hei.entry.Address != Constants.kTempInvalidAddress)
                    {
                        if (hei.entry.word == Interlocked.CompareExchange(ref hei.bucket->bucket_entries[index], Constants.kInvalidAddress, target_entry_word))
                        {
                            if (hei.slot == Constants.kInvalidEntrySlot)
                            {
                                // Record the free slot and continue to search for the key
                                hei.slot = index;
                                entry_slot_bucket = hei.bucket;
                            }
                            continue;
                        }
                    }
                    if (hei.tag == hei.entry.Tag && !hei.entry.Tentative)
                    {
                        hei.slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation). Don't mask off the non-address bits here; they're needed for CAS.
                target_entry_word = *(((long*)hei.bucket) + Constants.kOverflowBucketIndex);
                while ((target_entry_word & Constants.kAddressMask) == 0)
                {
                    // There is no next bucket. If slot is Constants.kInvalidEntrySlot then we did not find an empty slot, so must allocate a new bucket.
                    if (hei.slot == Constants.kInvalidEntrySlot)
                    {
                        // Allocate new bucket
                        var logicalBucketAddress = overflowBucketsAllocator.Allocate();
                        var physicalBucketAddress = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(logicalBucketAddress);
                        long compare_word = target_entry_word;
                        target_entry_word = logicalBucketAddress;
                        target_entry_word |= compare_word & ~Constants.kAddressMask;

                        long result_word = Interlocked.CompareExchange(
                            ref hei.bucket->bucket_entries[Constants.kOverflowBucketIndex],
                            target_entry_word,
                            compare_word);

                        if (compare_word != result_word)
                        {
                            // Install of new bucket failed; free the allocation and and continue the search using the winner's entry
                            overflowBucketsAllocator.Free(logicalBucketAddress);
                            target_entry_word = result_word;
                            continue;
                        }

                        // Install of new overflow bucket succeeded; the tag was not found, so return the first slot of the new bucket
                        hei.bucket = physicalBucketAddress;
                        hei.slot = 0;
                        hei.entry = default;
                        return false;   // tag was not found
                    }

                    // Tag was not found and an empty slot was found, so return the empty slot
                    hei.bucket = entry_slot_bucket;
                    hei.entry = default;
                    return false;       // tag was not found
                }

                // The next bucket was there or was allocated. Move to it.
                hei.bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(target_entry_word & Constants.kAddressMask);
            } while (true);
        }


        /// <summary>
        /// Look for an existing entry (tentative or otherwise) for this hash/tag, other than the specified "except for this" bucket/slot.
        /// </summary>
        /// <returns>True if found, else false. Does not return a free slot.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOtherSlotForThisTagMaybeTentativeInternal(ushort tag, ref HashBucket* bucket, ref int slot, HashBucket* except_bucket, int except_entry_slot)
        {
            var target_entry_word = default(long);
            var entry_slot_bucket = default(HashBucket*);

            do
            {
                // Search through the bucket looking for our key. Last entry is reserved for the overflow pointer.
                for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                {
                    target_entry_word = *(((long*)bucket) + index);
                    if (0 == target_entry_word)
                        continue;

                    HashBucketEntry entry = default;
                    entry.word = target_entry_word;
                    if (tag == entry.Tag)
                    {
                        if ((except_entry_slot == index) && (except_bucket == bucket))
                            continue;
                        slot = index;
                        return true;
                    }
                }

                // Go to next bucket in the chain (if it is a nonzero overflow allocation).
                target_entry_word = *(((long*)bucket) + Constants.kOverflowBucketIndex) & Constants.kAddressMask;
                if (target_entry_word == 0)
                    return false;
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
            found = Interlocked.CompareExchange(ref bucket->bucket_entries[entrySlot], desired, expected);
            return found == expected;
        }
    }
}

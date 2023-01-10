// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core.Utilities
{
    /// <summary>
    /// A static class to provide constants witout having to use the full type specifications.
    /// </summary>
    internal static class InMemKV
    {
        internal const int kChunkSize = 16;

        // Parameterless struct ctors are silently ignored (in C# 10) for array allocation, so use a "default".
        // If this changes we can use "invalid" and set it to kChunkSize + 1.
        internal const int kDefaultOverflowIndex = 0;
    }

    /// <summary>
    /// User functions that replace <see cref="IFasterEqualityComparer{T}"/> to allow comparison of TKey and THeapKey, as well as other methods.
    /// </summary>
    internal interface IInMemKVUserFunctions<TKey, THeapKey, TValue>
    {
        void Dispose(ref THeapKey key, ref TValue value);

        THeapKey CreateHeapKey(ref TKey key);

        ref TKey GetHeapKeyRef(THeapKey heapKey);

        bool IsActive(ref TValue value);

        bool Equals(ref TKey key, THeapKey heapKey);

        long GetHashCode64(ref TKey key);
    }

    /// <summary>
    /// A single key's entry in the store.
    /// </summary>
    internal struct InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>
        where TUserFunctions : IInMemKVUserFunctions<TKey, THeapKey, TValue>
    {
        // This is a transient structure that contains information about the location of a key entry.
        internal struct Location
        {
            internal InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> chunk;    // 'default' if the target is the initial entry
            internal int chunkEntryIndex;
            internal InMemKV<TKey, THeapKey, TValue, TUserFunctions> kv;
            int bucketIndex;

            internal Location(InMemKV<TKey, THeapKey, TValue, TUserFunctions> kv, int bucketIndex)
            {
                this.kv = kv;
                this.bucketIndex = bucketIndex;
                this.chunk = default;
                this.chunkEntryIndex = InMemKV.kDefaultOverflowIndex;
            }

            internal void SetToInitialEntry()
            {
                this.chunk = default;
                this.chunkEntryIndex = InMemKV.kDefaultOverflowIndex;
            }

            internal void Set(InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> chunk, int entryIndex)
            {
                this.chunk = chunk;
                this.chunkEntryIndex = entryIndex;
            }

            internal bool IsInOverflow => this.chunk is not null;

            internal ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> EntryRef 
                => ref (IsInOverflow ? ref this.chunk[this.chunkEntryIndex] : ref kv.buckets[bucketIndex].InitialEntry);

            public override string ToString() => $"entry {{{EntryRef}}}; IsInOverflow {IsInOverflow}; entryIndex {chunkEntryIndex}}}";
        }

        internal THeapKey heapKey;
        internal TValue value;

        internal void Initialize(THeapKey key)
        {
            this.heapKey = key;
            this.value = default;
        }

        internal void Initialize() => Initialize(default);

        internal void Set(ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> other, IInMemKVUserFunctions<TKey, THeapKey, TValue> userFunctions)
        {
            userFunctions.Dispose(ref this.heapKey, ref this.value);
            this.heapKey = other.heapKey;
            this.value = other.value;
        }

        internal void Dispose(TUserFunctions userFunctions)
        {
            userFunctions.Dispose(ref this.heapKey, ref this.value);
            Initialize();
        }

        internal bool IsActive(TUserFunctions userFunctions) => userFunctions.IsActive(ref this.value);

        internal bool IsDefault => this.heapKey is null;

        public override string ToString() => $"key {{{heapKey}}}; value {{{value}}}; isDef {IsDefault}";
    }

    /// <summary>
    /// A single chunk (C# array) of <see cref="InMemKVEntry{TKey, THeapKey, TValue, TUserFunctions}"/> structs.
    /// </summary>
    internal class InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions>
        where TUserFunctions : IInMemKVUserFunctions<TKey, THeapKey, TValue>
    {
        internal InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> prev;
        internal InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> next;

        private readonly InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>[] entries = new InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>[InMemKV.kChunkSize];

        internal ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> this[int index] => ref this.entries[index];

        internal void LinkAfter(InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> prior)
        {
            Debug.Assert(prior.next is null, "We should only append to the end of the linked list");
            this.prev = prior;
            prior.next = this;
        }

        internal InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> UnlinkAfter()
        {
            Debug.Assert(this.next is null, "We should only remove from the end of the linked list");
            var prev = this.prev;
            if (this.prev is not null)
            {
                this.prev.next = null;
                this.prev = null;
            }
            return prev;
        }
    }

    /// <summary>
    /// A vertebra of the hash table spine, containing the single initial <see cref="InMemKVEntry{TKey, THeapKey, TValue, TUserFunctions}"/>,
    /// the final <see cref="InMemKVChunk{TKey, THeapKey, TValue, TUserFunctions}"/> if any are allocated, and methods for shared and exclusive
    /// locking of the bucket.
    /// </summary>
    internal struct InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions>
        where TUserFunctions : IInMemKVUserFunctions<TKey, THeapKey, TValue>
    {
        #region Bucket Constants
        // We use our own locking etc. here as we do not use Tentative in the same way, we have lock promotion, and we have an ExclusiveGeneration value.
        // We reuse the part of the PreviousAddress space as LastChunkEntryIndex, but only sizeof(int) * 8 bits, because we are limiting it to the offset
        // within a single chunk. So the layout of our bucket word is:
        // [GGGGGGGG] [GGGGGGGG] [GGGGGGGG][G][X][SSSSSS] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
        //     where X = exclusive lock, S = shared lock, R = readcache, A = address, G = ExclusiveGeneration

        const int kLastActiveChunkEntryIndexBits = 32;
        const long kLastActiveChunkEntryIndexBitMask = (1L << kLastActiveChunkEntryIndexBits) - 1;

        // Shift position of lock in word
        const int kSharedLockBitOffset = kLastActiveChunkEntryIndexBits;

        // Use the same lock bit count as RecordInfo--7 lock bits: 6 shared lock bits + 1 exclusive lock bit
        const int kSharedLockBits = RecordInfo.kSharedLockBits;
        const int kExclusiveLockBits = RecordInfo.kExclusiveLockBits;

        // Shared lock constants
        const long kSharedLockBitMask = ((1L << kSharedLockBits) - 1) << kSharedLockBitOffset;
        const long kSharedLockIncrement = 1L << kSharedLockBitOffset;

        // Exclusive lock constants
        const int kExclusiveLockBitOffset = kSharedLockBitOffset + kSharedLockBits;
        const long kExclusiveLockBitMask = 1L << kExclusiveLockBitOffset;

        // ExclusiveGeneration constants
        const int kExclusiveGenerationBitOffset = kExclusiveLockBitOffset + kExclusiveLockBits;
        const int kExclusiveGenerationBits = 25;
        const long kExclusiveGenerationBitMask = ((1L << kExclusiveGenerationBits) - 1) << kExclusiveGenerationBitOffset;
        const long kExclusiveGenerationIncrement = 1L << kExclusiveGenerationBitOffset;
        #endregion Bucket Constants

        #region Bucket Data
        // The single word that implements locking etc. using the above constants.
        private long word;

        // The initial entry. Often we will have only one, so no allocation is needed.
        internal InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> InitialEntry;

        // The last overflow chunk, if any are allocated. We store the last one instead of the first to facilitate compaction,
        // which accesses the end of the chunk list.
        internal InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> LastOverflowChunk;
        #endregion Bucket Data

        internal bool HasEntries => !this.InitialEntry.IsDefault;
        internal bool HasOverflow => this.LastOverflowChunk is not null;

        internal bool IsXLocked => (word & kExclusiveLockBitMask) != 0;
        internal long NumSLocks => (word & kSharedLockBitMask) >> kSharedLockBitOffset;

        // The index of the last active entry in the overflow chunks
        internal int LastActiveChunkEntryIndex
        {
            get => (int)(word & kLastActiveChunkEntryIndexBitMask);
            set => word = (word & ~kLastActiveChunkEntryIndexBitMask) | (value & kLastActiveChunkEntryIndexBitMask);
        }

        // The ExclusiveGeneration value enables optimization of Compact(); it wraps around on overflow (currently at 33m).
        internal int ExclusiveGeneration => (int)((word & kExclusiveGenerationBitMask) >> kExclusiveGenerationBitOffset);
        internal void IncrementExclusiveGeneration() => word = (word & ~kExclusiveGenerationBitMask) | IncrementExclusiveGeneration(word & kExclusiveGenerationBitMask);
        internal static long IncrementExclusiveGeneration(long value) => value == kExclusiveGenerationBitMask ? 0 : value + kExclusiveGenerationIncrement;

        public InMemKVBucket()
        {
            this.word = default;
            this.InitialEntry = default;
            this.LastOverflowChunk = default;

            // Parameterless struct ctors are silently ignored (in C# 10) for array allocation, so use a "default".
            // If this changes we can use "invalid" set to kChunkSize + 1 here (and when removing the last overflow chunk) and Assert() a stronger consistency check.
            this.LastActiveChunkEntryIndex = InMemKV.kDefaultOverflowIndex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void XLock()
        {
            // Acquire exclusive lock (readers may still be present; we'll drain them below)
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if ((expected_word & kExclusiveLockBitMask) == 0 && Interlocked.CompareExchange(ref word, expected_word | kExclusiveLockBitMask, expected_word) == expected_word)
                    break;
            }

            // Wait for readers to drain
            while ((word & kSharedLockBitMask) != 0)
                Thread.Yield();
            this.IncrementExclusiveGeneration();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void XUnlock()
        {
            Debug.Assert(IsXLocked, "Trying to X unlock an unlocked bucket");
            word &= ~kExclusiveLockBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SLock()
        {
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if ((expected_word & kExclusiveLockBitMask) == 0 // not exclusively locked
                    && (expected_word & kSharedLockBitMask) != kSharedLockBitMask // shared lock is not full
                    && Interlocked.CompareExchange(ref word, expected_word + kSharedLockIncrement, expected_word) == expected_word)
                    break;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SUnlock()
        {
            Debug.Assert(NumSLocks > 0, "Trying to S unlock an unlocked bucket");
            Interlocked.Add(ref word, -kSharedLockIncrement);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool SUnlockAndTryXLock(out bool wasImmediate)
        {
            Debug.Assert(NumSLocks > 0, $"Bucket SLock count should not be 0 when promoting");
            var prevGen = this.word & kExclusiveGenerationBitMask;

            // Acquire exclusive lock in place of this thread's shared lock (other readers may still be present; we'll drain them below).
            // We must SUnlock first to avoid deadlock in the "draining other readers" phase if are multiple threads in here.
            wasImmediate = true;
            SUnlock();

            int maxxRetries = 10;    

            while (true)
            {
                for (; ; Thread.Yield())
                {
                    long expected_word = word;
                    bool acquired = (expected_word & kExclusiveLockBitMask) == 0 // not exclusively locked
                                    && Interlocked.CompareExchange(ref word, expected_word | kExclusiveLockBitMask, expected_word) == expected_word;
                    if ((this.word & kExclusiveGenerationBitMask) != prevGen)
                        wasImmediate = false;
                    if (acquired)
                        break;
                }

                // Wait for readers to drain. In the LockTable scenario, we have no key locks on this thread, but a second thread may have a
                // key lock and a third thread may have a read lock trying to acquire that key lock; let go of our bucket XLock to let the second
                // thread acquire the read lock to unlock it, and retry the lock acquisition.
                for (var ii = 0; ii < Constants.kMaxReaderLockDrainSpins / 2; ++ii)     // Make this shorter than RecordInfo's XLock drain
                {
                    if ((word & kSharedLockBitMask) == 0)
                        break;
                    Thread.Yield();
                }

                if ((word & kSharedLockBitMask) == 0)
                    break;

                // Release the exclusive bit and return false so an Unlock has a chance to get into the bucket.
                // To reset this bit while spinning to drain readers, we must use CAS to avoid losing a reader unlock.
                for (; ; Thread.Yield())
                {
                    long expected_word = word;
                    if (Interlocked.CompareExchange(ref word, expected_word & ~kExclusiveLockBitMask, expected_word) == expected_word)
                        break;
                }

                if (--maxxRetries <= 0)
                    return false;
                Thread.Yield();
            }

            this.IncrementExclusiveGeneration();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SUnlockAndTryXLockAndCompaction(TUserFunctions userFunctions, ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation)
        {
            // We don't need try{} because we don't allocate.
            if (this.SUnlockAndTryXLock(out bool wasImmediate))
            {
                DoCompaction(userFunctions, ref entryLocation, wasImmediate);
                this.XUnlock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DoCompaction(TUserFunctions userFunctions, ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation, bool wasImmediateXlock)
        {
            // Some other thread may've removed all entries before we got the lock.
            if (!this.HasEntries)
                return;

            if (this.LastOverflowChunk is null)
            {
                // There are no overflow chunks; see if initialEntry is active. (Note that we may have gotten here even though entryLocation
                // may be in the overflow chunks, if other threads trimmed away trailing InActive entries).
                if (!this.InitialEntry.IsActive(userFunctions))
                {
                    this.InitialEntry.Dispose(userFunctions);
                    entryLocation.kv.DecrementActiveBuckets();
                }
                return;
            }

            if (wasImmediateXlock)
            {
                // Need to recheck IsActive because even if wasImmediateXLock, another thread may have already had an SLock on the bucket,
                // or gotten it, and added a lock between the time we previously checked IsActive and the time we got the XLock.
                ref var targetEntry = ref entryLocation.EntryRef;
                if (!targetEntry.IsDefault && !targetEntry.IsActive(userFunctions))
                    this.CompactInto(userFunctions, ref entryLocation);
            }
            else
            {
                // We did not acquire the Xlock immediately (before another thread got it), so make sure the target entry is still valid:
                // The chunk must be in our assigned list, and the entry at the index must be inactive (but not default). Some notes about this:
                //  1. We only insert new entries at the end of the list, never in the middle. This means our target entry will never be at a
                //     position "later" than it was when we made this call.
                //  2. Every CompactInto() removes InActive entries at the end of the list (an InActive entry will never be swapped into a
                //     target entry). Since another thread got the XLock before us, our target entry was removed if it was at the end of the list.
                for (var chunk = this.LastOverflowChunk; chunk is not null; chunk = chunk.prev)
                {
                    if (object.ReferenceEquals(chunk, entryLocation.chunk))
                    {
                        ref var targetEntry = ref entryLocation.EntryRef;
                        if (!targetEntry.IsDefault && !targetEntry.IsActive(userFunctions))
                            this.CompactInto(userFunctions, ref entryLocation);
                        break;
                    }
                }
            }

            if (!this.HasEntries)
                entryLocation.kv.DecrementActiveBuckets();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CompactInto(TUserFunctions userFunctions, ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation)
        {
#if DEBUG
            Debug.Assert(this.HasOverflow, "Should only Compact when there are overflow allocations");
            ref var targetEntry = ref entryLocation.EntryRef;
            Debug.Assert(!targetEntry.IsActive(userFunctions), "Should only CompactInto an InActive entry");
            Debug.Assert(!targetEntry.IsDefault, "Should only CompactInto a non-IsDefault) entry");
#endif

            // Other threads may have final-unlocked one or more entries at the end of the active set; if so, back
            // up over them, removing them as we go. Note that this may back up over entryLocation; we don't stop if so.
            var removedTargetChunk = false;
            ref var lastEntry = ref this.LastOverflowChunk[this.LastActiveChunkEntryIndex];
            while (!lastEntry.IsDefault && !lastEntry.IsActive(userFunctions))
            {
                // We must always clear lastEntry, even if freeing the chunk.
                lastEntry.Dispose(userFunctions);
                removedTargetChunk = RemoveLastActiveChunkEntry(userFunctions, ref entryLocation, ref lastEntry);

                if (this.LastOverflowChunk is null)
                {
                    // We've freed all chunks. If InitialEntry is InActive, Clear() it, then return.
                    if (!this.InitialEntry.IsActive(userFunctions))
                        this.InitialEntry.Dispose(userFunctions);
                    return;
                }
                lastEntry = ref this.LastOverflowChunk[this.LastActiveChunkEntryIndex];
            }

            if (!removedTargetChunk)
            {
                // We did not back up over toEntry, so transfer the entry data, then remove the last entry (that we just transferred from).
                if (entryLocation.IsInOverflow)
                {
                    ref var toEntry = ref entryLocation.EntryRef;
                    if (toEntry.IsDefault)
                        return;
                    toEntry.Set(ref lastEntry, userFunctions);
                }
                else
                {
                    // Note that we can never back up over InitialEntry (removedChunkTarget will always be false because we never free a 'null' chunk).
                    this.InitialEntry.Set(ref lastEntry, userFunctions);
                }

                // Because we copied lastEntry's key and value to a new location, we can't Dispose() them
                lastEntry.Initialize();
                RemoveLastActiveChunkEntry(userFunctions, ref entryLocation, ref lastEntry);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool RemoveLastActiveChunkEntry(TUserFunctions userFunctions, ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation,
                                                ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> lastEntry)
        {
            if (this.LastActiveChunkEntryIndex == 0)
            {
                // Back up to the previous chunk's last entry--or perhaps to kInvalidOverflowIndex, which means the previous "chunk" is InitialEntry.
                var result = this.RemoveLastChunk(entryLocation);
                this.LastActiveChunkEntryIndex = this.LastOverflowChunk is null ? InMemKV.kDefaultOverflowIndex : InMemKV.kChunkSize - 1;
                return result;
            }

            // Not the first entry on the chunk so just decrement
            --this.LastActiveChunkEntryIndex;
            return false;
        }

        private bool RemoveLastChunk(InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation)
        {
            var freed = this.LastOverflowChunk;
            this.LastOverflowChunk = this.LastOverflowChunk.UnlinkAfter();
            entryLocation.kv.OnChunkFreed(freed);
            return object.ReferenceEquals(freed, entryLocation.chunk);
        }

        internal ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> GetNextFreeChunkEntry()
        {
            Debug.Assert(this.IsXLocked, "Must have bucket XLock before getting next free chunk entry");
            Debug.Assert(!this.InitialEntry.IsDefault, "Should not be getting a free chunk entry when InitialEntry.IsDefault");
            Debug.Assert(this.HasOverflow || this.LastActiveChunkEntryIndex == InMemKV.kDefaultOverflowIndex, "If there are no overflow chunks, LastActiveChunkEntryIndex should be kDefaultOverflowIndex");
            if (this.HasOverflow && this.LastActiveChunkEntryIndex < InMemKV.kChunkSize - 1)
                return ref this.LastOverflowChunk[++LastActiveChunkEntryIndex];
            return ref AddChunk();
        }

        private ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> AddChunk()
        {
            var chunk = new InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions>();
            if (this.LastOverflowChunk is not null)
                chunk.LinkAfter(this.LastOverflowChunk);
            this.LastOverflowChunk = chunk;
            this.LastActiveChunkEntryIndex = 0;
            return ref chunk[this.LastActiveChunkEntryIndex];
        }

        internal void Dispose(TUserFunctions userFunctions)
        {
            // Back up through the overflow chunks.
            var lastActiveEntryIndexOnChunk = this.LastActiveChunkEntryIndex;
            while (this.HasOverflow)
            {
                for (var iEntry = 0; iEntry < lastActiveEntryIndexOnChunk; ++iEntry)
                {
                    ref var entry = ref this.LastOverflowChunk[iEntry];
                    Debug.Assert(!entry.IsDefault, "Entry should not be default");
                    entry.Dispose(userFunctions);
                }
                lastActiveEntryIndexOnChunk = InMemKV.kChunkSize - 1;
                this.LastOverflowChunk = this.LastOverflowChunk.prev;
            }
            if (!this.InitialEntry.IsDefault)
                InitialEntry.Dispose(userFunctions);
        }

        public override string ToString()
        {
            var locks = $"{(this.IsXLocked ? "x" : string.Empty)}{this.NumSLocks}";
            return $"initEntry {{{InitialEntry}}}, locks {locks}, LastACEI {LastActiveChunkEntryIndex}, XGen {ExclusiveGeneration}";
        }
    }

    /// <summary>
    /// The high-level In-Memory Key/Value store. Contains the list of buckets, the freelist, and user functions.
    /// </summary>
    internal class InMemKV<TKey, THeapKey, TValue, TUserFunctions> : IDisposable
        where TUserFunctions : IInMemKVUserFunctions<TKey, THeapKey, TValue>
    {
        internal readonly InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions>[] buckets;

        private readonly TUserFunctions userFunctions;

        private readonly ConcurrentStack<InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions>> freeList = new();
        internal int FreeListCount => freeList.Count;
        private readonly int maxFreeChunks;

        internal InMemKV(int numBuckets, int maxFreeChunks, TUserFunctions userFunctions)
        {
            this.buckets = new InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions>[numBuckets];
            this.maxFreeChunks = maxFreeChunks;
            this.userFunctions = userFunctions;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions> GetBucket(ref TKey key, out int index) => ref GetBucket(this.userFunctions.GetHashCode64(ref key), out index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions> GetBucket(long hash, out int index)
        {
            index = (int)(hash >= 0 ? hash % buckets.Length : -hash % buckets.Length);
            return ref buckets[index];
        }

        internal bool HasEntries(ref TKey key) => GetBucket(ref key, out _).HasEntries;
        internal bool HasEntries(long hash) => GetBucket(hash, out _).HasEntries;

        long NumActiveBuckets = 0;

        void IncrementActiveBuckets() => Interlocked.Increment(ref NumActiveBuckets);
        internal void DecrementActiveBuckets() => Interlocked.Decrement(ref NumActiveBuckets);

        public bool IsActive => this.NumActiveBuckets > 0;

#region Individual operation callbacks
        internal interface IFindEntryFunctions
        {
            void NotFound(ref TKey key);
            void FoundEntry(ref TKey key, ref TValue value);
        }

        internal interface IAddEntryFunctions
        {
            void AddedEntry(ref TKey key, ref TValue value);
        }

        internal interface IFindOrAddEntryFunctions : IAddEntryFunctions
        {
            void FoundEntry(ref TKey key, ref TValue value);
        }
#endregion

        // This function is here instead of on the bucket due to: CS8170 Struct members cannot return 'this' or other instance members by reference
        private ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions> TryToFindEntry(ref InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions> bucket, ref TKey key, 
                                                                                        ref InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation, out bool found)
        {
            if (!bucket.InitialEntry.IsDefault && this.userFunctions.Equals(ref key, bucket.InitialEntry.heapKey))
            {
                found = true;
                entryLocation.SetToInitialEntry();
                return ref bucket.InitialEntry;
            }

            // Back up through the overflow chunks.
            var lastActiveEntryIndexOnChunk = bucket.LastActiveChunkEntryIndex;
            for (var chunk = bucket.LastOverflowChunk; chunk is not null; chunk = chunk.prev)
            {
                for (var iEntry = 0; iEntry <= lastActiveEntryIndexOnChunk; ++iEntry)
                {
                    ref var entry = ref chunk[iEntry];
                    Debug.Assert(!entry.IsDefault, "Entry should not be default");
                    if (this.userFunctions.Equals(ref key, entry.heapKey))
                    {
                        found = true;
                        entryLocation.Set(chunk, iEntry);
                        return ref entry;
                    }
                }
                lastActiveEntryIndexOnChunk = InMemKV.kChunkSize - 1;
            }

            // If we're here, it was not found.
            found = false;
            return ref bucket.InitialEntry; // We have to return a reference to something
        }

        /// <summary>
        /// Try to find the entry, and call the user functions
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hash">The hash code of the key, to avoid recalculating</param>
        /// <param name="funcs">The caller's implementation of the appropriate functions (see the <typeparamref name="TFuncs"/> type constraint)</param>
        /// <returns>true if the key was found (even if it was rendered InActive and removed), else false</returns>
        /// <remarks>InMemKV does not have a Remove method, because its compaction is based on the InActive property;
        ///     use a <typeparamref name="TFuncs"/> implementation that renderes the entry InActive in FoundEntry.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool FindEntry<TFuncs>(ref TKey key, long hash, ref TFuncs funcs) 
            where TFuncs : IFindEntryFunctions
        {
            ref var bucket = ref GetBucket(hash, out var bucketIndex);
            bucket.SLock();

            if (!bucket.InitialEntry.IsDefault)
            {
                InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation = new(this, bucketIndex);
                ref var entry = ref TryToFindEntry(ref bucket, ref key, ref entryLocation, out bool found);
                if (found)
                {
                    funcs.FoundEntry(ref key, ref entry.value);
                    if (!entry.IsActive(userFunctions))
                        bucket.SUnlockAndTryXLockAndCompaction(userFunctions, ref entryLocation);
                    else
                        bucket.SUnlock();
                    return true;
                }
            }
            funcs.NotFound(ref key);
            bucket.SUnlock();
            return false;
        }

        /// <summary>
        /// Try to find the entry, and call the user functions
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hash">The hash code of the key, to avoid recalculating</param>
        /// <param name="funcs">The caller's implementation of the appropriate functions (see the <typeparamref name="TFuncs"/> type constraint)</param>
        /// <returns>true if the key was found (even if it was rendered InActive and removed), else false</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool FindOrAddEntry<TFuncs>(ref TKey key, long hash, ref TFuncs funcs)
            where TFuncs : IFindOrAddEntryFunctions
        {
            ref var bucket = ref GetBucket(hash, out var bucketIndex);
            bucket.SLock();

            InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation = new(this, bucketIndex);
            ref var entry = ref TryToFindEntry(ref bucket, ref key, ref entryLocation, out bool found);
            if (found)
            {
                funcs.FoundEntry(ref key, ref entry.value);
                if (!entry.IsActive(userFunctions))
                    bucket.SUnlockAndTryXLockAndCompaction(userFunctions, ref entryLocation);
                else
                    bucket.SUnlock();
                return true;
            }

            // This has try/catch so can't be inlined; make it a separate function.
            return SUnlockAndTryXLockAndFindOrAddEntry(ref key, ref bucket, bucketIndex, ref funcs);
        }

        private bool SUnlockAndTryXLockAndFindOrAddEntry<TFuncs>(ref TKey key, ref InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions> bucket, int bucketIndex, ref TFuncs funcs)
            where TFuncs : IFindOrAddEntryFunctions
        {
            // Add a new entry. We XLock whenever we change the element membership. Use try/finally due to allocation
            if (!bucket.SUnlockAndTryXLock(out bool wasImmediateXlock))
                return false;

            bool wasActive = bucket.HasEntries;
            try
            {
                // If someone else acquired the xlock before we did, we must recheck once we acquire it to make sure the key wasn't added by another thread.
                if (!wasImmediateXlock)
                {
                    InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation = new(this, bucketIndex);
                    ref var entry = ref TryToFindEntry(ref bucket, ref key, ref entryLocation, out bool found);
                    if (found)
                    {
                        funcs.FoundEntry(ref key, ref entry.value);
                        if (!entry.IsActive(userFunctions))
                        {
                            // Here the wasImmediateXLock is true because we held the XLock when performing the user operation.
                            bucket.DoCompaction(userFunctions, ref entryLocation, wasImmediateXlock: true);
                        }
                        return true;
                    }
                }

                AddEntry(ref key, ref funcs, ref bucket, wasActive);
                return true;
            }
            catch (Exception)
            {
                // Allocation threw, so remove the pre-increment if we added it.
                if (!wasActive)
                    DecrementActiveBuckets();
                throw;
            }
            finally
            {
                bucket.XUnlock();
            }
        }

        /// <summary>
        /// Add the entry, and call the user functions
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hash">The hash code of the key, to avoid recalculating</param>
        /// <param name="funcs">The caller's implementation of the appropriate functions (see the <typeparamref name="TFuncs"/> type constraint)</param>
        public void AddEntry<TFuncs>(ref TKey key, long hash, ref TFuncs funcs)
            where TFuncs : IAddEntryFunctions
        {
            ref var bucket = ref GetBucket(hash, out var bucketIndex);

            // This is like FindOrAddEntry, except we know the key is not in the table, so we XLock directly.
            bucket.XLock();

#if DEBUG
            InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation = new(this, bucketIndex);
            _ = ref TryToFindEntry(ref bucket, ref key, ref entryLocation, out bool found);
            Debug.Assert(!found, "Should not find key in AddEntry()");
#endif

            bool wasActive = bucket.HasEntries;
            try
            {
                AddEntry(ref key, ref funcs, ref bucket, wasActive);
            }
            catch (Exception)
            {
                // Allocation threw, so remove the pre-increment if we added it.
                if (!wasActive)
                    DecrementActiveBuckets();
                throw;
            }
            finally
            {
                bucket.XUnlock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddEntry<TFuncs>(ref TKey key, ref TFuncs funcs, ref InMemKVBucket<TKey, THeapKey, TValue, TUserFunctions> bucket, bool wasActive) 
            where TFuncs : IAddEntryFunctions
        {
            // At this point we know we will add an entry, so pre-increment the active-bucket count to avoid a race where we add the entry but 
            // another thread does not see it immediately because this will become the first active bucket.
            if (!wasActive)
                IncrementActiveBuckets();

            // Find the next free entry
            ref var entry = ref bucket.InitialEntry.IsDefault ? ref bucket.InitialEntry : ref bucket.GetNextFreeChunkEntry();
            entry.Initialize(userFunctions.CreateHeapKey(ref key));
            funcs.AddedEntry(ref key, ref entry.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGet(ref TKey key, out TValue value) => TryGet(ref key, this.userFunctions.GetHashCode64(ref key), out value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGet(ref TKey key, long hash, out TValue value)
        {
            ref var bucket = ref GetBucket(hash, out var bucketIndex);
            bucket.SLock();
            InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation = new(this, bucketIndex);
            ref var entry = ref TryToFindEntry(ref bucket, ref key, ref entryLocation, out bool found);
            value = found ? entry.value : default;
            bucket.SUnlock();
            return found;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnChunkFreed(InMemKVChunk<TKey, THeapKey, TValue, TUserFunctions> chunk)
        {
            if (this.freeList.Count < this.maxFreeChunks)
                this.freeList.Push(chunk);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ContainsKey(ref TKey key, long hash)
        {
            ref var bucket = ref GetBucket(hash, out var bucketIndex);
            bucket.SLock();
            InMemKVEntry<TKey, THeapKey, TValue, TUserFunctions>.Location entryLocation = new(this, bucketIndex);
            _ = ref TryToFindEntry(ref bucket, ref key, ref entryLocation, out bool found);
            bucket.SUnlock();
            return found;
        }

        public void Dispose()
        {
            if (this.buckets != null)
            {
                foreach (var bucket in this.buckets)
                    bucket.Dispose(this.userFunctions);
            }
            this.freeList.Clear();
        }
    }
}

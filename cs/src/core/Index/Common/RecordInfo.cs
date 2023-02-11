// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static FASTER.core.Utility;

namespace FASTER.core
{
    // RecordInfo layout (64 bits total):
    // [Unused2][Modified][InNewVersion][Filler][Dirty][Unused2][Sealed] [Valid][Tombstone][X][SSSSSS] [RAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where X = exclusive lock, S = shared lock, R = readcache, A = address
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Previous address
        const int kPreviousAddressBits = 48;
        const long kPreviousAddressMaskInWord = (1L << kPreviousAddressBits) - 1;

        // Shift position of lock in word
        const int kLockShiftInWord = kPreviousAddressBits;

        // We use 7 lock bits: 6 shared lock bits + 1 exclusive lock bit
        internal const int kSharedLockBits = 6;
        internal const int kExclusiveLockBits = 1;

        // Shared lock constants
        const long kSharedLockMaskInWord = ((1L << kSharedLockBits) - 1) << kLockShiftInWord;
        const long kSharedLockIncrement = 1L << kLockShiftInWord;

        // Exclusive lock constants
        const int kExclusiveLockBitOffset = kLockShiftInWord + kSharedLockBits;
        const long kExclusiveLockBitMask = 1L << kExclusiveLockBitOffset;
        const long kLockBitMask = kSharedLockMaskInWord | kExclusiveLockBitMask;

        // Other marker bits
        const int kTombstoneBitOffset = kExclusiveLockBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kUnused1BitOffset = kValidBitOffset + 1;
        const int kSealedBitOffset = kUnused1BitOffset + 1;
        const int kDirtyBitOffset = kSealedBitOffset + 1;
        const int kFillerBitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kFillerBitOffset + 1;
        const int kModifiedBitOffset = kInNewVersionBitOffset + 1;
        // If these become used, start with the highest number
        internal const int kUnusedBitOffset = kModifiedBitOffset + 1;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kUnused1BitMask = 1L << kUnused1BitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;
        const long kFillerBitMask = 1L << kFillerBitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;
        const long kModifiedBitMask = 1L << kModifiedBitOffset;
        internal const long kUnused2BitMask = 1L << kUnusedBitOffset;

        [FieldOffset(0)]
        private long word;

        public static void WriteInfo(ref RecordInfo info, bool inNewVersion, bool tombstone, long previousAddress)
        {
            info.word = default;
            info.Tombstone = tombstone;
            info.SetValid();
            info.Dirty = false;
            info.PreviousAddress = previousAddress;
            info.InNewVersion = inNewVersion;
            info.Modified = false;
        }

        public bool Equals(RecordInfo other) => this.word == other.word;

        public long GetHashCode64() => Utility.GetHashCode(this.word);

        public bool IsLocked => (word & (kExclusiveLockBitMask | kSharedLockMaskInWord)) != 0;

        public bool IsLockedExclusive => (word & kExclusiveLockBitMask) != 0;
        public bool IsLockedShared => NumLockedShared != 0;

        public byte NumLockedShared => (byte)((word & kSharedLockMaskInWord) >> kLockShiftInWord);

        public void ClearLocks() => word &= ~(kExclusiveLockBitMask | kSharedLockMaskInWord);

        // We ignore locks and temp bits for disk images
        public void ClearBitsForDiskImages() => word &= ~(kExclusiveLockBitMask | kSharedLockMaskInWord | kDirtyBitMask);

        private static bool IsClosedWord(long word) => (word & (kSealedBitMask | kValidBitMask)) != kValidBitMask;

        public bool IsClosed => IsClosedWord(word);

        public bool TryLock(LockType lockType)
        {
            if (lockType == LockType.Shared)
                return this.TryLockShared();
            if (lockType == LockType.Exclusive)
                return this.TryLockExclusive();
            else
                Debug.Fail($"Unexpected LockType: {lockType}");
            return false;
        }

        public void Unlock(LockType lockType)
        {
            if (lockType == LockType.Shared)
                this.UnlockShared();
            else if (lockType == LockType.Exclusive)
                this.UnlockExclusive();
            else
                Debug.Fail($"Unexpected LockType: {lockType}");
        }

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="TryLockExclusive"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusive()
        {
            Debug.Assert(!IsLockedShared, "Trying to X unlock an S locked record");
            Debug.Assert(IsLockedExclusive, "Trying to X unlock an unlocked record");
            word &= ~kExclusiveLockBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Try to take an exclusive (write) lock on RecordInfo
        /// </summary>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockExclusive()
        {
            int spinCount = Constants.kMaxLockSpins;

            // Acquire exclusive lock (readers may still be present; we'll drain them later)
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if ((expected_word & kExclusiveLockBitMask) == 0)
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word | kExclusiveLockBitMask, expected_word))
                        break;
                }
                if (spinCount > 0 && --spinCount <= 0)
                    return false;
            }

            // Wait for readers to drain. Another session may hold an SLock on this record and need an epoch refresh to unlock, so limit this to avoid deadlock.
            for (var ii = 0; ii < Constants.kMaxReaderLockDrainSpins; ++ii)
            {
                if ((word & kSharedLockMaskInWord) == 0)
                {
                    // Someone else may have transferred/invalidated the record while we were draining reads.
                    if (!IsClosedWord(this.word))
                        return true;
                    break;
                }
                Thread.Yield();
            }

            // Release the exclusive bit and return false so the caller will retry the operation.
            // To reset this bit while spinning to drain readers, we must use CAS to avoid losing a reader unlock.
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if (Interlocked.CompareExchange(ref word, expected_word & ~kExclusiveLockBitMask, expected_word) == expected_word)
                    break;
            }
            return false;
        }

        /// <summary>Unlock RecordInfo that was previously locked for shared access, via <see cref="TryLockShared"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockShared()
        {
            // X and S locks means an X lock is still trying to drain readers, like this one.
            Debug.Assert((word & kLockBitMask) != kExclusiveLockBitMask, "Trying to S unlock an X-only locked record");
            Debug.Assert(IsLockedShared, "Trying to S unlock an unlocked record");
            Interlocked.Add(ref word, -kSharedLockIncrement);
        }

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockShared()
        {
            int spinCount = Constants.kMaxLockSpins;

            // Acquire shared lock
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if (((expected_word & kExclusiveLockBitMask) == 0) // not exclusively locked
                    && (expected_word & kSharedLockMaskInWord) != kSharedLockMaskInWord) // shared lock is not full
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word + kSharedLockIncrement, expected_word))
                        break;
                }
                if (spinCount > 0 && --spinCount <= 0) 
                    return false;
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeLockShared() => this.word += kSharedLockIncrement;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeLockExclusive() => this.word |= kExclusiveLockBitMask;

        /// <summary>
        /// Try to reset the modified bit of the RecordInfo
        /// </summary>
        /// <returns>Whether the modified bit was reset successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryResetModifiedAtomic()
        {
            int spinCount = Constants.kMaxLockSpins;
            while (true)
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if ((expected_word & kModifiedBitMask) == 0)
                    return true;
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & (~kModifiedBitMask), expected_word))
                    break;
                if (spinCount > 0 && --spinCount <= 0)
                    return false;
                Thread.Yield();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TransferLocksFrom(ref RecordInfo source)
        {
            // This is called only in the Lock Table, when we have an exclusive bucket lock, so no interlock is needed and we clear the locks
            // to make it InActive and to ensure that ReadCache won't double-copy if there is a CAS failure during ReadCacheEvict.
            this.word &= ~(kExclusiveLockBitMask | kSharedLockMaskInWord);
            this.word |= source.word & (kExclusiveLockBitMask | kSharedLockMaskInWord);
            source.word &= ~(kExclusiveLockBitMask | kSharedLockMaskInWord);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TransferReadLocksFromAndMarkSourceAtomic(ref RecordInfo source, bool seal, bool removeEphemeralLock)
        {
            // This is called when tranferring read locks from the read cache or Lock Table read-only region to a new log record.
            Debug.Assert(!this.IsLockedExclusive, "Must only transfer readlocks (there can be only one exclusive lock so it is handled directly)");
            for (; ; Thread.Yield())
            {
                long expected_word = source.word;
                var new_word = expected_word;

                // Mark the source record atomically with the transfer.
                if (seal)
                    new_word |= kSealedBitMask;
                else
                    new_word &= ~kValidBitMask;

                // If the source record has an ephemeral lock, remove it now.
                if (removeEphemeralLock)
                    new_word -= kSharedLockIncrement;

                // Update the source record; this ensures we atomically transfer the lock count while setting the mark bit.
                // If that succeeds, then we update our own word.
                if (expected_word == Interlocked.CompareExchange(ref source.word, new_word, expected_word))
                {
                    this.word = (new_word & ~kSealedBitMask) | kValidBitMask;
                    return;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpdateAddress(long expectedPrevAddress, long newPrevAddress)
        {
            var expected_word = word;
            RecordInfo newRI = new() { word = expected_word };
            if (newRI.PreviousAddress != expectedPrevAddress)
                return false;
            newRI.PreviousAddress = newPrevAddress;
            return expected_word == Interlocked.CompareExchange(ref this.word, newRI.word, expected_word);
        }

        public bool IsNull() => word == 0;

        public bool Tombstone
        {
            get => (word & kTombstoneBitMask) > 0;
            set
            {
                if (value) word |= kTombstoneBitMask;
                else word &= ~kTombstoneBitMask;
            }
        }

        public bool Valid
        {
            get => (word & kValidBitMask) > 0;
            set
            {
                if (value) word |= kValidBitMask;
                else word &= ~kValidBitMask;
            }
        }

        public bool Sealed => (word & kSealedBitMask) > 0;
        public void Seal() => word |= kSealedBitMask;

        public void ClearDirtyAtomic()
        {
            while (true)
            {
                long expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kDirtyBitMask, expected_word))
                    break;
                Thread.Yield();
            }
        }

        public bool Dirty
        {
            get => (word & kDirtyBitMask) > 0;
            set
            {
                if (value) word |= kDirtyBitMask;
                else word &= ~kDirtyBitMask;
            }
        }

        public bool Modified
        {
            get => (word & kModifiedBitMask) > 0;
            set
            {
                if (value) word |= kModifiedBitMask;
                else word &= ~kModifiedBitMask;
            }
        }

        public bool Filler
        {
            get => (word & kFillerBitMask) > 0;
            set
            {
                if (value) word |= kFillerBitMask;
                else word &= ~kFillerBitMask;
            }
        }

        public bool InNewVersion
        {
            get => (word & kInNewVersionBitMask) > 0;
            set
            {
                if (value) word |= kInNewVersionBitMask;
                else word &= ~kInNewVersionBitMask;
            }
        }

        public void SetDirtyAndModified() => word |= kDirtyBitMask | kModifiedBitMask;
        public void SetModified() => word |= kModifiedBitMask;
        public void ClearModified() => word &= (~kModifiedBitMask);
        public void SetDirty() => word |= kDirtyBitMask;
        public void SetTombstone() => word |= kTombstoneBitMask;
        public void SetValid() => word |= kValidBitMask;
        public void SetInvalid() => word &= ~kValidBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalidAtomic()
        {
            while (true)
            {
                long expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kValidBitMask, expected_word))
                    return;
                Thread.Yield();
            }
        }

        public bool Invalid => (word & kValidBitMask) == 0;

        public bool SkipOnScan => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;

        /// <summary>
        /// Indicates whether this RecordInfo is a valid source for updates or record locks.
        /// </summary>
        public bool IsValidUpdateOrLockSource => (word & (kValidBitMask | kSealedBitMask)) == kValidBitMask;

        public long PreviousAddress
        {
            get => word & kPreviousAddressMaskInWord;
            set
            {
                word &= ~kPreviousAddressMaskInWord;
                word |= value & kPreviousAddressMaskInWord;
            }
        }

        public bool PreviousAddressIsReadCache => (this.word & Constants.kReadCacheBitMask) != 0;
        public long AbsolutePreviousAddress => AbsoluteAddress(this.PreviousAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength() => kTotalSizeInBytes;

        internal bool Unused1
        { 
            get => (word & kUnused1BitMask) != 0; 
            set => word = value ? word | kUnused1BitMask : word & ~kUnused1BitMask;
        }

        internal bool Unused2
        {
            get => (word & kUnused2BitMask) != 0;
            set => word = value ? word | kUnused2BitMask : word & ~kUnused2BitMask;
        }

        public override string ToString()
        {
            var paRC = this.PreviousAddressIsReadCache ? "(rc)" : string.Empty;
            var locks = $"{(this.IsLockedExclusive ? "x" : string.Empty)}{this.NumLockedShared}";
            static string bstr(bool value) => value ? "T" : "F";
            return $"prev {this.AbsolutePreviousAddress}{paRC}, locks {locks}, valid {bstr(Valid)}, mod {bstr(Modified)},"
                 + $" tomb {bstr(Tombstone)}, seal {bstr(Sealed)}, dirty {bstr(Dirty)}, fill {bstr(Filler)}, Un1 {bstr(Unused2)}, Un2 {bstr(Unused1)}";
        }
    }
}

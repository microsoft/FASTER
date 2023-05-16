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
    // [Unused1][Modified][InNewVersion][Filler][Dirty][Unused2][Sealed][Valid][Tombstone][X][SSSSSS] [RAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where X = exclusive lock, S = shared lock, R = readcache, A = address
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Previous address
        internal const int kPreviousAddressBits = 48;
        internal const long kPreviousAddressMaskInWord = (1L << kPreviousAddressBits) - 1;

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

        // Other marker bits. Unused* means bits not yet assigned; use the highest number when assigning
        const int kTombstoneBitOffset = kExclusiveLockBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kSealedBitOffset = kValidBitOffset + 1;
        const int kUnused2BitOffset = kSealedBitOffset + 1;
        const int kDirtyBitOffset = kUnused2BitOffset + 1;
        const int kFillerBitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kFillerBitOffset + 1;
        const int kModifiedBitOffset = kInNewVersionBitOffset + 1;
        internal const int kUnused1BitOffset = kModifiedBitOffset + 1;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kUnused2BitMask = 1L << kUnused2BitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;
        const long kFillerBitMask = 1L << kFillerBitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;
        const long kModifiedBitMask = 1L << kModifiedBitOffset;
        internal const long kUnused1BitMask = 1L << kUnused1BitOffset;

        [FieldOffset(0)]
        private long word;

        public void WriteInfo(bool inNewVersion, bool tombstone, long previousAddress)
        {
            this.word = default;
            this.Tombstone = tombstone;
            this.SetValid();
            this.PreviousAddress = previousAddress;
            this.IsInNewVersion = inNewVersion;
        }

        public bool Equals(RecordInfo other) => this.word == other.word;

        public long GetHashCode64() => Utility.GetHashCode(this.word);

        public bool IsLockedExclusive => (word & kExclusiveLockBitMask) != 0;
        public bool IsLockedShared => NumLockedShared != 0;
        public bool IsLocked => IsLockedExclusive || IsLockedShared;

        public byte NumLockedShared => (byte)((word & kSharedLockMaskInWord) >> kLockShiftInWord);

        // We ignore locks and temp bits for disk images
        public void ClearBitsForDiskImages()
        {
            // Locks can be evicted even with ephemeral locks, if the record is locked when BlockAllocate allows an epoch refresh
            // that sends it below HeadAddress. Sealed records are normal. In Pending IO completions, Ephemeral locking does not
            // lock records read from disk (they should be found in memory). But a Sealed record may become current again during
            // recovery, if the RCU-inserted record was not written to disk during a crash, etc. So clear these bits here.
            word &= ~(kLockBitMask | kDirtyBitMask | kSealedBitMask);
        }

        private static bool IsClosedWord(long word) => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;

        public bool IsClosed => IsClosedWord(word);
        private bool IsSealed => (this.word & kSealedBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeLockShared() => this.word += kSharedLockIncrement;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeLockExclusive() => this.word |= kExclusiveLockBitMask;

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="TryLockExclusive"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusive()
        {
            Debug.Assert(!IsLockedShared, "Trying to X unlock an S locked record");
            Debug.Assert(IsLockedExclusive, "Trying to X unlock an unlocked record");
            Debug.Assert(!IsSealed, "Trying to X unlock a Sealed record");
            word &= ~kExclusiveLockBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="TryLockExclusive"/>, and which is a source that has become Sealed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusiveAndSeal()
        {
            Debug.Assert(!IsLockedShared, "Trying to X unlock an S locked record");
            Debug.Assert(IsLockedExclusive, "Trying to X unlock an unlocked record");
            Debug.Assert(!IsSealed, "Trying to X unlock a Sealed record");
            word = (word & ~kExclusiveLockBitMask) | kSealedBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Seal this record (currently only called to prepare it for inline revivification).
        /// </summary>
        public bool TrySeal()
        {
            // If this fails for any reason it means another record is trying to modify (perhaps revivify) it, so return false to RETRY_LATER.
            long expected_word = word;
            return !IsClosedWord(expected_word) && expected_word == Interlocked.CompareExchange(ref word, expected_word | kSealedBitMask, expected_word);
        }

        /// <summary>
        /// Unseal this record that was previously sealed for revivification.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Unseal()
        {
            // Do not assert IsSealed; we only unseal in a short scope in revivification when not using LockTable, and this saves us an "if" when calling this.
            Debug.Assert(Valid, "Record should be valid");
            word &= ~kSealedBitMask;
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
                Debug.Assert(!IsClosedWord(expected_word), "Should not be X locking closed records, pt 1");
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
                    // Someone else may have closed the record while we were draining reads.
                    Debug.Assert(!IsClosedWord(this.word), "Should not be X locking readcache records, pt 2");
                    return true;
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
            // X *and* S locks means an X lock is still trying to drain readers, like this one.
            Debug.Assert((word & kLockBitMask) != kExclusiveLockBitMask, "Trying to S unlock an X-only locked record");
            Debug.Assert(IsLockedShared, "Trying to S unlock an unlocked record");
            Debug.Assert(!IsSealed, "Trying to S unlock a Sealed record");
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
                Debug.Assert(!IsClosedWord(expected_word), "Should not be S locking closed records");
                if (((expected_word & kExclusiveLockBitMask) == 0) // not exclusively locked
                    && (expected_word & kSharedLockMaskInWord) != kSharedLockMaskInWord) // shared lock is not full
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word + kSharedLockIncrement, expected_word))
                        return true;
                }
                if (spinCount > 0 && --spinCount <= 0) 
                    return false;
            }
        }

        /// <summary>
        /// Try to reset the modified bit of the RecordInfo
        /// </summary>
        /// <returns>Whether the modified bit was reset successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryResetModifiedAtomic()
        {
            int spinCount = Constants.kMaxLockSpins;
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if ((expected_word & kModifiedBitMask) == 0)
                    return true;
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & (~kModifiedBitMask), expected_word))
                    return true;
                if (spinCount > 0 && --spinCount <= 0)
                    return false;
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

        public void ClearDirtyAtomic()
        {
            for (; ; Thread.Yield())
            {
                long expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kDirtyBitMask, expected_word))
                    break;
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

        public bool IsInNewVersion
        {
            get => (word & kInNewVersionBitMask) > 0;
            set
            {
                if (value) word |= kInNewVersionBitMask;
                else word &= ~kInNewVersionBitMask;
            }
        }

        public void SetDirtyAndModified() => word |= kDirtyBitMask | kModifiedBitMask;
        public void SetDirty() => word |= kDirtyBitMask;
        public void SetTombstone() => word |= kTombstoneBitMask;
        public void SetValid() => word |= kValidBitMask;
        public void SetInvalid() => word &= ~(kValidBitMask | kExclusiveLockBitMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalidAtomic()
        {
            for (; ; Thread.Yield())
            {
                long expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kValidBitMask, expected_word))
                    return;
            }
        }

        public bool Invalid => (word & kValidBitMask) == 0;

        public bool SkipOnScan => IsClosedWord(word);

        public long PreviousAddress
        {
            get => word & kPreviousAddressMaskInWord;
            set
            {
                word &= ~kPreviousAddressMaskInWord;
                word |= value & kPreviousAddressMaskInWord;
            }
        }

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
            var paRC = IsReadCache(this.PreviousAddress) ? "(rc)" : string.Empty;
            var locks = $"{(this.IsLockedExclusive ? "x" : string.Empty)}{this.NumLockedShared}";
            static string bstr(bool value) => value ? "T" : "F";
            return $"prev {AbsoluteAddress(this.PreviousAddress)}{paRC}, locks {locks}, valid {bstr(Valid)}, tomb {bstr(Tombstone)}, seal {bstr(this.IsSealed)},"
                 + $" mod {bstr(Modified)}, dirty {bstr(Dirty)}, fill {bstr(Filler)}, Un1 {bstr(Unused1)}, Un2 {bstr(Unused2)}";
        }
    }
}

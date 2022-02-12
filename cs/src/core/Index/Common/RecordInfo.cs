// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    // RecordInfo layout (64 bits total):
    // [--][InNewVersion][Filler][Dirty][Tentative][Sealed] [Valid][Tombstone][X][SSSSSS] [RAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where X = exclusive lock, S = shared lock, R = readcache, A = address, - = unused
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
        const int kSharedLockBits = 6;
        const int kExclusiveLockBits = 1;

        // Shared lock constants
        const long kSharedLockMaskInWord = ((1L << kSharedLockBits) - 1) << kLockShiftInWord;
        const long kSharedLockIncrement = 1L << kLockShiftInWord;

        // Exclusive lock constants
        const int kExclusiveLockBitOffset = kLockShiftInWord + kSharedLockBits;
        const long kExclusiveLockBitMask = 1L << kExclusiveLockBitOffset;

        // Other marker bits
        const int kTombstoneBitOffset = kExclusiveLockBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kTentativeBitOffset = kValidBitOffset + 1;
        const int kSealedBitOffset = kTentativeBitOffset + 1;
        const int kDirtyBitOffset = kSealedBitOffset + 1;
        const int kFillerBitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kFillerBitOffset + 1;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kTentativeBitMask = 1L << kTentativeBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;
        const long kFillerBitMask = 1L << kFillerBitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;

        [FieldOffset(0)]
        private long word;

        public static void WriteInfo(ref RecordInfo info, bool inNewVersion, bool tombstone, bool dirty, long previousAddress)
        {
            info.word = default;
            info.Tombstone = tombstone;
            info.SetValid();
            info.Dirty = dirty;
            info.PreviousAddress = previousAddress;
            info.InNewVersion = inNewVersion;
        }

        public bool Equals(RecordInfo other) => this.word == other.word;

        public bool IsLocked => (word & (kExclusiveLockBitMask | kSharedLockMaskInWord)) != 0;

        public bool IsLockedExclusive => (word & kExclusiveLockBitMask) != 0;

        public byte NumLockedShared => (byte)((word & kSharedLockMaskInWord) >> kLockShiftInWord);

        public void ClearLocks() => word &= ~(kExclusiveLockBitMask | kSharedLockMaskInWord);

        public bool IsIntermediate => IsIntermediateWord(word);

        private static bool IsIntermediateWord(long word) => (word & (kTentativeBitMask | kSealedBitMask)) != 0;

        /// <summary>
        /// Take exclusive (write) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LockExclusive() => TryLockExclusive(spinCount: -1);

        // For new records, which don't need the Interlocked overhead.
        internal void SetLockExclusiveBit() => this.word |= kExclusiveLockBitMask;

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="LockExclusive"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusive()
        {
            Debug.Assert(IsLockedExclusive, "Trying to X unlock an unlocked record");
            word &= ~kExclusiveLockBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Try to take an exclusive (write) lock on RecordInfo
        /// </summary>
        /// <param name="spinCount">Number of attempts before giving up</param>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockExclusive(int spinCount = 1)
        {
            // Acquire exclusive lock (readers may still be present; we'll drain them later)
            while (true)
            {
                long expected_word = word;
                if (IsIntermediateWord(expected_word))
                    return false;
                if ((expected_word & kExclusiveLockBitMask) == 0)
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word | kExclusiveLockBitMask, expected_word))
                        break;
                }
                if (spinCount > 0 && --spinCount <= 0) return false;
                Thread.Yield();
            }

            // Wait for readers to drain
            while ((word & kSharedLockMaskInWord) != 0) Thread.Yield();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockExclusiveRaw()
        {
            // Acquire exclusive lock, without spin limit or considering Intermediate state
            while (true)
            {
                long expected_word = word;
                if ((expected_word & kExclusiveLockBitMask) == 0)
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word | kExclusiveLockBitMask, expected_word))
                        break;
                }
                Thread.Yield();
            }

            // Wait for readers to drain
            while ((word & kSharedLockMaskInWord) != 0) Thread.Yield();
        }

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LockShared() => TryLockShared(spinCount: -1);

        /// <summary>
        /// Unlock RecordInfo that was previously locked for shared access, via <see cref="LockShared"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockShared()
        {
            Debug.Assert((word & kSharedLockMaskInWord) != 0, "Trying to S unlock an unlocked record");
            Interlocked.Add(ref word, -kSharedLockIncrement);
        }

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        /// <param name="spinCount">Number of attempts before giving up</param>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockShared(int spinCount = 1)
        {
            // Acquire shared lock
            while (true)
            {
                long expected_word = word;
                if (IsIntermediateWord(expected_word))
                    return false;
                if (((expected_word & kExclusiveLockBitMask) == 0) // not exclusively locked
                    && (expected_word & kSharedLockMaskInWord) != kSharedLockMaskInWord) // shared lock is not full
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word + kSharedLockIncrement, expected_word))
                        break;
                }
                if (spinCount > 0 && --spinCount <= 0) return false;
                Thread.Yield();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool LockSharedRaw(int spinCount = 1)
        {
            // Acquire shared lock, without spin limit or considering Intermediate state
            while (true)
            {
                long expected_word = word;
                if (((expected_word & kExclusiveLockBitMask) == 0) // not exclusively locked
                    && (expected_word & kSharedLockMaskInWord) != kSharedLockMaskInWord) // shared lock is not full
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word + kSharedLockIncrement, expected_word))
                        break;
                }
                Thread.Yield();
            }
            return true;
        }

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool LockExclusiveFromShared() => TryLockExclusiveFromShared(spinCount: -1);

        /// <summary>
        /// Promote a shared (read) lock on RecordInfo to exclusive
        /// </summary>
        /// <param name="spinCount">Number of attempts before giving up</param>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockExclusiveFromShared(int spinCount = 1)
        {
            // Acquire shared lock
            while (true)
            {
                // Even though we own the lock here, it might be in the process of eviction, which seals it
                long expected_word = word;
                if (IsIntermediateWord(expected_word))
                    return false;
                if ((expected_word & kExclusiveLockBitMask) == 0) // not exclusively locked
                {
                    var new_word = expected_word | kExclusiveLockBitMask;
                    if ((expected_word & kSharedLockMaskInWord) != 0) // shared lock is not empty
                        new_word -= kSharedLockIncrement;
                    else
                        Debug.Fail($"SharedLock count should not be 0");
                    if (expected_word == Interlocked.CompareExchange(ref word, new_word, expected_word))
                        break;
                }
                if (spinCount > 0 && --spinCount <= 0) return false;
                Thread.Yield();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyLocksFrom(RecordInfo other)
        {
            word &= ~(kExclusiveLockBitMask | kSharedLockMaskInWord);
            word |= other.word & (kExclusiveLockBitMask | kSharedLockMaskInWord);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TransferLocksFrom(ref RecordInfo other)
        {
            this.CopyLocksFrom(other);
            other.ClearLocks();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpdateAddress(long newPrevAddress)
        {
            var expectedWord = word;
            RecordInfo newRI = new() { word = word };
            newRI.PreviousAddress = newPrevAddress;
            var foundWord = Interlocked.CompareExchange(ref this.word, newRI.word, expectedWord);
            return foundWord == expectedWord;
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
        }

        public bool Tentative
        {
            get => (word & kTentativeBitMask) > 0;
            set
            {
                if (value) word |= kTentativeBitMask;
                else word &= ~kTentativeBitMask;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetTentativeAtomic(bool value)
        {
            // Call this when locking may be done simultaneously
            while (this.Tentative != value)
            {
                long expected_word = word;
                long new_word = value ? (word | kTentativeBitMask) : (word & ~kTentativeBitMask);
                long current_word = Interlocked.CompareExchange(ref word, new_word, expected_word);
                if (expected_word == current_word)
                    return;

                // Tentative records should not be operated on by other threads.
                Debug.Assert((word & kSealedBitMask) == 0 && !this.Invalid);
                Thread.Yield();
            }
        }

        public bool Sealed => (word & kSealedBitMask) > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Seal(bool manualLocking = false)
        {
            // If manualLocking, we own this lock or are transferring to the Lock Table, so just set the sealed bit.
            long sealBits = manualLocking ? kSealedBitMask : kExclusiveLockBitMask | kSealedBitMask;
            while (true)
            {
                long expected_word = word;

                // If someone else sealed this, we fail this attempt.
                if (IsIntermediateWord(expected_word) || ((expected_word & kValidBitMask) == 0))
                    return false;
                if ((expected_word & sealBits) == 0)
                {
                    long new_word = word | sealBits;
                    long current_word = Interlocked.CompareExchange(ref word, new_word, expected_word);
                    if (expected_word == current_word)
                    {
                        // (Lock+)Seal succeeded; remove lock if not doing manual locking
                        if (!manualLocking)
                            this.UnlockExclusive();
                        return true;
                    }

                    if ((word & kSealedBitMask) > 0 || this.Invalid)
                        return false;
                }
                Thread.Yield();
            }
        }

        internal void Unseal()
        {
            Debug.Assert(Sealed, "Record should have been sealed");
            word &= ~kSealedBitMask;
        }

        internal void UnsealIfSealed() => word &= ~kSealedBitMask;

        public bool DirtyAtomic
        {
            set
            {
                while (true)
                {
                    long expected_word = word;
                    long new_word = value ? (word | kDirtyBitMask) : (word & ~kDirtyBitMask);
                    if (expected_word == Interlocked.CompareExchange(ref word, new_word, expected_word))
                        break;
                    Thread.Yield();
                }
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

        public void SetDirty() => word |= kDirtyBitMask;
        public void SetTombstone() => word |= kTombstoneBitMask;
        public void SetValid() => word |= kValidBitMask;
        public void SetInvalid() => word &= ~(kValidBitMask | kTentativeBitMask);

        public bool Invalid => (word & kValidBitMask) == 0;

        public bool SkipOnScan => Invalid || (word & (kSealedBitMask | kTentativeBitMask)) != 0;

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
        public static int GetLength()
        {
            return kTotalSizeInBytes;
        }

        public override string ToString() => word.ToString();
    }
}

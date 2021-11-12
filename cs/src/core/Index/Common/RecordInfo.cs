// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    // RecordInfo layout (64 bits total):
    // [--][InNewVersion][Filler][Dirty][Stub][Sealed] [Valid][Tombstone][X][SSSSSS] [RAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where X = exclusive lock, S = shared lock, R = readcache, A = address, - = unused
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
        const int kSharedLockBits = 6;
        const int kExlcusiveLockBits = 1;

        // Shared lock constants
        const long kSharedLockMaskInWord = ((1L << kSharedLockBits) - 1) << kLockShiftInWord;
        const long kSharedLockIncrement = 1L << kLockShiftInWord;

        // Exclusive lock constants
        const int kExclusiveLockBitOffset = kLockShiftInWord + kSharedLockBits;
        const long kExclusiveLockBitMask = 1L << kExclusiveLockBitOffset;

        // Other marker bits
        const int kTombstoneBitOffset = kExclusiveLockBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kStubBitOffset = kValidBitOffset + 1;
        const int kSealedBitOffset = kStubBitOffset + 1;
        const int kDirtyBitOffset = kSealedBitOffset + 1;
        const int kFillerBitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kFillerBitOffset + 1;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kStubBitMask = 1L << kStubBitOffset;
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

        /// <summary>
        /// Take exclusive (write) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LockExclusive() => TryLockExclusive(spinCount: -1);

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="LockExclusive"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusive()
        {
            Debug.Assert((word & kExclusiveLockBitMask) != 0);
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

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LockShared() => TryLockShared(spinCount: -1);

        /// <summary>
        /// Unlock RecordInfo that was previously locked for shared access, via <see cref="LockShared"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockShared()
        {
            Debug.Assert((word & kSharedLockMaskInWord) != 0);
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

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LockExclusiveFromShared() => TryLockExclusiveFromShared(spinCount: -1);

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
                long expected_word = word;
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

        public bool Stub
        {
            get => (word & kStubBitMask) > 0;
            set
            {
                if (value) word |= kStubBitMask;
                else word &= ~kStubBitMask;
            }
        }

        public bool Sealed
        {
            get => (word & kSealedBitMask) > 0;
            set
            {
                if (value) word |= kSealedBitMask;
                else word &= ~kSealedBitMask;
            }
        }

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
        public void SetInvalid() => word &= ~kValidBitMask;

        public bool Invalid => (word & kValidBitMask) == 0;

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

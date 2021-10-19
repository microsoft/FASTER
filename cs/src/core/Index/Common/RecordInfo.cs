// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    // RecordInfo layout (64 bits total):
    // [VVVVV][Dirty][Stub][Sealed] [Valid][Tombstone][X][SSSSS] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where V = version, X = exclusive lock, S = shared lock, A = address
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Previous address
        const int kPreviousAddressBits = 48;
        const long kPreviousAddressMaskInWord = (1L << kPreviousAddressBits) - 1;

        // Shift position of lock in word
        const int kLockShiftInWord = 48;

        // We use 6 lock bits: 5 shared lock bits + 1 exclusive lock bit
        const int kSharedLockBits = 5;
        const int kExlcusiveLockBits = 1;

        // Shared lock constants
        const long kSharedLockMaskInWord = ((1L << kSharedLockBits) - 1) << kLockShiftInWord;
        const long kSharedLockIncrement = 1L << kLockShiftInWord;

        // Exclusive lock constants
        const int kExclusiveLockBitOffset = 53;
        const long kExclusiveLockBitMask = 1L << kExclusiveLockBitOffset;

        // Other marker bits
        const int kTombstoneBitOffset = 54;
        const int kValidBitOffset = 55;
        const int kStubBitOffset = 56;
        const int kSealedBitOffset = 57;
        const int kDirtyBitOffset = 58;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kStubBitMask = 1L << kStubBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;

        // Shift position of version in word
        const int kVersionShiftInWord = 59;

        // We use 5 version bits
        const int kVersionBits = 5;

        // Version constants
        const long kVersionMaskInWord = ((1L << kVersionBits) - 1) << kVersionShiftInWord;
        internal const long kVersionMaskInInteger = (1L << kVersionBits) - 1;

        [FieldOffset(0)]
        private long word;

        public static void WriteInfo(ref RecordInfo info, int checkpointVersion, bool tombstone, bool invalidBit, long previousAddress)
        {
            info.word = default;
            info.Tombstone = tombstone;
            info.Invalid = invalidBit;
            info.PreviousAddress = previousAddress;
            info.Version = checkpointVersion;
        }

        /// <summary>
        /// Take exclusive (write) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LockExclusive()
        {
            // Acquire exclusive lock (readers may still be present)
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
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="LockExclusive"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusive() => word &= ~kExclusiveLockBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LockShared()
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
                Thread.Yield();
            }
        }

        /// <summary>
        /// Unlock RecordInfo that was previously locked for shared access, via <see cref="LockShared"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockShared() => Interlocked.Add(ref word, -kSharedLockIncrement);

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
            get => (word & kTombstoneBitMask) > 0;
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

        public bool Dirty
        {
            get => (word & kDirtyBitMask) > 0;
            set
            {
                if (value) word |= kDirtyBitMask;
                else word &= ~kDirtyBitMask;
            }
        }

        public bool Invalid
        {
            get => !((word & kValidBitMask) > 0);
            set
            {
                if (value) word &= ~kValidBitMask;
                else word |= kValidBitMask;
            }
        }

        public int Version
        {
            get => (int)((word & kVersionMaskInWord) >> kVersionShiftInWord);
            set
            {
                word &= ~kVersionMaskInWord;
                word |= (value & kVersionMaskInInteger) << kVersionShiftInWord;
            }
        }

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
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetShortVersion(long version) => (int) (version & kVersionMaskInInteger);

        public override string ToString() => word.ToString();
    }
}

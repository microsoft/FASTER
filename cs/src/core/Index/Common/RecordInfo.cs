// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

//#define RECORD_INFO_WITH_PIN_COUNT

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
#if RECORD_INFO_WITH_PIN_COUNT
    [StructLayout(LayoutKind.Explicit, Size = 12)]
#else
    [StructLayout(LayoutKind.Explicit, Size = 8)]
#endif
    public unsafe struct RecordInfo
    {
        public const int kFinalBitOffset = 48;

        public const int kTombstoneBitOffset = 49;

        public const int kInvalidBitOffset = 50;

        public const int kVersionBits = 13;

        public const int kVersionShiftInWord = 51;

        public const long kVersionMaskInWord = ((1L << kVersionBits) - 1) << kVersionShiftInWord;

        public const long kVersionMaskInInteger = (1L << kVersionBits) - 1;

        public const long kPreviousAddressMask = (1L << 48) - 1;

        public const long kFinalBitMask = (1L << kFinalBitOffset);

        public const long kTombstoneMask = (1L << kTombstoneBitOffset);

        public const long kInvalidBitMask = (1L << kInvalidBitOffset);

#if RECORD_INFO_WITH_PIN_COUNT
        public const int kTotalSizeInBytes = sizeof(long) + sizeof(int);

        public const int kTotalBits = kTotalSizeInBytes * 8;

        [FieldOffset(0)]
        private long word;

        [FieldOffset(sizeof(long))]
        private int access_data;

        public static void WriteInfo(RecordInfo* info, int checkpointVersion, bool final, bool tombstone, bool invalidBit, long previousAddress)
        {
            info->word = default(long);
            info->Final = final;
            info->Tombstone = tombstone;
            info->Invalid = invalidBit;
            info->PreviousAddress = previousAddress;
            info->Version = checkpointVersion;
            info->access_data = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPin()
        {
            return Interlocked.Increment(ref access_data) > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryMarkReadOnly()
        {
            return Interlocked.CompareExchange(ref access_data, int.MinValue, 0) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MarkReadOnly()
        {
            var found_value = Interlocked.CompareExchange(ref access_data, int.MinValue, 0);
            if (found_value != 0)
            {
                int num_iterations = 1000;
                Thread.SpinWait(num_iterations);
                while (Interlocked.CompareExchange(ref access_data, int.MinValue, 0) != 0)
                {
                    Thread.SpinWait(num_iterations);
                    num_iterations <<= 1;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unpin()
        {
            Interlocked.Decrement(ref access_data);
        }

#else
        public const int kTotalSizeInBytes = sizeof(long);

        public const int kTotalBits = kTotalSizeInBytes * 8;

        [FieldOffset(0)]
        private long word;

        public static void WriteInfo(ref RecordInfo info, int checkpointVersion, bool final, bool tombstone, bool invalidBit, long previousAddress)
        {
            info.word = default(long);
            info.Final = final;
            info.Tombstone = tombstone;
            info.Invalid = invalidBit;
            info.PreviousAddress = previousAddress;
            info.Version = checkpointVersion;
        }
        

        public static string ToString(RecordInfo* info)
        {
            return "RecordHeader Word = " + info->word;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPin()
        {
            throw new InvalidOperationException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryMarkReadOnly()
        {
            throw new InvalidOperationException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MarkReadOnly()
        {
            throw new InvalidOperationException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unpin()
        {
            throw new InvalidOperationException();
        }
#endif
        public bool IsNull()
        {
            return word == 0;
        }

        public bool Tombstone
        {
            get
            {
                return (word & kTombstoneMask) > 0;
            }

            set
            {
                if (value)
                {
                    word |= kTombstoneMask;
                }
                else
                {
                    word &= ~kTombstoneMask;
                }
            }
        }

        public bool Final
        {
            get
            {
                return (word & kFinalBitMask) > 0;
            }
            set
            {
                if (value)
                {
                    word |= kFinalBitMask;
                }
                else
                {
                    word &= ~kFinalBitMask;
                }
            }
        }

        public bool Invalid
        {
            get
            {
                return !((word & kInvalidBitMask) > 0);
            }
            set
            {
                if (value)
                {
                    word &= ~kInvalidBitMask; 
                }
                else
                {
                    word |= kInvalidBitMask;
                }
            }
        }

        public int Version
        {
            get
            {
                return (int)(((word & kVersionMaskInWord) >> kVersionShiftInWord) & kVersionMaskInInteger);
            }
            set
            {
                word &= ~kVersionMaskInWord;
                word |= ((value & kVersionMaskInInteger) << kVersionShiftInWord);
            }
        }

        public long PreviousAddress
        {
            get
            {
                return (word & kPreviousAddressMask);
            }
            set
            {
                word &= ~kPreviousAddressMask;
                word |= (value & kPreviousAddressMask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength()
        {
            return kTotalSizeInBytes;
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct AddressInfo
    {
        public const int kAddressBitOffset = 48;

        public const int kSizeBits = 13;

        public const int kSizeShiftInWord = 49;

        public const long kSizeMaskInWord = ((1L << kSizeBits) - 1) << kSizeShiftInWord;

        public const long kSizeMaskInInteger = (1L << kSizeBits) - 1;

        public const long kAddressMask = (1L << kAddressBitOffset) - 1;

        public const long kDiskBitMask = (1L << kAddressBitOffset);

        public const int kTotalSizeInBytes = sizeof(long);

        public const int kTotalBits = kTotalSizeInBytes * 8;

        [FieldOffset(0)]
        private long word;

        public static void WriteInfo(AddressInfo* info, long address, bool isDiskAddress = false, int size = 0)
        {
            info->word = default(long);
            info->IsDiskAddress = isDiskAddress;
            info->Address = address;
            info->Size = size;
        }

        public static string ToString(AddressInfo* info)
        {
            return "RecordHeader Word = " + info->word;
        }

        public bool IsDiskAddress
        {
            get
            {
                return (word & kDiskBitMask) > 0;
            }

            set
            {
                if (value)
                {
                    word |= kDiskBitMask;
                }
                else
                {
                    word &= ~kDiskBitMask;
                }
            }
        }

        public int Size
        {
            get
            {
                return (int)(((word & kSizeMaskInWord) >> kSizeShiftInWord) & kSizeMaskInInteger);
            }
            set
            {
                word &= ~kSizeMaskInWord;
                word |= ((value & kSizeMaskInInteger) << kSizeShiftInWord);
            }
        }

        public long Address
        {
            get
            {
                return (word & kAddressMask);
            }
            set
            {
                word &= ~kAddressMask;
                word |= (value & kAddressMask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength()
        {
            return kTotalSizeInBytes;
        }
    }
}

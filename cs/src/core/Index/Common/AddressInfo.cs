// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct AddressInfo
    {
        public const int kTotalBits = sizeof(long) * 8;

        public const int kAddressBits = 42;
        public const int kSizeBits = 21;
        public const int kMultiplierBits = kTotalBits - kAddressBits - kSizeBits;

        public const long kSizeMaskInWord = ((1L << kSizeBits) - 1) << kAddressBits;
        public const long kSizeMaskInInteger = (1L << kSizeBits) - 1;

        public const long kMultiplierMaskInWord = ((1L << kMultiplierBits) - 1) << (kAddressBits + kSizeBits);
        public const long kMultiplierMaskInInteger = (1L << kMultiplierBits) - 1;

        public const long kAddressMask = (1L << kAddressBits) - 1;


        [FieldOffset(0)]
        private long word;

        public static void WriteInfo(AddressInfo* info, long address, long size)
        {
            info->word = default(long);
            info->Address = address;
            info->Size = size;
        }

        public static string ToString(AddressInfo* info)
        {
            return "RecordHeader Word = " + info->word;
        }

        public long Size
        {
            get
            {
                int multiplier = (int)(((word & kMultiplierMaskInWord) >> (kAddressBits + kSizeBits)) & kMultiplierMaskInInteger);
                return (multiplier == 0 ? 512 : 1<<20)*(((word & kSizeMaskInWord) >> kAddressBits) & kSizeMaskInInteger);
            }
            set
            {
                int multiplier = 0;
                int val = (int)(value >> 9);
                if ((value & ((1<<9)-1)) != 0) val++;

                if (val >= (1 << kSizeBits))
                {
                    val = (int)(value >> 20);
                    if ((value & ((1<<20) - 1)) != 0) val++;
                    multiplier = 1;
                    if (val >= (1 << kSizeBits))
                    {
                        throw new Exception("Unsupported object size: " + value);
                    }
                }
                word &= ~kSizeMaskInWord;
                word &= ~kMultiplierMaskInWord;

                word |= (val & kSizeMaskInInteger) << kAddressBits;
                word |= (multiplier & kMultiplierMaskInInteger) << (kAddressBits + kSizeBits);
            }
        }

        public long Address
        {
            get
            {
                return word & kAddressMask;
            }
            set
            {
                word &= ~kAddressMask;
                word |= (value & kAddressMask);
            }
        }
    }
}

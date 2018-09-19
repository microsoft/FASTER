// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define EIGHT_BYTE_KEY 
//#define FIXED_SIZE_KEY
//#define VARIABLE_SIZE_KEY

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.core
{
#if FIXED_SIZE_KEY
    [StructLayout(LayoutKind.Explicit, Size = kSizeInBytes)]
    public unsafe struct Key
    {
        public const int kNumFields = 1;

        public const int kSizeInBytes = kNumFields * sizeof(long);

        [FieldOffset(0)]
        public fixed long values[kNumFields];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsEqual(Key* k1, Key* k2)
        {
            return Utility.IsEqual((byte*)k1, (byte*)k2, kSizeInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetHash(Key* key)
        {
            return Utility.HashBytes((byte*)key, kSizeInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(Key* src, Key* dst)
        {
            Utility.Copy((byte*)src, (byte*)dst, kSizeInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLogicalSize(Key* key)
        {
            return kSizeInBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetPhysicalSize(Key* key)
        {
            return kSizeInBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EstimateAveragePhysicalSize()
        {
            return kSizeInBytes;
        }
    }
#elif EIGHT_BYTE_KEY
    [StructLayout(LayoutKind.Explicit, Size = kSizeInBytes)]
    public unsafe struct Key
    {
        public const int kSizeInBytes = sizeof(long);

        [FieldOffset(0)]
        public long value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public new long GetHashCode()
        {
            return Utility.GetHashCode(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Equals(Key* k1, Key* k2)
        {
            return k1->value == k2->value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetHashCode(Key* key)
        {
            return Utility.GetHashCode(*((long*)key));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(Key* src, Key* dst)
        {
            dst->value = src->value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(Key* key)
        {
            return kSizeInBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EstimateAveragePhysicalSize()
        {
            return kSizeInBytes;
        }
        
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(Key* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(Key* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }

        public static void Free(Key* key)
        {
            throw new InvalidOperationException();
        }

        public static Key* MoveToContext(Key* key)
        {
            return key;
        }
    }
#elif VARIABLE_SIZE_KEY
    public unsafe struct Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsEqual(Key* k1, Key* k2)
        {
            return Utility.IsEqual((byte*)k1, (byte*)k2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetHash(Key* key)
        {
            return Utility.HashBytes((byte*)key, GetPhysicalSize(key));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(Key* src, Key* dst)
        {
            Utility.Copy((byte*)src, (byte*)dst, GetPhysicalSize(src));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLogicalSize(Key* key)
        {
            return *((int*)key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetPhysicalSize(Key* key)
        {
            return GetLogicalSize(key) + sizeof(int);
        }
    }
#endif
}

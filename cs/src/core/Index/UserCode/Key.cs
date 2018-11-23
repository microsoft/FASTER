// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

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
    public unsafe struct Key : IKey<Key>
    {
        public const int kSizeInBytes = sizeof(long);

        [FieldOffset(0)]
        public long value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64()
        {
            return Utility.GetHashCode(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ref Key k2)
        {
            return value == k2.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShallowCopy(ref Key dst)
        {
            dst = this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return kSizeInBytes;
        }

        public bool HasObjectsToSerialize()
        {
            return false;
        }

        public void Serialize(Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public void Deserialize(Stream fromStream)
        {
            throw new InvalidOperationException();
        }

        public void Free()
        {
            throw new InvalidOperationException();
        }

        public ref Key MoveToContext(ref Key key)
        {
            return ref key;
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

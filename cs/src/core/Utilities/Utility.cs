// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Security;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.Win32.SafeHandles;
using System.Diagnostics;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Empty type
    /// </summary>
    public struct Empty
    {
        /// <summary>
        /// Default
        /// </summary>
        public static readonly Empty Default = default(Empty);
    }

    /// <summary>
    /// FASTER utility functions
    /// </summary>
    public static class Utility
    {
        /// <summary>
        /// Get size of type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static unsafe int GetSize<T>(this T value)
        {
            T[] arr = new T[2];
            return (int)((long)Unsafe.AsPointer(ref arr[1]) - (long)Unsafe.AsPointer(ref arr[0]));
        }

        /// <summary>
        /// Is type blittable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static bool IsBlittable<T>()
        {
            if (default(T) == null)
                return false;

            try
            {
                var tmp = new T[1];
                var h = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                h.Free();
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Helper function used to check if two byte arrays are equal
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dest"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static bool IsEqual(byte* src, byte* dest)
        {
            if (*(int*)src == *(int*)dest)
            {
                for (int i = 0; i < *(int*)src; i++)
                {
                    if (*(src + 4 + i) != *(dest + 4 + i))
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        /// <summary>
        /// Helper function used to check if two byte arrays of given length are equal
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static bool IsEqual(byte* src, byte* dst, int length)
        {
            for (int i = 0; i < length; i++)
            {
                if (*(src + i) != *(dst + i))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Copy numBytes bytes from src to dest
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dest"></param>
        /// <param name="numBytes"></param>
        public unsafe static void Copy(byte* src, byte* dest, int numBytes)
        {
            for(int i = 0; i < numBytes; i++)
            {
                *(dest + i) = *(src + i);
            }
        }

        /// <summary>
        /// Get 64-bit hash code for a long value
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetHashCode(long input)
        {
            long local_rand = input;
            long local_rand_hash = 8;

            local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
            local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
            local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
            local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
            local_rand_hash = 40343 * local_rand_hash;

            return (long)Rotr64((ulong)local_rand_hash, 45);
        }


        /// <summary>
        /// Get 64-bit hash code for a byte array
        /// </summary>
        /// <param name="pbString"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long HashBytes(byte* pbString, int len)
        {
            const long magicno = 40343;
            char* pwString = (char*)pbString;
            int cbBuf = len / 2;
            ulong hashState = (ulong)len;

            for (int i = 0; i < cbBuf; i++, pwString++)
                hashState = magicno * hashState + (ulong)*pwString;

            if ((len & 1) > 0)
            {
                char* pC = (char*)pwString;
                hashState = magicno * hashState + (ulong)*pC;
            }

            return (long)Rotr64(magicno * hashState, 4);
        }
    
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong Rotr64(ulong x, int n)
        {
            return (((x) >> n) | ((x) << (64 - n)));
        }

        /// <summary>
        /// Is power of 2
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsPowerOfTwo(long x)
        {
            return (x > 0) && ((x & (x - 1)) == 0);
        }

        internal static readonly int[] MultiplyDeBruijnBitPosition2 = new int[32]
        {
            0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
            31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
        };

        /// <summary>
        /// Get log base 2
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLogBase2(int x)
        {
            return MultiplyDeBruijnBitPosition2[(uint)(x * 0x077CB531U) >> 27];
        }

        /// <summary>
        /// Get log base 2
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int GetLogBase2(ulong value)
        {
            int i;
            for (i = -1; value != 0; i++)
                value >>= 1;

            return (i == -1) ? 0 : i;
        }

        /// <summary>
        /// Check if power of two
        /// </summary>
        /// <param name="x"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Is32Bit(long x)
        {
            return ((ulong)x < 4294967295ul);
        }


        /// <summary>
        /// A 32-bit murmur3 implementation.
        /// </summary>
        /// <param name="h"></param>
        /// <returns></returns>
        internal static int Murmur3(int h)
        {
            uint a = (uint)h;
            a ^= a >> 16;
            a *= 0x85ebca6b;
            a ^= a >> 13;
            a *= 0xc2b2ae35;
            a ^= a >> 16;
            return (int)a;
        }
    }
}

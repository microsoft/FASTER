// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Helper utility functions for ASCII numbers
    /// </summary>
    public static unsafe class Utils
    {
        /// <summary>
        /// Convert sequence of ASCII bytes into long number
        /// </summary>
        /// <param name="source">Source bytes</param>
        /// <returns>Result</returns>
        public static long BytesToLong(Span<byte> source)
        {
            fixed (byte* ptr = source)
                return BytesToLong(source.Length, ptr);
        }

        /// <summary>
        /// Convert sequence of ASCII bytes into long number
        /// </summary>
        /// <param name="length">Length of number</param>
        /// <param name="source">Source bytes</param>
        /// <returns>Result</returns>
        public static long BytesToLong(int length, byte *source)
        {
            var end = source + length;
            long result = 0;
            while (source < end)
                result = result * 10 + *source++ - '0';
            return result;
        }


        /// <summary>
        /// Convert long number into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="result">Byte pointer</param>
        /// <returns>Length of number in result</returns>
        public static int LongToBytes(long value, Span<byte> dest)
        {
            var numDigits = NumDigits(value);
            if (LongToBytes(value, numDigits, dest))
                return numDigits;
            return 0;
        }

        /// <summary>
        /// Convert long number into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="length">Number of digits in value</param>
        /// <param name="result">Byte pointer</param>
        /// <returns>Whether the conversion succeeded</returns>
        public static bool LongToBytes(long value, int length, Span<byte> dest)
        {
            if (length > dest.Length)
                return false;
            fixed (byte* ptr = dest)
                LongToBytes(value, length, ptr);
            return true;
        }

        /// <summary>
        /// Convert long number into sequence of ASCII bytes
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="length">Number of digits in value</param>
        /// <param name="result">Byte pointer with sufficient space to hold ASCII value</param>
        public static void LongToBytes(long value, int length, byte* result)
        {
            result += length;
            do
            {
                *--result = (byte)((byte)'0' + (value % 10));
                value /= 10;
            } while (value > 0);
        }

        /// <summary>
        /// Return number of digits in given non-negative number
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        public static int NumDigits(long v)
        {
            if (v < 10) return 1;
            if (v < 100) return 2;
            if (v < 1000) return 3;
            if (v < 100000000L)
            {
                if (v < 1000000)
                {
                    if (v < 10000) return 4;
                    return 5 + (v >= 100000 ? 1 : 0);
                }
                return 7 + (v >= 10000000L ? 1 : 0);
            }

            if (v < 1000000000L) return 9;
            if (v < 10000000000L) return 10;
            if (v < 100000000000L) return 11;
            if (v < 1000000000000L) return 12;
            if (v < 10000000000000L) return 13;
            if (v < 100000000000000L) return 14;
            if (v < 1000000000000000L) return 15;
            if (v < 10000000000000000L) return 16;
            if (v < 100000000000000000L) return 17;
            if (v < 1000000000000000000L) return 18;
            return 19;
        }
    }
}
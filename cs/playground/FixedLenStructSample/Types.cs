// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.InteropServices;

namespace FixedLenStructSample
{
    /// <summary>
    /// Represents a fixed length struct type (Length = 32)
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Length)]
    public unsafe struct FixedLenKey : IFasterEqualityComparer<FixedLenKey>
    {
        private const int Length = 32;

        [FieldOffset(0)]
        private byte data;

        public FixedLenKey(string v)
        {
            if (Length < 2 * (v.Length + 1))
                throw new Exception("Insufficient space to store string");

            fixed (byte* ptr = &data)
            {
                var data = (char*)ptr;
                for (var i = 0; i < Length / sizeof(char); i++)
                {
                    if (i < v.Length)
                        *(data + i) = v[i];
                    else
                        *(data + i) = '\0';
                }
            }
        }

        public void CopyTo(ref FixedLenKey dst)
        {
            fixed (byte* source = &data, destination = &dst.data)
                Buffer.MemoryCopy(source, destination, Length, Length);
        }

        public bool Equals(ref FixedLenKey k1, ref FixedLenKey k2)
        {
            fixed (byte* pk1 = &k1.data, pk2 = &k2.data)
            {
                for (var i = 0; i < Length; i++)
                {
                    var left = *(pk1 + i);
                    var right = *(pk2 + i);
                    if (left != right)
                        return false;
                }
            }

            return true;
        }

        public long GetHashCode64(ref FixedLenKey k)
        {
            fixed (byte* data = &k.data)
                return Utility.HashBytes(data, Length);
        }

        public override string ToString()
        {
            fixed (byte* ptr = &data)
                return new string((char*)ptr);
        }
    }

    /// <summary>
    /// Represents a fixed length struct type (Length = 64)
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Length)]
    public unsafe struct FixedLenValue : IFasterEqualityComparer<FixedLenValue>
    {
        private const int Length = 64;

        [FieldOffset(0)]
        private byte data;

        public FixedLenValue(string v)
        {
            if (Length < 2 * (v.Length + 1))
                throw new Exception("Insufficient space to store string");

            fixed (byte* ptr = &data)
            {
                var data = (char*)ptr;
                for (var i = 0; i < Length / sizeof(char); i++)
                {
                    if (i < v.Length)
                        *(data + i) = v[i];
                    else
                        *(data + i) = '\0';
                }
            }
        }

        public void CopyTo(ref FixedLenValue dst)
        {
            fixed (byte* source = &data, destination = &dst.data)
                Buffer.MemoryCopy(source, destination, Length, Length);
        }

        public bool Equals(ref FixedLenValue k1, ref FixedLenValue k2)
        {
            fixed (byte* pk1 = &k1.data, pk2 = &k2.data)
            {
                for (var i = 0; i < Length; i++)
                {
                    var left = *(pk1 + i);
                    var right = *(pk2 + i);
                    if (left != right)
                        return false;
                }
            }

            return true;
        }

        public long GetHashCode64(ref FixedLenValue k)
        {
            fixed (byte* data = &k.data)
                return Utility.HashBytes(data, Length);
        }

        public override string ToString()
        {
            fixed (byte* ptr = &data)
                return new string((char*)ptr);
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

#define EIGHT_BYTE_KEY 
//#define FIXED_SIZE_KEY
//#define VARIABLE_SIZE_KEY

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key : IKey<Key>
    {
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

        public override string ToString()
        {
            return "{ " + value + " }";
        }
    }
}

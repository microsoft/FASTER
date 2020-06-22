// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.Benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key : IFasterEqualityComparer<Key>
    {
        [FieldOffset(0)]
        public long value;


        public override string ToString()
        {
            return "{ " + value + " }";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref Key k)
        {
            return Utility.GetHashCode(k.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.value == k2.value;
        }

    }
}

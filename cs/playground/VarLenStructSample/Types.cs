// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VarLenStructSample
{
    /// <summary>
    /// Represents a variable length type, with direct access to the first two
    /// integer fields in the variable length chunk of memory
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VarLenType
    {
        [FieldOffset(0)]
        public int length;

        [FieldOffset(4)]
        public int field1;

        public void ToIntArray(ref int[] dst)
        {
            dst = new int[length];
            int* src = (int*)Unsafe.AsPointer(ref this);
            for (int i = 0; i < length; i++)
            {
                dst[i] = *src;
                src++;
            }
        }

        public void CopyTo(ref VarLenType dst)
        {
            var fulllength = length * sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this),
                Unsafe.AsPointer(ref dst), fulllength, fulllength);
        }
    }

    public struct VarLenTypeComparer : IFasterEqualityComparer<VarLenType>
    {
        public long GetHashCode64(ref VarLenType k)
        {
            return Utility.GetHashCode(k.length) ^ Utility.GetHashCode(k.field1);
        }

        public unsafe bool Equals(ref VarLenType k1, ref VarLenType k2)
        {
            int* src = (int*)Unsafe.AsPointer(ref k1);
            int* dst = (int*)Unsafe.AsPointer(ref k2);
            int len = *src;

            for (int i = 0; i < len; i++)
                if (*(src + i) != *(dst + i))
                    return false;
            return true;
        }
    }

    public struct VarLenLength : IVariableLengthStruct<VarLenType>
    {
        public int GetInitialLength()
        {
            return 2 * sizeof(int);
        }

        public int GetLength(ref VarLenType t)
        {
            return sizeof(int) * t.length;
        }
    }
}

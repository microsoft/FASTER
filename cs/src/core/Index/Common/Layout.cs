// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.core
{
    internal unsafe struct Layout
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo* GetInfo(long physicalAddress)
        {
            return (RecordInfo*)physicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref Key GetKey(long physicalAddress)
        {
            return ref Unsafe.AsRef<Key>((byte*)physicalAddress + RecordInfo.GetLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetKeyAddress(long physicalAddress)
        {
            return (long)((byte*)physicalAddress + RecordInfo.GetLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref Value GetValue(long physicalAddress)
        {
            return ref Unsafe.AsRef<Value>((byte*)physicalAddress + RecordInfo.GetLength() + default(Key).GetLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetValueAddress(long physicalAddress)
        {
            return (long)((byte*)physicalAddress + RecordInfo.GetLength() + default(Key).GetLength());
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetPhysicalSize(long physicalAddress)
        {
            return RecordInfo.GetLength() + default(Key).GetLength() + default(Value).GetLength();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetInitialPhysicalSize(ref Key key, ref Input input, Functions functions)
        {
            return 
                RecordInfo.GetLength() +
                key.GetLength() + 
                functions.InitialValueLength(ref key, ref input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EstimatePhysicalSize(ref Key key, ref Value value)
        {
            return RecordInfo.GetLength() + key.GetLength() + value.GetLength();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasTotalRecord(byte* buffer, int availableBytes, out int bytesRequired)
        {
            bytesRequired = GetPhysicalSize((long)buffer);
            if (availableBytes < bytesRequired)
            {
                return false;
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetAveragePhysicalSize()
        {
            return RecordInfo.GetLength() + default(Key).GetLength() + default(Value).GetLength();
        }
    }
}

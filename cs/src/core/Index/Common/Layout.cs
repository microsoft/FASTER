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
        public static Key* GetKey(long physicalAddress)
        {
            return (Key*)((byte*)physicalAddress + RecordInfo.GetLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Value* GetValue(long physicalAddress)
        {
            return (Value*)((byte*)physicalAddress + RecordInfo.GetLength() + Key.GetLength(default(Key*)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetPhysicalSize(long physicalAddress)
        {
            return RecordInfo.GetLength() + Key.GetLength(default(Key*)) + Value.GetLength(default(Value*));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetInitialPhysicalSize(Key* key, Input* input)
        {
            return 
                RecordInfo.GetLength() + 
                Key.GetLength(default(Key*)) + 
                Functions.InitialValueLength(key, input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EstimatePhysicalSize(Key* key, Value* value)
        {
            return RecordInfo.GetLength() + Key.GetLength(key) + Value.GetLength(value);
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
            return RecordInfo.GetLength() + Key.GetLength(default(Key*)) + Value.GetLength(default(Value*));
        }
    }
}

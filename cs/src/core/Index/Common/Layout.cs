// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define FIXED_SIZE
//#define VARIABLE_SIZE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.core
{
#if FIXED_SIZE
    public unsafe struct Layout
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
#elif VARIABLE_SIZE
    public unsafe struct Layout
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetPhysicalSize(long physicalAddress)
        {
            return RecordInfo.GetPhysicalSize() + Key.GetPhysicalSize(GetKey(physicalAddress)) + Value.GetPhysicalSize(GetValue(physicalAddress));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetInitialPhysicalSize(Key* key, Input* input)
        {
            return RecordInfo.GetPhysicalSize() + Key.GetPhysicalSize(key) + Value.GetInitialPhysicalSize(input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo* GetInfo(long physicalAddress)
        {
            return (RecordInfo*)physicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Key* GetKey(long physicalAddress)
        {
            return (Key*)((byte*)physicalAddress + RecordInfo.GetPhysicalSize());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Value* GetValue(long physicalAddress)
        {
            return (Value*)((byte*)physicalAddress + RecordInfo.GetPhysicalSize() + Key.GetPhysicalSize(GetKey(physicalAddress)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EstimatePhysicalSize(Key* key, Value* value)
        {
            return RecordInfo.GetPhysicalSize() + Key.GetPhysicalSize(key) + Value.GetPhysicalSize(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasTotalRecord(byte* buffer, int availableBytes, out int bytesRequired)
        {
            bytesRequired = RecordInfo.GetPhysicalSize() + sizeof(int);
            if (availableBytes < bytesRequired)
            {
                return false;
            }

            bytesRequired += Key.GetPhysicalSize(GetKey((long)buffer)) + sizeof(int);
            if (availableBytes < bytesRequired)
            {
                return false;
            }

            bytesRequired += Value.GetPhysicalSize(GetValue((long)buffer));
            if (availableBytes < bytesRequired)
            {
                return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetAveragePhysicalSize()
        {
            return 512;
        }
    }
#endif
}

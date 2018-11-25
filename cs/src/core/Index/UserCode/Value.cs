// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{

#if EIGHT_BYTE_VALUE
    [StructLayout(LayoutKind.Explicit, Size = kSizeInBytes)]
    public unsafe struct Value : IValue<Value>
    {
        public const int kValuesStartOffset = 0;
        public const int kSizeInBytes = sizeof(long) + kValuesStartOffset;

        [FieldOffset(kValuesStartOffset)]
        public long value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquireReadLock()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseReadLock()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquireWriteLock()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseWriteLock()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShallowCopy(ref Value dst)
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
        }

        public void Deserialize(Stream fromStream)
        {
        }

        public void Free()
        {
        }

        public ref Value MoveToContext(ref Value value)
        {
            return ref value;
        }
    }
#elif FIXED_SIZE_VALUE_WITH_LOCK
    [StructLayout(LayoutKind.Explicit, Size = kSizeInBytes)]
    public unsafe struct Value
    {
        public const int kNumFields = 1;

        public const int kValuesStartOffset = sizeof(long);

        public const int kSizeInBytes = kNumFields * sizeof(long) + kValuesStartOffset;

        public const long expected_exclusive_lock_word = 0;

        [FieldOffset(0)]
        public int lock_data;

        [FieldOffset(4)]
        public int dummy;

        [FieldOffset(kValuesStartOffset)]
        public fixed long values[kNumFields];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireReadLock(Value* value)
        {
            //incremented value
            var val = Interlocked.Increment(ref value->lock_data);
            if (val < 0)
            {
                do
                {
                    //found value
                    val = Interlocked.CompareExchange(ref value->lock_data, 1, 0);
                    if (val == 0)
                    {
                        break;
                    }
                    else if (val > 0)
                    {
                        val = Interlocked.Increment(ref value->lock_data);
                    }
                } while (val < 0);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseReadLock(Value* value)
        {
            Interlocked.Decrement(ref value->lock_data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireWriteLock(Value* value)
        {
            var found_value = Interlocked.CompareExchange(ref value->lock_data, int.MinValue, 0);
            if (found_value != 0)
            {
                int num_iterations = 1000;
                Thread.SpinWait(num_iterations);
                while (Interlocked.CompareExchange(ref value->lock_data, int.MinValue, 0) != 0)
                {
                    Thread.SpinWait(num_iterations);
                    num_iterations <<= 1;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseWriteLock(Value* value)
        {
            Interlocked.Exchange(ref value->lock_data, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(Value* src, Value* dst)
        {
            if (kNumFields == 12)
            {
                *(dst->values + 0) = *(src->values + 0);
                *(dst->values + 1) = *(src->values + 1);
                *(dst->values + 2) = *(src->values + 2);
                *(dst->values + 3) = *(src->values + 3);
                *(dst->values + 4) = *(src->values + 4);
                *(dst->values + 5) = *(src->values + 5);
                *(dst->values + 6) = *(src->values + 6);
                *(dst->values + 7) = *(src->values + 7);
                *(dst->values + 8) = *(src->values + 8);
                *(dst->values + 9) = *(src->values + 9);
                *(dst->values + 10) = *(src->values + 10);
                *(dst->values + 11) = *(src->values + 11);
                *(dst->values + 12) = *(src->values + 12);
            }
            else
            {
                for (int i = 0; i < kNumFields; i++)
                    *(dst->values + i) = *(src->values + i);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(Value* value)
        {
            return kSizeInBytes;
        }

        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(Value* key, Stream toStream)
        {
        }

        public static void Deserialize(Value* key, Stream fromStream)
        {
        }

        public static void Free(Value* value)
        {
        }

        public static Value* MoveToContext(Value* value)
        {
            return value;
        }
    }
#elif FIXED_SIZE_VALUE
    [StructLayout(LayoutKind.Explicit, Size = kSizeInBytes)]
    public unsafe struct Value
    {
        public const int kNumFields = 1;

        public const int kValuesStartOffset = 0;

        public const int kSizeInBytes = kNumFields * sizeof(long);

        [FieldOffset(kValuesStartOffset)]
        public fixed long values[kNumFields];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireReadLock(Value* value)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseReadLock(Value* value)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireWriteLock(Value* value)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseWriteLock(Value* value)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(Value* src, Value* dst)
        {
            Utility.Copy((byte*)src, (byte*)dst, kSizeInBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(Value* value)
        {
            return kSizeInBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetPhysicalSize(Value* value)
        {
            return kSizeInBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetInitialPhysicalSize(Input* input)
        {
            return kSizeInBytes;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EstimateAveragePhysicalSize()
        {
            return kSizeInBytes;
        }

        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(Value* key, Stream toStream)
        {
        }

        public static void Deserialize(Value* key, Stream fromStream)
        {
        }

        public static void Free(Value* value)
        {
        }

        public static Value* MoveToContext(Value* value)
        {
            return value;
        }
    }
#endif
}

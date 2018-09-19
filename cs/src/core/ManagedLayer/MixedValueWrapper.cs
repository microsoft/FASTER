// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Threading;

namespace FASTER.core
{
    [FASTER.core.Roslyn.TypeKind("internal")]
#if VALUE_ATOMIC
#if BLIT_VALUE
    public unsafe struct MixedValueWrapper
    {
        public MixedValue value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedValueWrapper* input)
        {
            return sizeof(MixedValueWrapper);
        }

        public static void Copy(MixedValueWrapper* src, MixedValueWrapper* dst)
        {
            dst->value = src->value;
        }

        // Shared read/write capabilities on value
        public static void AcquireReadLock(MixedValueWrapper* value)
        {
        }

        public static void ReleaseReadLock(MixedValueWrapper* value)
        {
        }

        public static void AcquireWriteLock(MixedValueWrapper* value)
        {
        }

        public static void ReleaseWriteLock(MixedValueWrapper* value)
        {
        }

    #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(MixedValueWrapper* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(MixedValueWrapper* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(MixedValueWrapper* key)
        {
            throw new InvalidOperationException();
        }
    #endregion

        public static MixedValueWrapper* MoveToContext(MixedValueWrapper* value)
        {
            return value;
        }
    }
#else
    public unsafe struct MixedValueWrapper
    {
        public BlittableTypeWrapper value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedValueWrapper* input)
        {
            return sizeof(void*);
        }

        public static void Copy(MixedValueWrapper* src, MixedValueWrapper* dst)
        {
            dst->value = BlittableTypeWrapper.Create(src->value.GetObject<MixedValue>().Clone());
        }

        // Shared read/write capabilities on value
        public static void AcquireReadLock(MixedValueWrapper* value)
        {
        }

        public static void ReleaseReadLock(MixedValueWrapper* value)
        {
        }

        public static void AcquireWriteLock(MixedValueWrapper* value)
        {
        }

        public static void ReleaseWriteLock(MixedValueWrapper* value)
        {
        }

    #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return true;
        }

        public static void Serialize(MixedValueWrapper* value, Stream toStream)
        {
            value->value.GetObject<MixedValue>().Serialize(toStream);
        }

        public static void Deserialize(MixedValueWrapper* value, Stream fromStream)
        {
            MixedValue v = new MixedValue();
            v.Deserialize(fromStream);
            value->value = BlittableTypeWrapper.Create(v);
        }

        public static void Free(MixedValueWrapper* value)
        {
            value->value.Free<MixedValue>();
        }
    #endregion

        public static MixedValueWrapper* MoveToContext(MixedValueWrapper* value)
        {
            return value;
        }
    }
#endif
#else
#if BLIT_VALUE
    public unsafe struct MixedValueWrapper
    {
        public MixedValue value;
        public int lock_data;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedValueWrapper* input)
        {
            return sizeof(MixedValueWrapper);
        }

        public static void Copy(MixedValueWrapper* src, MixedValueWrapper* dst)
        {
            dst->value = src->value;
            dst->lock_data = 0;
        }

        // Shared read/write capabilities on value
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireReadLock(MixedValueWrapper* value)
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
        public static void ReleaseReadLock(MixedValueWrapper* value)
        {
            Interlocked.Decrement(ref value->lock_data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireWriteLock(MixedValueWrapper* value)
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
        public static void ReleaseWriteLock(MixedValueWrapper* value)
        {
            Interlocked.Exchange(ref value->lock_data, 0);
        }

    #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(MixedValueWrapper* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(MixedValueWrapper* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(MixedValueWrapper* key)
        {
            throw new InvalidOperationException();
        }
    #endregion

        public static MixedValueWrapper* MoveToContext(MixedValueWrapper* value)
        {
            var valuePtr = (MixedValueWrapper*)MallocFixedPageSize<long>.PhysicalInstance.Allocate();
            *valuePtr = *value;
            return valuePtr;
        }
    }

#else
    [StructLayout(LayoutKind.Explicit, Size = 12)]
    public unsafe struct MixedValueWrapper
    {
        [FieldOffset(0)]
        public BlittableTypeWrapper value;
        [FieldOffset(8)]
        public int lock_data;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedValueWrapper* input)
        {
            return sizeof(void*) + sizeof(int);
        }

        public static void Copy(MixedValueWrapper* src, MixedValueWrapper* dst)
        {
            dst->lock_data = 0;
            dst->value = BlittableTypeWrapper.Create(src->value.GetObject<MixedValue>().Clone());
        }

        // Shared read/write capabilities on value
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireReadLock(MixedValueWrapper* value)
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
        public static void ReleaseReadLock(MixedValueWrapper* value)
        {
            Interlocked.Decrement(ref value->lock_data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void AcquireWriteLock(MixedValueWrapper* value)
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
        public static void ReleaseWriteLock(MixedValueWrapper* value)
        {
            Interlocked.Exchange(ref value->lock_data, 0);
        }

#region Serialization
        public static bool HasObjectsToSerialize()
        {
            return true;
        }

        public static void Serialize(MixedValueWrapper* value, Stream toStream)
        {
            value->value.GetObject<MixedValue>().Serialize(toStream);
        }

        public static void Deserialize(MixedValueWrapper* value, Stream fromStream)
        {
            MixedValue v = new MixedValue();
            v.Deserialize(fromStream);
            value->value = BlittableTypeWrapper.Create(v);
        }

        public static void Free(MixedValueWrapper* value)
        {
            value->value.Free<MixedValue>();
        }
#endregion

        public static MixedValueWrapper* MoveToContext(MixedValueWrapper* value)
        {
            var valuePtr = (MixedValueWrapper*)MallocFixedPageSize<long>.PhysicalInstance.Allocate();
            *valuePtr = *value;
            return valuePtr;
        }
    }
#endif
#endif
}

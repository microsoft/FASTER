// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace PerfTest
{
    /// <summary>
    /// Represents a variable length value type, with direct access to the first two
    /// reinterpret_cast{integer} fields in the variable length chunk of memory
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VarLenValue : ICacheValue<VarLenValue>
    {
        [FieldOffset(0)]
        public int Length;

        [FieldOffset(4)]
        internal int field1;

        public long Value
        {
            get => field1;
            set => field1 = (int)value;
        }

        public int[] ToIntArray()
        {
            var dst = new int[Length];
            int* src = (int*)Unsafe.AsPointer(ref this);
            for (int i = 0; i < Length; ++i, ++src)
                dst[i] = *src;
            return dst;
        }

        public void CopyTo(ref VarLenValue dst)
        {
            var fullLength = Length * sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), fullLength, fullLength);
        }

        public VarLenValue Clone()
        {
            int* dst = stackalloc int[this.Length];
            int* self = (int*)Unsafe.AsPointer(ref this);
            for (int j = 0; j < this.Length; j++)
                *(dst + j) = *(self + j);
            ref VarLenValue value = ref *(VarLenValue*)dst;
            return value;
        }

        public bool CompareValue(long returnedValue)
        {
            if ((int)returnedValue != this.field1)
                return false;
#if false // For debugging only; for long data lengths we don't want to distort speed
            int* self = (int*)Unsafe.AsPointer(ref this);
            for (int ii = 2; ii < this.Length; ++ii, ++self)
            {
                if (self[ii] != this.Length)
                    return false;
            }
#endif
            return true;
        }

        public VarLenValue Create(long first)
        {
            // To keep with the average of CacheGlobals.DataSize, modulo across the range 2x that.
            var len = Math.Max(CacheGlobals.MinDataSize, (int)first % (((CacheGlobals.DataSize - 2) / sizeof(int)) * 2));
            int* intMem = stackalloc int[len];
            ref VarLenValue value = ref *(VarLenValue*)intMem;
            for (int j = 0; j < len; j++)
                *(intMem + j) = len;
            value.Value = first;
            return value;
        }
    }

    public struct VarLenValueComparer : IFasterEqualityComparer<VarLenValue>
    {
        public long GetHashCode64(ref VarLenValue k) 
            => Utility.GetHashCode(k.Length) ^ Utility.GetHashCode(k.field1);

        public unsafe bool Equals(ref VarLenValue k1, ref VarLenValue k2)
        {
            int* src = (int*)Unsafe.AsPointer(ref k1);
            int* dst = (int*)Unsafe.AsPointer(ref k2);
            int len = *src;

            for (int i = 0; i < len; i++)
            {
                if (*(src + i) != *(dst + i))
                    return false;
            }
            return true;
        }
    }

    public struct VarLenValueLength<TValue> : IVariableLengthStruct<TValue>
    {
        public int GetAverageLength() => CacheGlobals.DataSize;

        public int GetInitialLength<Input>(ref Input input) => 2 * sizeof(int);

        public int GetLength(ref TValue t) => t is VarLenValue vv ? sizeof(int) * vv.Length : throw new InvalidOperationException();
    }

    public struct VarLenInput
    {
    }

    public struct VarLenOutput : ICacheOutput<VarLenValue>
    {
        public VarLenValue Value { get; set; }
    }

    public class VarLenFunctions : IFunctions<CacheKey, VarLenValue, CacheInput, VarLenOutput, CacheContext>
    {
        public void RMWCompletionCallback(ref CacheKey key, ref CacheInput _, CacheContext ctx, Status status) { }

        public void ReadCompletionCallback(ref CacheKey key, ref CacheInput _, ref VarLenOutput output, CacheContext ctx, Status status)
        {
            if (status != Status.OK)
                Console.WriteLine("Sample1 error! Status != OK");
            else if (!output.Value.CompareValue(key.key))
                Console.WriteLine($"Sample1 error! Output value {output.Value.Value} != key {key.key}");
        }

        public void UpsertCompletionCallback(ref CacheKey key, ref VarLenValue output, CacheContext ctx) { }

        public void DeleteCompletionCallback(ref CacheKey key, CacheContext ctx) { }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) 
            => Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);

        // Read functions
        public void SingleReader(ref CacheKey key, ref CacheInput _, ref VarLenValue value, ref VarLenOutput dst) 
            => dst.Value = value.Clone();

        public void ConcurrentReader(ref CacheKey key, ref CacheInput _, ref VarLenValue value, ref VarLenOutput dst)
            => dst.Value = value.Clone();

        // Upsert functions
        public void SingleWriter(ref CacheKey key, ref VarLenValue src, ref VarLenValue dst) 
            => src.CopyTo(ref dst);

        public bool ConcurrentWriter(ref CacheKey key, ref VarLenValue src, ref VarLenValue dst)
        {
            src.CopyTo(ref dst);
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref CacheKey key, ref CacheInput _, ref VarLenValue value) { }

        public bool InPlaceUpdater(ref CacheKey key, ref CacheInput _, ref VarLenValue value) => true;

        public void CopyUpdater(ref CacheKey key, ref CacheInput _, ref VarLenValue oldValue, ref VarLenValue newValue) { }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.PerfTest
{
    /// <summary>
    /// Represents a variable length value type, with direct access to the first two
    /// reinterpret_cast{integer} fields in the variable length chunk of memory
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VarLenValue
    {
        // Note that we assume Globals.MinDataSize is 8
        [FieldOffset(0)] internal int Length;
        [FieldOffset(4)] internal int field1;

        internal static int MaxLen => Globals.MaxDataSize / sizeof(int);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(ref VarLenValue dst)
        {
            if (this.Length == 0)
                throw new ApplicationException("0-length VarLen");
            var fullLength = Length * sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), fullLength, fullLength);
        }

        public void SetValueAndLength(long value)
        {
            // The single instance must be created in the driving program due to the use of stackalloc.
            // There, it is created at max size; here, we are just modifying the length to be written/read.
            // To keep with the average of Globals.DataSize, modulo across the range 2x that.
            var len = Math.Max(Globals.MinDataSize, (int)value % (((Globals.DataSize - 2) / sizeof(int)) * 2));
            int* thisPtr = (int*)Unsafe.AsPointer(ref this);
            for (int ii = 0; ii < len; ii++)
                *(thisPtr + ii) = len;
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
        public int GetAverageLength() => Globals.DataSize;

        public int GetInitialLength<Input>(ref Input input) => 2 * sizeof(int);

        public int GetLength(ref TValue t) => t is VarLenValue vv ? sizeof(int) * vv.Length : throw new InvalidOperationException();
    }

    public struct VarLenInput
    {
    }

    public struct VarLenOutput
    {
        internal GetVarLenValueRef getValueRef;
        internal int threadIndex;

        internal VarLenOutput(GetVarLenValueRef getValueRef, int threadIndex)
        {
            this.getValueRef = getValueRef;
            this.threadIndex = threadIndex;
        }

        internal ref VarLenValue GetValueRef() => ref this.getValueRef.GetRef(this.threadIndex);
    }

    unsafe class GetVarLenValueRef : IGetValueRef<VarLenValue, VarLenOutput>, IDisposable
    {
        // This class is why we need the whole IGetValueRef idea; allocating unmanaged memory
        // for VarLenValue. These are per-thread (that's why we keep an array, and that also
        // makes it easier to free on Dispose(). During operations, Upserts can copy from this
        // to the store (in VERIFY mode we can write values to be copied) and Reads can copy from
        // the store to this (in VERIFY mode, verifying the values read), without fear of conflict.
        IntPtr[] values;

        internal GetVarLenValueRef(int count)
        {
            values = Enumerable.Range(0, count).Select(ii => Marshal.AllocHGlobal(sizeof(int) * VarLenValue.MaxLen)).ToArray();
            for (var ii = 0; ii < count; ++ii)
            {
                var ptr = values[ii];
                for (var jj = 0; jj < VarLenValue.MaxLen; ++jj)
                    Marshal.WriteInt32(ptr, jj * sizeof(int), 0);
                ref var varlen = ref *(VarLenValue*)ptr.ToPointer();
                varlen.SetValueAndLength(8);
            }
        }

        public ref VarLenValue GetRef(int threadIndex) => ref *(VarLenValue*)values[threadIndex].ToPointer();

        public VarLenOutput GetOutput(int threadIndex) => new VarLenOutput(this, threadIndex);

        public void Dispose()
        {
            if (!(values is null))
            {
                foreach (var intptr in values)
                    Marshal.FreeHGlobal(intptr);
                values = null;
            }
        }
    }

    public class VarLenFunctions : IFunctions<Key, VarLenValue, Input, VarLenOutput, Empty>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref Key key, ref Input _, Empty ctx, Status status)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref Key key, ref Input _, ref VarLenOutput output, Empty ctx, Status status)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref Key key, ref VarLenValue output, Empty ctx)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref Key key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input _, ref VarLenValue value, ref VarLenOutput dst)
            => value.CopyTo(ref dst.GetValueRef());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input _, ref VarLenValue value, ref VarLenOutput dst) 
            => value.CopyTo(ref dst.GetValueRef());

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref VarLenValue src, ref VarLenValue dst) 
            => src.CopyTo(ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref VarLenValue src, ref VarLenValue dst)
        {
            src.CopyTo(ref dst);
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref VarLenValue value)
            => value.field1 = input.value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref VarLenValue value)
        {
            value.field1 += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref VarLenValue oldValue, ref VarLenValue newValue)
            => newValue.field1 = input.value + oldValue.field1;
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.PerfTest
{
    /// <summary>
    /// Represents a variable length value type, with direct access to the first
    /// reinterpret_cast{Data} field in the variable length chunk of memory
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VarLenValue
    {
        // Note that we assume Globals.MinDataSize is 8 and Globals.DataSize is a multiple of that
        [StructLayout(LayoutKind.Explicit)]
        internal struct Data
        {
            // Number of Data structs (so Global.DataSize / sizeof(Data))
            [FieldOffset(0)] internal ushort Length;

            // Modified by RMW
            [FieldOffset(2)] internal ushort Modified;

            // This is key.key which must be an int as we allocate an array of sequential Keys
            [FieldOffset(4)] internal int Value;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool Equals(Data other) 
                => this.Length == other.Length && this.Modified == other.Modified && this.Value == other.Value;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long GetHashCode64()
                => Utility.GetHashCode(this.Length) ^ Utility.GetHashCode(this.Modified) ^ Utility.GetHashCode(this.Value);

            public override string ToString() => $"len {Length}, mod {Modified}, val {Value}";
        }

        [FieldOffset(0)] internal Data data;

        internal static int MaxIntLength => Globals.MaxDataSize / sizeof(int);

        internal int ByteLength 
            => this.data.Length > 0 ? this.data.Length * sizeof(Data) : throw new ApplicationException("0-length VarLen");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValueAndLength(long value, long mod)
        {
            // The single instance must be created once per thread in the driving program due to GlobalAlloc perf impact.
            // There, it is created at max size; here, we are just modifying the length (<= max size) to be written/read.
            // To exercise variable-length handling we don't want a constant Globals.DataSize length, but stay reasonably
            // close to Globals.DataSize by keeping it in the upper half of Globals.DataSize, which averages 3/4 datasize.
            var halfSize = Globals.DataSize / 2;
            var modSize = ((int)value % halfSize) + halfSize;
            var len = (ushort)(modSize < Globals.MinDataSize ? Globals.MinDataSize : modSize);
            Data* dataPtr = (Data*)Unsafe.AsPointer(ref this);

            // Because of the %, len will never == MaxDataSize, so add 1; this ranges from the first element after half
            // through the last item.
            this.data.Length = (ushort)(len / sizeof(Data) + 1);
            this.data.Modified = (ushort)(mod % ushort.MaxValue);
            this.data.Value = (int)value;
            if (Globals.Verify)
            {
                for (int ii = 1; ii < this.data.Length; ii++)
                    *(dataPtr + ii) = this.data;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetModified(int value)
        {
            this.data.Modified = (ushort)(value % ushort.MaxValue);
            if (Globals.Verify)
            {
                // Faster to just copy the whole 8 bytes than do the bit shifting to get to the ushort
                Data* dataPtr = (Data*)Unsafe.AsPointer(ref this);
                for (int ii = 1; ii < this.data.Length; ii++)
                    *(dataPtr + ii) = this.data;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CopyTo(ref VarLenValue dst, bool isUpsert)
        {
            // If this is an Upsert (not initial insert), we want to preserve dst length; otherwise
            // we will write past FASTER's buffer length if it's shorter and will lose knowledge of
            // the extra space if it's larger. Note: For initial insert, memory is zero'd by FASTER
            // so dst.data.Length will be 0. If it's not Upsert, then we are reading into ThreadRef
            // which is allocated to max size; if it'is Upsert, then we are writing from ThreadRef.
            // Either way, the ThreadRef value can be modified however we want here.
            if (isUpsert && dst.data.Length > 0 && dst.data.Length != this.data.Length)
            {
                this.data.Length = dst.data.Length;
                if (Globals.Verify)
                {
                    // Verify needs uniform data, so update all our data items out to the new length.
                    Data* dataPtr = (Data*)Unsafe.AsPointer(ref this);
                    for (int ii = 1; ii < this.data.Length; ii++)
                        *(dataPtr + ii) = this.data;
                }
            }
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), this.ByteLength, this.ByteLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            Data* dataPtr = (Data*)Unsafe.AsPointer(ref this);
            for (var ii = 1; ii < this.data.Length; ++ii)
            {
                if (!(*(dataPtr + ii)).Equals(this.data))
                    throw new ApplicationException($"dataPtr[{ii}] != this.data");
            }
        }

        public override string ToString() => $"ByteLen {ByteLength}, {data}";
    }

    public struct VarLenValueComparer : IFasterEqualityComparer<VarLenValue>
    {
        public long GetHashCode64(ref VarLenValue k) 
            => k.data.GetHashCode64();

        public unsafe bool Equals(ref VarLenValue k1, ref VarLenValue k2)
        {
            VarLenValue.Data* src = (VarLenValue.Data*)Unsafe.AsPointer(ref k1);
            VarLenValue.Data* dst = (VarLenValue.Data*)Unsafe.AsPointer(ref k2);
            int len = (*src).Length;

            for (int i = 0; i < len; i++)
            {
                if (!(*(src + i)).Equals(*(dst + i)))
                    return false;
            }
            return true;
        }
    }

    public unsafe struct VarLenValueLength<TValue> : IVariableLengthStruct<TValue>
    {
        public int GetAverageLength() => Globals.DataSize;

        public int GetInitialLength<Input>(ref Input input) => sizeof(VarLenValue.Data);

        public int GetLength(ref TValue t) => t is VarLenValue vv ? vv.ByteLength : throw new InvalidOperationException();
    }

    public struct VarLenOutput
    {
        internal VarLenThreadValueRef getThreadValueRef;
        internal int threadIndex;

        internal VarLenOutput(VarLenThreadValueRef getThreadValueRef, int threadIndex)
        {
            this.getThreadValueRef = getThreadValueRef;
            this.threadIndex = threadIndex;
        }

        internal ref VarLenValue ThreadValueRef => ref this.getThreadValueRef.GetRef(this.threadIndex);

        public override string ToString() => this.ThreadValueRef.ToString();
    }

    unsafe class VarLenThreadValueRef : IThreadValueRef<VarLenValue, VarLenOutput>, IDisposable
    {
        // This class is why we need the whole IThreadValueRef idea; allocating unmanaged memory
        // for VarLenValue. These are per-thread (that's why we keep an array, and that also
        // makes it easier to free on Dispose()). During operations, Upserts can copy from this
        // to the store (in VERIFY mode we can write values to be copied) and Reads can copy from
        // the store to this (in VERIFY mode, verifying the values read), without fear of conflict.
        IntPtr[] values;

        internal VarLenThreadValueRef(int threadCount)
        {
            this.values = new IntPtr[threadCount];
            for (var ii = 0; ii < threadCount; ++ii)
            {
                values[ii] = Marshal.AllocHGlobal(sizeof(int) * VarLenValue.MaxIntLength);
                for (var jj = 0; jj < VarLenValue.MaxIntLength; ++jj)
                    Marshal.WriteInt32(values[ii], jj * sizeof(int), 0);
                ref var varlen = ref *(VarLenValue*)values[ii].ToPointer();
                varlen.SetInitialValueAndLength(0, 0);
            }
        }

        public ref VarLenValue GetRef(int threadIndex) => ref *(VarLenValue*)values[threadIndex].ToPointer();

        public VarLenOutput GetOutput(int threadIndex) => new VarLenOutput(this, threadIndex);

        public void SetInitialValue(ref VarLenValue valueRef, long value) => valueRef.SetInitialValueAndLength(value, 0);

        public void SetUpsertValue(ref VarLenValue valueRef, long value, long mod) => valueRef.SetInitialValueAndLength((int)(value % int.MaxValue), mod);

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
        #region Upsert
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref VarLenValue src, ref VarLenValue dst)
        {
            SingleWriter(ref key, ref src, ref dst);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref VarLenValue src, ref VarLenValue dst)
        {
            if (Globals.Verify)
            {
                if (!Globals.IsInitialInsertPhase)
                    dst.Verify(key.key);
                src.Verify(key.key);
            }
            src.CopyTo(ref dst, true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref Key key, ref VarLenValue value, Empty ctx)
        {
            if (Globals.Verify)
                value.Verify(key.key);
        }
        #endregion Upsert

        #region Read
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref VarLenValue value, ref VarLenOutput dst)
            => SingleReader(ref key, ref input, ref value, ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input _, ref VarLenValue value, ref VarLenOutput dst)
        {
            if (Globals.Verify)
                value.Verify(key.key);
            value.CopyTo(ref dst.ThreadValueRef, false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref Key key, ref Input _, ref VarLenOutput output, Empty ctx, Status status)
        {
            if (Globals.Verify)
                output.ThreadValueRef.Verify(key.key);
        }
        #endregion Read

        #region RMW
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref VarLenValue value)
            => value.SetInitialValueAndLength(key.key, input.value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref VarLenValue value)
        {
            if (Globals.Verify)
                value.Verify(key.key);
            value.SetModified(value.data.Modified + input.value);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref VarLenValue oldValue, ref VarLenValue newValue)
        {
            if (Globals.Verify)
                oldValue.Verify(key.key);
            newValue.SetInitialValueAndLength(key.key, oldValue.data.Modified + input.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref Key key, ref Input _, Empty ctx, Status status)
        { }
        #endregion RMW

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref Key key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");
    }
}

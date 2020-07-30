// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.PerfTest
{
    /// <summary>
    /// Represents a variable length value type, with direct access to the first
    /// reinterpret_cast{Data} field in the variable length chunk of memory
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VarLenType : IKey
    {
        // Note that we assume Globals.MinDataSize is 8 and Globals.DataSize is a multiple of that
        [StructLayout(LayoutKind.Explicit)]
        internal struct Data
        {
            // Number of Data structs (so Globals.DataSize / sizeof(Data))
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

        public long Value => this.data.Value;

        internal static int MaxIntLength => Globals.MaxDataSize / sizeof(int);

        internal int ByteLength 
            => this.data.Length > 0 ? this.data.Length * sizeof(Data) : throw new ApplicationException("0-length VarLen");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetNewLength(int value, bool isKey)
        {
            var size = isKey ? Globals.KeySize : Globals.ValueSize;

            // To exercise variable-length handling we don't want a constant Globals.DataSize length, but stay reasonably
            // close to Globals.DataSize by keeping it in the upper part of Globals.DataSize. Because of the %, len will
            // never == MaxDataSize, so add 1; this ranges from the first element after (size - (pad)) through the last item.
            if (size >= sizeof(Data) * 8)
                return (value % (sizeof(Data) * 4) + (size - sizeof(Data) * 4) + 1) / sizeof(Data);
            else if (size >= sizeof(Data) * 4)
                return (value % (sizeof(Data) * 2) + (size - sizeof(Data) * 2) + 1) / sizeof(Data);
            return size / sizeof(Data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValueAndLength(long value, long mod)
        {
            // The single instance must be created once per thread in the driving program due to GlobalAlloc perf impact.
            // There, it is created at max size; here, we are just modifying the length (<= max size) to be written/read.
            var len = GetNewLength((int)value, isKey:false);
            Data* dataPtr = (Data*)Unsafe.AsPointer(ref this);

            this.data.Length = (ushort)len;
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
        internal void CopyTo(ref VarLenType dst, bool isUpsert)
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

        public struct EqualityComparer : IFasterEqualityComparer<VarLenType>
        {
            public long GetHashCode64(ref VarLenType k) => k.data.GetHashCode64();

            public unsafe bool Equals(ref VarLenType k1, ref VarLenType k2)
            {
                var src = (Data*)Unsafe.AsPointer(ref k1);
                var dst = (Data*)Unsafe.AsPointer(ref k2);

                if (!src->Equals(*dst))
                    return false;

                if (Globals.Verify)
                {
                    int len = (*src).Length;
                    for (int i = 1; i < len; i++)
                    {
                        if (!(*(src + i)).Equals(*(dst + i)))
                            return false;
                    }
                }
                return true;
            }
        }
    }

    public unsafe struct VarLenTypeLength : IVariableLengthStruct<VarLenType>
    {
        public int GetAverageLength() => Globals.ValueSize;

        public int GetInitialLength<Input>(ref Input input) => sizeof(VarLenType.Data);

        public int GetLength(ref VarLenType t) => t.ByteLength;

        public int GetLength<Input>(ref VarLenType t, ref Input input) => t.ByteLength;
    }

    public struct VarLenOutput
    {
        internal VarLenValueWrapper valueWrapper;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal VarLenOutput(VarLenValueWrapper wrapper) => this.valueWrapper = wrapper;

        internal ref VarLenType ValueRef => ref this.valueWrapper.GetRef();

        public override string ToString() => this.ValueRef.ToString();
    }

    unsafe class VarLenValueWrapperFactory : IValueWrapperFactory<VarLenType, VarLenOutput, VarLenValueWrapper>, IDisposable
    {
        // This class is why we need the whole IValueWrapperFactory idea; allocating unmanaged memory for VarLenType. These are per-thread
        // (that's why we keep an array, and that also makes it easier to free on Dispose()). During operations, Upserts can copy from this
        // to the store (in VERIFY mode we can write values to be copied) and Reads can copy from the store to this (in VERIFY mode, verifying
        // the values read), without fear of conflict.
        VarLenValueWrapper[] wrappers;

        internal VarLenValueWrapperFactory(int threadCount)
            => this.wrappers = Enumerable.Range(0, threadCount).Select(ii => new VarLenValueWrapper()).ToArray();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public VarLenValueWrapper GetValueWrapper(int threadIndex) => this.wrappers[threadIndex];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public VarLenOutput GetOutput(int threadIndex) => new VarLenOutput(this.wrappers[threadIndex]);

        public void Dispose()
        {
            if (!(wrappers is null))
            {
                foreach (var wrapper in wrappers)
                    wrapper.Dispose();
                wrappers = null;
            }
        }
    }

    unsafe class VarLenValueWrapper : IValueWrapper<VarLenType>
    {
        private readonly IntPtr value;

        internal VarLenValueWrapper()
        {
            this.value = Marshal.AllocHGlobal(sizeof(int) * VarLenType.MaxIntLength);
            for (var jj = 0; jj < VarLenType.MaxIntLength; ++jj)
                Marshal.WriteInt32(this.value, jj * sizeof(int), 0);
            ref var varlen = ref *(VarLenType*)this.value.ToPointer();
            varlen.SetInitialValueAndLength(0, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal VarLenValueWrapper(IntPtr intValue) => this.value = intValue;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref VarLenType GetRef() => ref *(VarLenType*)value.ToPointer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value) => this.GetRef().SetInitialValueAndLength(value, 0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetUpsertValue(long value, long mod) => this.GetRef().SetInitialValueAndLength((int)(value % int.MaxValue), mod);

        internal void Dispose() => Marshal.FreeHGlobal(this.value);
    }

    internal unsafe class VarLenKeyManager : KeyManagerBase<VarLenType>, IKeyManager<VarLenType>, IDisposable
    {
        readonly List<IntPtr> chunks = new List<IntPtr>();
        VarLenType*[] initKeys;
        VarLenType*[] opKeys;

        internal override void Initialize(ZipfSettings zipfSettings, RandomGenerator rng, int initCount, int opCount)
        {
            PopulateInitKeys(initCount);
            this.opKeys = new VarLenType*[opCount];

            if (!(zipfSettings is null))
            {
                // Use intermediate integer indexes because we can't pass pointers.
                var intIndices = Enumerable.Range(0, initCount).ToArray();
                var opIndices = new Zipf<int>().GenerateOpKeys(zipfSettings, intIndices, opCount);
                for (var ii = 0; ii < opCount; ++ii)
                    this.opKeys[ii] = this.initKeys[opIndices[ii]];
            }
            else
            {
                // Note: copying saves memory on larger keySizes but reading initKeys thrashes the cache, so this
                // is slower than Blittable (and Benchmark).
                for (var ii = 0; ii < opCount; ++ii)
                    opKeys[ii] = this.initKeys[rng.Generate64((ulong)initCount)];
            }
        }

        private void PopulateInitKeys(int initCount)
        {
            this.initKeys = new VarLenType*[initCount];
            int chunkItems = 1024 * 1024;
            int chunkOffset = chunkItems; // Init at end so we allocate on the first loop iteration
            VarLenType.Data* chunkPtr = null;
            for (var ii = 0; ii < initCount; ++ii)
            {
                if (chunkOffset > chunkItems - Globals.KeySize / sizeof(VarLenType.Data))
                {
                    var chunkSize = chunkItems * sizeof(VarLenType.Data);
                    var chunk = Marshal.AllocHGlobal(chunkSize);
                    chunks.Add(chunk);
                    for (var jj = 0; jj < chunkSize / sizeof(long); ++jj)
                        Marshal.WriteInt32(chunk, jj * sizeof(long), 0);
                    chunkPtr = (VarLenType.Data*)chunk.ToPointer();
                    chunkOffset = 0;
                }
                initKeys[ii] = (VarLenType*)(chunkPtr + chunkOffset);
                initKeys[ii]->data.Length = (ushort)VarLenType.GetNewLength(ii, isKey:true);
                initKeys[ii]->data.Value = ii;
                chunkOffset += initKeys[ii]->data.Length;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref VarLenType GetInitKey(int index) => ref *this.initKeys[index];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref VarLenType GetOpKey(int index) => ref *this.opKeys[index];

        public override void Dispose()
        {
            foreach (var intptr in chunks)
                Marshal.FreeHGlobal(intptr);
            chunks.Clear();
        }
    }

    public class VarLenFunctions<TKey> : IFunctions<TKey, VarLenType, Input, VarLenOutput, Empty>
        where TKey : IKey
    {
        #region Upsert
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref TKey key, ref VarLenType src, ref VarLenType dst)
        {
            SingleWriter(ref key, ref src, ref dst);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref TKey key, ref VarLenType src, ref VarLenType dst)
        {
            if (Globals.Verify)
            {
                if (!Globals.IsInitialInsertPhase)
                    dst.Verify(key.Value);
                src.Verify(key.Value);
            }
            src.CopyTo(ref dst, true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref TKey key, ref VarLenType value, Empty ctx)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
        }
        #endregion Upsert

        #region Read
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref TKey key, ref Input input, ref VarLenType value, ref VarLenOutput dst)
            => SingleReader(ref key, ref input, ref value, ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref TKey key, ref Input _, ref VarLenType value, ref VarLenOutput dst)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
            value.CopyTo(ref dst.ValueRef, false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref TKey key, ref Input _, ref VarLenOutput output, Empty ctx, Status status)
        {
            if (Globals.Verify)
                output.ValueRef.Verify(key.Value);
        }
        #endregion Read

        #region RMW
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref TKey key, ref Input input, ref VarLenType value)
            => value.SetInitialValueAndLength(key.Value, input.value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref TKey key, ref Input input, ref VarLenType value)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
            value.SetModified(value.data.Modified + input.value);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref TKey key, ref Input input, ref VarLenType oldValue, ref VarLenType newValue)
        {
            if (Globals.Verify)
                oldValue.Verify(key.Value);
            newValue.SetInitialValueAndLength(key.Value, oldValue.data.Modified + input.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref TKey key, ref Input _, Empty ctx, Status status)
        { }
        #endregion RMW

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref TKey key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");
    }
}

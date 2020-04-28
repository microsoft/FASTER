// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Performance.Common;
using System;
using System.Linq;
using System.Runtime.CompilerServices;

namespace FASTER.PerfTest
{
    public class ObjectValue : IKey
    {
        public static int KeyVectorSize => (Globals.KeySize - Globals.MinDataSize) / BlittableData.SizeOf;
        public static int ValueVectorSize => (Globals.ValueSize - Globals.MinDataSize) / BlittableData.SizeOf;

        internal BlittableData data;
        internal BlittableData[] vector;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetInitialValue(long value, long mod, bool isKey)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            var vectorSize = isKey ? KeyVectorSize : ValueVectorSize;
            if (vectorSize > 0)
                this.vector = new BlittableData[vectorSize];
            if (Globals.Verify)
            {
                // Note: Use VectorSize instead of vector.Length, because we don't allocate
                // if VectorSize is 0 (to save the perf cost)
                for (var ii = 0; ii < vectorSize; ++ii)
                    vector[ii] = this.data;
            }
        }

        public uint Modified
        {
            get => data.Modified;
            set
            {
                data.Modified = value;
                if (Globals.Verify && !(this.vector is null))
                {
                    // Faster to just copy the whole 8 bytes than do the bit shifting to get to the int
                    for (var ii = 0; ii < this.vector.Length; ++ii)
                        this.vector[ii] = data;
                }
            }
        }

        public long Value => this.data.Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectValue CopyForStore()
        {
            // If we are verifying we must deep-copy if copying to the store
            if (!Globals.Verify)
                return this;

            var clone = new ObjectValue();
            clone.SetInitialValue(this.data.Value, this.data.Modified, isKey:false); // this is called only from Functions, for values
            return clone;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            if (!(this.vector is null))
            {
                for (var ii = 0; ii < this.vector.Length; ++ii)
                {
                    if (this.vector[ii] != this.data)
                        throw new ApplicationException($"this.vector[{ii}] ({this.vector[ii]}) != this.data ({this.data})");
                }
            }
            return true;
        }

        public override string ToString() => this.data.ToString();

        public struct EqualityComparer : IFasterEqualityComparer<ObjectValue>
        {
            public long GetHashCode64(ref ObjectValue k) => k.data.GetHashCode64();

            public unsafe bool Equals(ref ObjectValue k1, ref ObjectValue k2)
                => k1.data == k2.data && (!Globals.Verify || (k1.Verify(k1.data.Value) && k2.Verify(k2.data.Value)));
        }
    }

    public class ObjectValueSerializer : BinaryObjectSerializer<ObjectValue>
    {
        protected readonly bool isKey;

        public ObjectValueSerializer(bool isKey) => this.isKey = isKey;

        public override void Deserialize(ref ObjectValue obj)
        {
            obj.data.Read(reader);
            var vectorSize = this.isKey ? ObjectValue.KeyVectorSize : ObjectValue.ValueVectorSize;
            if (vectorSize > 0) {
                obj.vector = new BlittableData[vectorSize];
                for (var ii = 0; ii < vectorSize; ++ii)
                    obj.vector[ii].Read(reader);
            }
        }

        public override void Serialize(ref ObjectValue obj)
        {
            obj.data.Write(writer);
            if (obj.vector != null)
            {
                for (var ii = 0; ii < obj.vector.Length; ++ii)
                    obj.vector[ii].Write(writer);
            }
        }
    }

    public struct ObjectValueOutput
    {
        public ObjectValue Value { get; set; }

        public override string ToString() => this.Value.ToString();
    }

    class ObjectThreadValueRef : IThreadValueRef<ObjectValue, ObjectValueOutput>
    {
        readonly ObjectValue[] values;

        internal ObjectThreadValueRef(int count)
            => values = Enumerable.Range(0, count).Select(ii => new ObjectValue()).ToArray();

        public ref ObjectValue GetRef(int threadIndex) => ref values[threadIndex];

        public ObjectValueOutput GetOutput(int threadIndex) => new ObjectValueOutput();

        public void SetInitialValue(ref ObjectValue valueRef, long value) => valueRef.SetInitialValue(value, 0, isKey:false);

        public void SetUpsertValue(ref ObjectValue valueRef, long value, long mod) => valueRef.SetInitialValue(value, mod, isKey:false);
    }

    internal class ObjectKeyManager : KeyManager<ObjectValue>
    {
        ObjectValue[] initKeys;
        ObjectValue[] opKeys;

        internal ObjectKeyManager(bool verbose) : base(verbose) { }

        internal override void Initialize(ZipfSettings zipfSettings, RandomGenerator rng, int initCount, int opCount)
        {
            this.initKeys = new ObjectValue[initCount];
            for (var ii = 0; ii < initCount; ++ii)
            {
                initKeys[ii] = new ObjectValue();
                initKeys[ii].SetInitialValue(ii, 0, isKey:true);
            }

            if (!(zipfSettings is null))
            {
                this.opKeys = new Zipf<ObjectValue>().GenerateOpKeys(zipfSettings, initKeys, opCount);
            }
            else
            {
                // Note: copying saves memory on larger keySizes but reading initKeys thrashes the cache, so this
                // is slower than Blittable (and Benchmark).
                this.opKeys = new ObjectValue[opCount];
                for (var ii = 0; ii < opCount; ++ii)
                    opKeys[ii] = this.initKeys[rng.Generate64((ulong)initCount)];
            }
        }

        internal override ref ObjectValue GetInitKey(int index) => ref this.initKeys[index];

        internal override ref ObjectValue GetOpKey(int index) => ref this.opKeys[index];

        public override void Dispose() { }
    }

    public class ObjectValueFunctions<TKey> : IFunctions<TKey, ObjectValue, Input, ObjectValueOutput, Empty>
        where TKey: IKey
    {
        #region Upsert
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref TKey key, ref ObjectValue src, ref ObjectValue dst)
        {
            SingleWriter(ref key, ref src, ref dst);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref TKey key, ref ObjectValue src, ref ObjectValue dst)
        {
            if (Globals.Verify)
            {
                if (!Globals.IsInitialInsertPhase)
                    dst.Verify(key.Value);
                src.Verify(key.Value);
            }
            dst = src.CopyForStore();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref TKey key, ref ObjectValue value, Empty ctx)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
        }
        #endregion Upsert

        #region Read
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref TKey key, ref Input input, ref ObjectValue value, ref ObjectValueOutput dst) 
            => SingleReader(ref key, ref input, ref value, ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref TKey key, ref Input _, ref ObjectValue value, ref ObjectValueOutput dst)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
            dst.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref TKey key, ref Input input, ref ObjectValueOutput output, Empty ctx, Status status)
        {
            if (Globals.Verify)
                output.Value.Verify(key.Value);
        }
        #endregion Read

        #region RMW
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref TKey key, ref Input input, ref ObjectValue value)
            => value.SetInitialValue(key.Value, (uint)input.value, isKey:false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref TKey key, ref Input input, ref ObjectValue value)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
            value.Modified += (uint)input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref TKey key, ref Input input, ref ObjectValue oldValue, ref ObjectValue newValue)
        {
            if (Globals.Verify)
                oldValue.Verify(key.Value);
            newValue.SetInitialValue(key.Value, oldValue.Modified + (uint)input.value, isKey:false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref TKey key, ref Input input, Empty ctx, Status status)
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

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Linq;
using System.Runtime.CompilerServices;

namespace FASTER.PerfTest
{
    public class ObjectValue
    {
        public static int VectorSize => (Globals.DataSize - Globals.MinDataSize) / BlittableData.SizeOf;

        internal BlittableData data;
        internal BlittableData[] vector;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            if (VectorSize > 0)
                this.vector = new BlittableData[VectorSize];
            if (Globals.Verify)
            {
                // Note: Use VectorSize instead of vector.Length, because we don't allocate
                // if VectorSize is 0 (to save the perf cost)
                for (var ii = 0; ii < VectorSize; ++ii)
                    vector[ii] = this.data;
            }
        }

        public uint Modified
        {
            get => data.Modified;
            set
            {
                data.Modified = value;
                if (Globals.Verify)
                {
                    // Faster to just copy the whole 8 bytes than do the bit shifting to get to the int
                    for (var ii = 0; ii < VectorSize; ++ii)
                        vector[ii] = data;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectValue CopyForStore()
        {
            // If we are verifying we must deep-copy if copying to the store
            if (!Globals.Verify)
                return this;

            var clone = new ObjectValue();
            clone.SetInitialValue(this.data.Value, this.data.Modified);
            return clone;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            for (var ii = 0; ii < VectorSize; ++ii)
            {
                if (vector[ii] != this.data)
                    throw new ApplicationException($"this.vector[{ii}] ({this.vector[ii]}) != this.data ({this.data})");
            }
        }

        public override string ToString() => this.data.ToString();
    }

    public class ObjectValueSerializer : BinaryObjectSerializer<ObjectValue>
    {
        public override void Deserialize(ref ObjectValue obj)
        {
            obj.data.Read(reader);
            if (ObjectValue.VectorSize > 0) {
                obj.vector = new BlittableData[ObjectValue.VectorSize];
                for (var ii = 0; ii < ObjectValue.VectorSize; ++ii)
                    obj.vector[ii].Read(reader);
            }
        }

        public override void Serialize(ref ObjectValue obj)
        {
            obj.data.Write(writer);
            for (var ii = 0; ii < ObjectValue.VectorSize; ++ii)
                obj.vector[ii].Write(writer);
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

        public void SetInitialValue(ref ObjectValue valueRef, long value) => valueRef.SetInitialValue(value, 0);

        public void SetUpsertValue(ref ObjectValue valueRef, long value, long mod) => valueRef.SetInitialValue(value, mod);
    }

    public class ObjectValueFunctions : IFunctions<Key, ObjectValue, Input, ObjectValueOutput, Empty>
    {
        #region Upsert
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref ObjectValue src, ref ObjectValue dst)
        {
            SingleWriter(ref key, ref src, ref dst);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref ObjectValue src, ref ObjectValue dst)
        {
            if (Globals.Verify)
            {
                if (!Globals.IsInitialInsertPhase)
                    dst.Verify(key.key);
                src.Verify(key.key);
            }
            dst = src.CopyForStore();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref Key key, ref ObjectValue value, Empty ctx)
        {
            if (Globals.Verify)
                value.Verify(key.key);
        }
        #endregion Upsert

        #region Read
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref ObjectValue value, ref ObjectValueOutput dst) 
            => SingleReader(ref key, ref input, ref value, ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input _, ref ObjectValue value, ref ObjectValueOutput dst)
        {
            if (Globals.Verify)
                value.Verify(key.key);
            dst.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref Key key, ref Input input, ref ObjectValueOutput output, Empty ctx, Status status)
        {
            if (Globals.Verify)
                output.Value.Verify(key.key);
        }
        #endregion Read

        #region RMW
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref ObjectValue value)
            => value.SetInitialValue(key.key, (uint)input.value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref ObjectValue value)
        {
            if (Globals.Verify)
                value.Verify(key.key);
            value.Modified += (uint)input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref ObjectValue oldValue, ref ObjectValue newValue)
        {
            if (Globals.Verify)
                oldValue.Verify(key.key);
            newValue.SetInitialValue(key.key, oldValue.Modified + (uint)input.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
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

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
        public static int VectorSize => (Globals.DataSize - Globals.MinDataSize) / sizeof(long);

        internal long value;
        internal long[] vector;
    }

    public class ObjectValueSerializer : BinaryObjectSerializer<ObjectValue>
    {
        public override void Deserialize(ref ObjectValue obj)
        {
            obj.value = reader.ReadInt64();
            if (ObjectValue.VectorSize > 0) {
                obj.vector = new long[ObjectValue.VectorSize];
                for (var ii = 0; ii < ObjectValue.VectorSize; ++ii)
                    obj.vector[ii] = reader.ReadInt64();
            }
        }

        public override void Serialize(ref ObjectValue obj)
        {
            writer.Write(obj.value);
            for (var ii = 0; ii < ObjectValue.VectorSize; ++ii)
                writer.Write(obj.vector[ii]);
        }
    }

    public struct ObjectValueOutput
    {
        public ObjectValue Value { get; set; }
    }

    class GetObjectValueRef : IGetValueRef<ObjectValue, ObjectValueOutput>
    {
        readonly ObjectValue[] values;

        internal GetObjectValueRef(int count)
            => values = Enumerable.Range(0, count).Select(ii => new ObjectValue()).ToArray();

        public ref ObjectValue GetRef(int threadIndex) => ref values[threadIndex];

        public ObjectValueOutput GetOutput(int threadIndex) => new ObjectValueOutput();
    }

    public class ObjectValueFunctions : IFunctions<Key, ObjectValue, Input, ObjectValueOutput, Empty>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref ObjectValue value, ref ObjectValueOutput dst) 
            => dst.Value = value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref ObjectValue src, ref ObjectValue dst)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref ObjectValue value)
            => value.value = input.value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref ObjectValue value)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref ObjectValue oldValue, ref ObjectValue newValue)
            => newValue.value = input.value + oldValue.value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref Key key, ref Input input, ref ObjectValueOutput output, Empty ctx, Status status)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input input, ref ObjectValue value, ref ObjectValueOutput dst) 
            => dst.Value = value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref ObjectValue src, ref ObjectValue dst) 
            => dst = src;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref Key key, ref ObjectValue value, Empty ctx)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref Key key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");
    }
}
   
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// For the long value
#pragma warning disable IDE0051 // Remove unused private members

namespace FASTER.PerfTest
{
    public interface IBlittableValue   // Needed for RMW
    {
        long Value { get; set; }
    }

    [StructLayout(LayoutKind.Sequential, Size = 8)]
    public struct BlittableValue8 : IBlittableValue
    {
        public long Value { get => this.value; set => this.value = value; }

        internal long value;
    }

    [StructLayout(LayoutKind.Sequential, Size = 16)]
    public struct BlittableValue16 : IBlittableValue
    {
        public long Value { get => this.value; set => this.value = value; }

        public long value;
        public readonly long extra1;
    }

    [StructLayout(LayoutKind.Sequential, Size = 32)]
    public struct BlittableValue32 : IBlittableValue
    {
        public long Value { get => this.value; set => this.value = value; }

        public long value;
        public readonly long extra1, extra2, extra3;
    }

    [StructLayout(LayoutKind.Sequential, Size = 64)]
    public struct BlittableValue64 : IBlittableValue
    {
        public long Value { get => this.value; set => this.value = value; }

        public long value;
        public readonly long extra1, extra2, extra3, extra4, extra5, extra6, extra7;
    }

    [StructLayout(LayoutKind.Sequential, Size = 128)]
    public struct BlittableValue128 : IBlittableValue
    {
        public long Value { get => this.value; set => this.value = value; }

        public long value;
        public readonly long extra1, extra2, extra3, extra4, extra5, extra6, extra7;
        public readonly long extra10, extra11, extra12, extra13, extra14, extra15, extra16, extra17;
    }

    [StructLayout(LayoutKind.Sequential, Size = 256)]
    public struct BlittableValue256 : IBlittableValue
    {
        public long Value { get => this.value; set => this.value = value; }

        public long value;
        public readonly long extra1, extra2, extra3, extra4, extra5, extra6, extra7;
        public readonly long extra10, extra11, extra12, extra13, extra14, extra15, extra16, extra17;
        public readonly long extra20, extra21, extra22, extra23, extra24, extra25, extra26, extra27;
        public readonly long extra30, extra31, extra32, extra33, extra34, extra35, extra36, extra37;
    }

    public struct BlittableOutput<TBlittableValue>
    {
        public TBlittableValue Value { get; set; }
    }

    class GetBlittableValueRef<TBlittableValue> : IGetValueRef<TBlittableValue, BlittableOutput<TBlittableValue>>
        where TBlittableValue : new()
    {
        readonly TBlittableValue[] values;

        internal GetBlittableValueRef(int count)
            => values = Enumerable.Range(0, count).Select(ii => new TBlittableValue()).ToArray();

        public ref TBlittableValue GetRef(int threadIndex) => ref values[threadIndex];

        public BlittableOutput<TBlittableValue> GetOutput(int threadIndex) => new BlittableOutput<TBlittableValue>();
    }

    public class BlittableFunctions<TBlittableValue> : IFunctions<Key, TBlittableValue, Input, BlittableOutput<TBlittableValue>, Empty>
        where TBlittableValue : IBlittableValue
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref TBlittableValue value, ref BlittableOutput<TBlittableValue> dst)
            => dst.Value = value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref TBlittableValue src, ref TBlittableValue dst)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref TBlittableValue value)
            => value.Value = input.value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref TBlittableValue value)
        {
            value.Value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref TBlittableValue oldValue, ref TBlittableValue newValue)
            => newValue.Value = input.value + oldValue.Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref Key key, ref Input input, ref BlittableOutput<TBlittableValue> output, Empty ctx, Status status)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input input, ref TBlittableValue value, ref BlittableOutput<TBlittableValue> dst)
            => dst.Value = value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref TBlittableValue src, ref TBlittableValue dst)
            => dst = src;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref Key key, ref TBlittableValue value, Empty ctx)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref Key key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");
    }
}

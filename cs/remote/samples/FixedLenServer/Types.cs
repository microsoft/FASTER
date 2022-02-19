// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.core;

namespace FasterFixedLenServer
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key : IFasterEqualityComparer<Key>
    {
        [FieldOffset(0)]
        public long value;

        public override string ToString()
        {
            return "{ " + value + " }";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref Key k)
        {
            return Utility.GetHashCode(k.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.value == k2.value;
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Value
    {
        [FieldOffset(0)]
        public long value;
    }

    public struct Input
    {
        public long value;
    }

    [StructLayout(LayoutKind.Explicit)]
    public struct Output
    {
        [FieldOffset(0)]
        public Value value;
    }


    public struct Functions : IFunctions<Key, Value, Input, Output, long>
    {
        // Callbacks
        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status, RecordMetadata recordMetadata) { }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status, RecordMetadata recordMetadata) { }

        public void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, long ctx) { }

        public void DeleteCompletionCallback(ref Key key, long ctx) { }

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
            => Debug.WriteLine($"Session {sessionID} ({(sessionName ?? "null")}) reports persistence until {commitPoint.UntilSerialNo}");

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, WriteReason reason) => dst = src;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            value.value = input.value;
            output.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Add(ref value.value, input.value);
            output.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
        {
            newValue.value = input.value + oldValue.value;
            output.value = newValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) { }

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output) => true;

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) { }

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output) => true;

        public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) { }

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address) { }

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, WriteReason reason) { }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) => true;

        public void DisposeKey(ref Key key) { }

        public void DisposeValue(ref Value value) { }
    }
}

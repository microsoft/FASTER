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

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
            => Debug.WriteLine($"Session {sessionID} ({(sessionName ?? "null")}) reports persistence until {commitPoint.UntilSerialNo}");

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
        {
            value.value = input.value;
            output.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
        {
            Interlocked.Add(ref value.value, input.value);
            output.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
        {
            newValue.value = input.value + oldValue.value;
            output.value = newValue;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) => true;

        public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) => true;

        public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }
        public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }
        public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { }
        public void DisposeDeserializedFromDisk(ref Key key, ref Value value) { }
        public void DisposeForRevivification(ref Key key, ref Value value, bool disposeKey) { }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.benchmark
{
    public struct Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public readonly void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public readonly void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public readonly void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
            Debug.WriteLine($"Session {sessionID} ({(sessionName ?? "null")}) reports persistence until {commitPoint.UntilSerialNo}");
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public readonly bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { value = default; return true; }

        public readonly bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
        {
            value.value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
        {
            newValue.value = input.value + oldValue.value;
            return true;
        }

        public readonly void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }

        public readonly bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;

        public readonly void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }

        public readonly bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public readonly void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }

        public readonly void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public readonly void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        public readonly void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }
        public readonly void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }
        public readonly void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { }
        public readonly void DisposeDeserializedFromDisk(ref Key key, ref Value value) { }
    }
}

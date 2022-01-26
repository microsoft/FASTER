// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.benchmark
{
    public struct Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref Key key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

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

        public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) { value = default; }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, WriteReason reason)
        {
            dst = src;
        }

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
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
        {
            newValue.value = input.value + oldValue.value;
        }

        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) => true;

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output) => true;

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) { }

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output) => true;

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address) { }

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, WriteReason reason) { }

        public void DisposeKey(ref Key key) { }

        /// <summary>
        /// Dispose the value; for example, in evicted log records. FASTER assumes deep-copy semantics such as cloning or refcounting. 
        /// </summary>
        /// <param name="value"></param>
        public void DisposeValue(ref Value value) { }
    }
}

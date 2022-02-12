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

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
            Debug.WriteLine($"Session {sessionID} ({(sessionName ?? "null")}) reports persistence until {commitPoint.UntilSerialNo}");
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

        public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) { value = default; }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo UpdateInfo, long address, WriteReason reason)
        {
            dst = src;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
        {
            value.value = input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
        {
            newValue.value = input.value + oldValue.value;
            return true;
        }

        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) => true;

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref UpdateInfo updateInfo) => true;

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) { }

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref UpdateInfo updateInfo) => true;

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) { }

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address, WriteReason reason) { }

        public void DisposeKey(ref Key key) { }

        /// <summary>
        /// Dispose the value; for example, in evicted log records. FASTER assumes deep-copy semantics such as cloning or refcounting. 
        /// </summary>
        /// <param name="value"></param>
        public void DisposeValue(ref Value value) { }
    }
}

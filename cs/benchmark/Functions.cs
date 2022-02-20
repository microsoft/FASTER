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
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { value = default; }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo)
        {
            value.value = input.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo)
        {
            newValue.value = input.value + oldValue.value;
        }

        public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref UpdateInfo updateInfo) => true;

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref UpdateInfo updateInfo) => true;

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason) { }

        public void DisposeKey(ref Key key) { }

        /// <summary>
        /// Dispose the value; for example, in evicted log records. FASTER assumes deep-copy semantics such as cloning or refcounting. 
        /// </summary>
        /// <param name="value"></param>
        public void DisposeValue(ref Value value) { }
    }
}

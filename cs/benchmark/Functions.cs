// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.CompilerServices;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.benchmark
{
    public struct Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        readonly bool locking;

        public Functions(bool locking) => this.locking = locking;

        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx)
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
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        {
            dst.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref Value src, ref Value dst)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.value = input.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
        {
            newValue.value = input.value + oldValue.value;
        }

        public bool SupportsLocking => locking;

        public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext)
        {
            recordInfo.SpinLock();
        }

        public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext)
        {
            recordInfo.Unlock();
            return true;
        }
    }
}

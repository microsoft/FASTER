// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using FASTER.core;
using System.Collections.Generic;

namespace FASTER.benchmark
{
    public struct Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, ref Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, ref Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Value value, ref Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref Key key, ref Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref Empty ctx)
        {
            dst.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref Empty ctx)
        {
            dst.value = value;
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref Value src, ref Value dst, ref Empty ctx)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, ref Empty ctx)
        {
            dst = src;
            return true;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Empty ctx)
        {
            value.value = input.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Empty ctx)
        {
            value.value += input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Empty ctx)
        {
            newValue.value = input.value + oldValue.value;
        }
    }
}

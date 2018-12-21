// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StructSample
{
    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public class Sample1Funcs : IFunctions<long, long, long, long, Empty>
    {
        // Read functions
        public void SingleReader(ref long key, ref long input, ref long value, ref long dst) => dst = value;
        public void ConcurrentReader(ref long key, ref long input, ref long value, ref long dst) => dst = value;

        // Write functions
        public void SingleWriter(ref long key, ref long src, ref long dst) => dst = src;
        public void ConcurrentWriter(ref long key, ref long src, ref long dst) => dst = src;

        // RMW functions
        public void InitialUpdater(ref long key, ref long input, ref long value) => value = input;
        public void CopyUpdater(ref long key, ref long input, ref long oldv, ref long newv) => newv = oldv + input;
        public void InPlaceUpdater(ref long key, ref long input, ref long value) => value += input;

        // Completion callbacks
        public void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status s) { }
        public void UpsertCompletionCallback(ref long key, ref long value, Empty ctx) { }
        public void RMWCompletionCallback(ref long key, ref long input, Empty ctx, Status s) { }
        public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
    }


    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public class Sample2Funcs : IFunctions<Key, Value, Input, Output, Empty>
    {
        // Read functions
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst) => dst.value = value;
        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst) => dst.value = value;

        // Write functions
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;
        public void ConcurrentWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        // RMW functions
        public void InitialUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
        public void InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
        }

        // Completion callbacks
        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status) { }
        public void UpsertCompletionCallback(ref Key key, ref Value output, Empty ctx) { }
        public void RMWCompletionCallback(ref Key key, ref Input output, Empty ctx, Status status) { }
        public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
    }
}

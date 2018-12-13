// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StructSampleCore
{
    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public class Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input output, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Value output, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref Key key, ref Value src, ref Value dst)
        {
            dst = src;
        }

        public void ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
        {
            dst = src;
        }

        // RMW functions
        public void InitialUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        public void InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
        }

        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }
}

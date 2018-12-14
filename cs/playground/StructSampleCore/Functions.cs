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
    public class Functions : IFunctions<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        public void RMWCompletionCallback(ref KeyStruct key, ref InputStruct output, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref KeyStruct key, ref ValueStruct output, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst = src;
        }

        public void ConcurrentWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst = src;
        }

        // RMW functions
        public void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        public void InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
        }

        public void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }
}

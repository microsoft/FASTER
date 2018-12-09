// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StructSampleCore
{
    /// <summary>
    /// Callback functions for FASTER operations customized to user types
    /// See \cs\src\core\Index\UserCode\Functions.cs for template details
    /// </summary>
    public class Functions : IFunctions<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        public void RMWCompletionCallback(ref KeyStruct key, ref InputStruct output, ref Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, ref Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref KeyStruct key, ref ValueStruct output, ref Empty ctx)
        {
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} repors persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst.AcquireWriteLock();
            dst = src;
            dst.ReleaseWriteLock();
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialValueLength(ref KeyStruct key, ref InputStruct input)
        {
            return default(ValueStruct).GetLength();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.AcquireWriteLock();
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            value.ReleaseWriteLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }
}

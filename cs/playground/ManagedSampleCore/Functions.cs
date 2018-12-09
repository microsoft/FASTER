// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StructSample
{
    public unsafe class Functions
    {
        public static void RMWCompletionCallback(KeyStruct* key, InputStruct* output, Empty* ctx, Status status)
        {
        }

        public static void ReadCompletionCallback(KeyStruct* key, InputStruct* input, OutputStruct* output, Empty* ctx, Status status)
        {
        }

        public static void UpsertCompletionCallback(KeyStruct* key, ValueStruct* output, Empty* ctx)
        {
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} repors persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleReader(KeyStruct* key, InputStruct* input, ValueStruct* value, OutputStruct* dst)
        {
            ValueStruct.Copy(value, (ValueStruct*)dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(KeyStruct* key, InputStruct* input, ValueStruct* value, OutputStruct* dst)
        {
            ValueStruct.AcquireReadLock(value);
            ValueStruct.Copy(value, (ValueStruct*)dst);
            ValueStruct.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(KeyStruct* key, ValueStruct* src, ValueStruct* dst)
        {
            ValueStruct.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(KeyStruct* key, ValueStruct* src, ValueStruct* dst)
        {
            ValueStruct.AcquireWriteLock(dst);
            ValueStruct.Copy(src, dst);
            ValueStruct.ReleaseWriteLock(dst);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(KeyStruct* key, InputStruct* input)
        {
            return ValueStruct.GetLength(default(ValueStruct*));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(KeyStruct* key, InputStruct* input, ValueStruct* value)
        {
            ValueStruct.Copy((ValueStruct*)input, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(KeyStruct* key, InputStruct* input, ValueStruct* value)
        {
            ValueStruct.AcquireWriteLock(value);
            value->vfield1 += input->ifield1;
            value->vfield2 += input->ifield2;
            ValueStruct.ReleaseWriteLock(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(KeyStruct* key, InputStruct* input, ValueStruct* oldValue, ValueStruct* newValue)
        {
            newValue->vfield1 = oldValue->vfield1 + input->ifield1;
            newValue->vfield2 = oldValue->vfield2 + input->ifield2;
        }
    }
}

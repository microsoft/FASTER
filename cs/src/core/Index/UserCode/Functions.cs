// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

#define REPLACE_STORE
//#define COUNT_STORE

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;

namespace FASTER.core
{
    public struct Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, ref Context ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, ref Context ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Value value, ref Context ctx)
        {
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }
#if COUNT_STORE
        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleReader(Key* key, Input* input, Value* value, Output* dst)
        {
            Value.Copy(value, (Value*)dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(Key* key, Input* input, Value* value, Output* dst)
        {
            Value.AcquireReadLock(value);
            Value.Copy(value, (Value*)dst);
            Value.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(Key* key, Value* src, Value* dst)
        {
            Value.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(Key* key, Value* src, Value* dst)
        {
            Value.AcquireWriteLock(dst);
            Value.Copy(src, dst);
            Value.ReleaseWriteLock(dst);
        }

        // RMW functions

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(Key* key, Input* input)
        {
            return Value.GetLength(default(Value*));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(Key* key, Input* input, Value* value)
        {
            value->value = ((Value*)input)->value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(Key* key, Input* input, Value* addr)
        {
            Interlocked.Add(ref addr->value, ((Value*)input)->value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(Key* key, Input* input, Value* oldValue, Value* newValue)
        {
            newValue->value = oldValue->value + ((Value*)input)->value;
        }

#elif REPLACE_STORE

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
            src.ShallowCopy(ref dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
        {
            src.ShallowCopy(ref dst);
        }

        // RMW functions

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialValueLength(ref Key key, ref Input input)
        {
            return default(Value).GetLength();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.value = input.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.AcquireWriteLock();
            value.value += input.value;
            value.ReleaseWriteLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
        {
            newValue.value = input.value + oldValue.value;
        }
#endif
    }
}

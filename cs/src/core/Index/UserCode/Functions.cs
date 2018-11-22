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
    public unsafe struct Functions
    {
        public static void RMWCompletionCallback(ref Key key, Input* input, Context* ctx, Status status)
        {
        }

        public static void ReadCompletionCallback(ref Key key, Input* input, Output* output, Context* ctx, Status status)
        {
        }

        public static void UpsertCompletionCallback(ref Key key, Value* value, Context* ctx)
        {
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} repors persistence until {1}", thread_id, serial_num);
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
        public static void SingleReader(ref Key key, Input* input, Value* value, Output* dst)
        {
            Value.Copy(value, (Value*)dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(ref Key key, Input* input, Value* value, Output* dst)
        {
            Value.AcquireReadLock(value);
            Value.Copy(value, (Value*)dst);
            Value.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(ref Key key, Value* src, Value* dst)
        {
            Value.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(ref Key key, Value* src, Value* dst)
        {
            Value.AcquireWriteLock(dst);
            Value.Copy(src, dst);
            Value.ReleaseWriteLock(dst);
        }

        // RMW functions

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(ref Key key, Input* input)
        {
            return Value.GetLength(default(Value*));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(ref Key key, Input* input, Value* value)
        {
            Value.Copy((Value*)input, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(ref Key key, Input* input, Value* value)
        {
            Value.AcquireWriteLock(value);
            Value.Copy((Value*)input, value);
            Value.ReleaseWriteLock(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(ref Key key, Input* input, Value* oldValue, Value* newValue)
        {
            Value.Copy((Value*)input, newValue);
        }
#endif
    }
}

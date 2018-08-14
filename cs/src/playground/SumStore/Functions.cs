// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SumStore
{
    public unsafe class Functions
    {
        public static void RMWCompletionCallback(AdId* key, Input* input, Empty* ctx)
        {
        }

        public static void ReadCompletionCallback(AdId* key, Input* input, Output* output, Empty* ctx)
        {
        }

        public static void UpsertCompletionCallback(AdId* key, NumClicks* input, Empty* ctx)
        {
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Console.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleReader(AdId* key, Input* input, NumClicks* value, Output* dst)
        {
            NumClicks.Copy(value, (NumClicks*)dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(AdId* key, Input* input, NumClicks* value, Output* dst)
        {
            NumClicks.AcquireReadLock(value);
            NumClicks.Copy(value, (NumClicks*)dst);
            NumClicks.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(AdId* key, NumClicks* src, NumClicks* dst)
        {
            NumClicks.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(AdId* key, NumClicks* src, NumClicks* dst)
        {
            NumClicks.AcquireWriteLock(dst);
            NumClicks.Copy(src, dst);
            NumClicks.ReleaseWriteLock(dst);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(AdId* key, Input* input)
        {
            return NumClicks.GetLength(default(NumClicks*));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(AdId* key, Input* input, NumClicks* value)
        {
            NumClicks.Copy(&input->numClicks, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(AdId* key, Input* input, NumClicks* value)
        {
            Interlocked.Add(ref value->numClicks, input->numClicks.numClicks);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(AdId* key, Input* input, NumClicks* oldValue, NumClicks* newValue)
        {
            newValue->numClicks += oldValue->numClicks + input->numClicks.numClicks;
        }
    }
}

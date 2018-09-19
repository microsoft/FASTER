// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define GENERIC_BLIT_VALUE

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    [FASTER.core.Roslyn.TypeKind("user")]
    public unsafe class MixedFunctionsWrapper
    {
        public static MixedUserFunctions userFunctions;

        public static void RMWCompletionCallback(MixedKeyWrapper* key, MixedInputWrapper* input, MixedContextWrapper* ctx, Status status)
        {
            userFunctions.RMWCompletionCallback(
                UserType.Convert(ctx), status);

            UserType.FreeFromContext(key);
            UserType.FreeFromContext(input);
            UserType.FreeFromContext(ctx);
        }

        public static void ReadCompletionCallback(MixedKeyWrapper* key, MixedInputWrapper* input, MixedOutputWrapper* output, MixedContextWrapper* ctx, Status status)
        {
            userFunctions.ReadCompletionCallback(
                UserType.Convert(ctx),
                UserType.Convert(output), status);

            UserType.FreeFromContext(key);
            UserType.FreeFromContext(input);
            UserType.FreeFromContext(output);
            UserType.FreeFromContext(ctx);
        }

        public static void UpsertCompletionCallback(MixedKeyWrapper* key, MixedValueWrapper* value, MixedContextWrapper* ctx)
        {
            userFunctions.UpsertCompletionCallback(
                UserType.Convert(ctx));

            UserType.FreeFromContext(key);
            UserType.FreeFromContext(value);
            UserType.FreeFromContext(ctx);
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleReader(MixedKeyWrapper* key, MixedInputWrapper* input, MixedValueWrapper* value, MixedOutputWrapper* dst)
        {
            userFunctions.Reader(
                UserType.Convert(key), 
                UserType.Convert(input),
                UserType.Convert(value),
                ref UserType.Convert(dst));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(MixedKeyWrapper* key, MixedInputWrapper* input, MixedValueWrapper* value, MixedOutputWrapper* dst)
        {
            MixedValueWrapper.AcquireReadLock(value);

            userFunctions.Reader(
                UserType.Convert(key),
                UserType.Convert(input),
                UserType.Convert(value),
                ref UserType.Convert(dst));

            MixedValueWrapper.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(MixedKeyWrapper* key, MixedValueWrapper* src, MixedValueWrapper* dst)
        {
            MixedValueWrapper.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(MixedKeyWrapper* key, MixedValueWrapper* src, MixedValueWrapper* dst)
        {
            MixedValueWrapper.AcquireWriteLock(dst);
            MixedValueWrapper.Copy(src, dst);
            MixedValueWrapper.ReleaseWriteLock(dst);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(MixedKeyWrapper* key, MixedInputWrapper* input)
        {
            return MixedValueWrapper.GetLength(null);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(MixedKeyWrapper* key, MixedInputWrapper* input, MixedValueWrapper* value)
        {
            UserType.Initialize(value);

            userFunctions.InitialUpdater(
                UserType.Convert(key),
                UserType.Convert(input),
                ref UserType.Convert(value));

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(MixedKeyWrapper* key, MixedInputWrapper* input, MixedValueWrapper* value)
        {
            MixedValueWrapper.AcquireWriteLock(value);

            userFunctions.InPlaceUpdater(
                UserType.Convert(key),
                UserType.Convert(input),
                ref UserType.Convert(value));

            MixedValueWrapper.ReleaseWriteLock(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(MixedKeyWrapper* key, MixedInputWrapper* input, MixedValueWrapper* oldValue, MixedValueWrapper* newValue)
        {
            UserType.Initialize(newValue);

            userFunctions.CopyUpdater(
                UserType.Convert(key),
                UserType.Convert(input),
                UserType.Convert(oldValue),
                ref UserType.Convert(newValue));
        }
    }
}

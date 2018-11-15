// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------------------------
using System;

namespace FASTER.core
{
    internal static class HashTableManager
    {
        private const bool PersistDll =
#if DEBUG
            true
#else

            false
#endif
            ;
        private const bool OptimizeCode =
#if DEBUG
            false
#else

            true
#endif
            ;
        public static TIFaster GetFasterHashTable<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>
            (long size, LogSettings logSettings, CheckpointSettings checkpointSettings, bool persistDll = PersistDll, bool optimizeCode = OptimizeCode)
        {
            var s = Roslyn.FasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>.GenerateFasterHashTableClass(persistDll, optimizeCode);
            var t = s.Item1;
            var instance = Activator.CreateInstance(t, size, logSettings, checkpointSettings);
            return (TIFaster)instance;
        }

        public static IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>
            GetMixedManagedFasterHashTable<TKey, TValue, TInput, TOutput, TContext, TFunctions>
            (long size, TFunctions functions, 
            LogSettings logSettings, CheckpointSettings checkpointSettings, bool treatValueAsAtomic,
            bool persistDll = PersistDll, bool optimizeCode = OptimizeCode)
            where TFunctions : IUserFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            var s = Roslyn.MixedBlitManagedFasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions>.GenerateGenericFasterHashTableClass(treatValueAsAtomic, persistDll, optimizeCode);
            var t = s.Item1;
            var instance = Activator.CreateInstance(t, size, functions, logSettings, checkpointSettings);
            return (IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>)instance;
        }
    }
}

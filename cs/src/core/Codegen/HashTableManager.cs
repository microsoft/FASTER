// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------------------------
using System;

namespace FASTER.core
{
    public static class HashTableManager
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
            (long size, IDevice logDevice, IDevice objectLogDevice, string checkpointDir, long LogTotalSizeBytes = 17179869184, double LogMutableFraction = 0.9, int LogPageSizeBits = 25, bool persistDll = PersistDll, bool optimizeCode = OptimizeCode)
        {
            var s = Roslyn.FasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>.GenerateFasterHashTableClass(persistDll, optimizeCode, LogTotalSizeBytes, LogMutableFraction, LogPageSizeBits);
            var t = s.Item1;
            var instance = Activator.CreateInstance(t, size, logDevice, objectLogDevice, checkpointDir);
            return (TIFaster)instance;
        }

        public static IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>
            GetMixedManagedFasterHashTable<TKey, TValue, TInput, TOutput, TContext, TFunctions>
            (long size, IDevice logDevice, IDevice objectLogDevice, string checkpointDir, TFunctions functions, bool treatValueAsAtomic, long LogTotalSizeBytes = 17179869184, double LogMutableFraction = 0.9, int LogPageSizeBits = 25, bool persistDll = PersistDll, bool optimizeCode = OptimizeCode)
            where TFunctions : IUserFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            var s = Roslyn.MixedBlitManagedFasterHashTableCompiler<TKey, TValue, TInput, TOutput, TContext, TFunctions>.GenerateGenericFasterHashTableClass(size, logDevice, treatValueAsAtomic, persistDll, optimizeCode);
            var t = s.Item1;
            var instance = Activator.CreateInstance(t, size, logDevice, objectLogDevice, checkpointDir, functions, LogTotalSizeBytes, LogMutableFraction, LogPageSizeBits);
            return (IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>)instance;
        }
    }
}

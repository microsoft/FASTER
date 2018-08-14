// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq.Expressions;
using static FASTER.core.Roslyn.Helper;

namespace FASTER.core
{
    public static class FASTERFactory
    {
        // Statically generate default instance to speed up further invocations

        // private static IManagedFAST<BlittableKey, BlittableValue, BlittableInput, BlittableOutput, BlittableContext>
        // dummyToGetCodeGenFaster = HashTableManager.GetManagedFasterHashTable<BlittableKey, BlittableValue, BlittableInput, BlittableOutput, BlittableContext, UserFunctions>(128, new NullDevice(), new UserFunctions(), false);
        // static FASTERFactory()
        // {
        //    if (dummyToGetCodeGenFaster == null) throw new Exception();
        // }

        public static IDevice CreateLogDevice(string logPath, long segmentSize = 1L << 30)
        {
            IDevice logDevice = new NullDevice();
            if (!String.IsNullOrWhiteSpace(logPath))
                logDevice = new WrappedDevice(new SegmentedLocalStorageDevice(logPath, segmentSize, false, false, true, false));

            return logDevice;
        }

        public static TIFaster
            Create<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>(
            long indexSizeBuckets, IDevice logDevice, 
            long LogTotalSizeBytes = 17179869184, double LogMutableFraction = 0.9, int LogPageSizeBits = 25,
            string checkpointDir = null
            )
        {
                
            return
                HashTableManager.GetFasterHashTable
                                <TKey, TValue, TInput, TOutput,
                                TContext, TFunctions, TIFaster>
                                (indexSizeBuckets, logDevice, checkpointDir, 
                                LogTotalSizeBytes, LogMutableFraction, LogPageSizeBits);
        }

        public static IManagedFAST<TKey, TValue, TInput, TOutput, TContext>
            Create<TKey, TValue, TInput, TOutput, TContext, TFunctions>
            (long indexSizeBuckets, IDevice logDevice, 
            TFunctions functions,
            long LogTotalSizeBytes = 17179869184, double LogMutableFraction = 0.9, int LogPageSizeBits = 25,
            bool treatValueAsAtomic = false, string checkpointDir = null)
            where TFunctions : IUserFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            return HashTableManager.GetMixedManagedFasterHashTable<TKey, TValue, TInput, TOutput, TContext, TFunctions>(indexSizeBuckets, logDevice, checkpointDir, functions, treatValueAsAtomic, LogTotalSizeBytes, LogMutableFraction, LogPageSizeBits);
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq.Expressions;
using static FASTER.core.Roslyn.Helper;

namespace FASTER.core
{
    public static class FasterFactory
    {

        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="segmentSize">Size of each chunk of the log</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, long segmentSize = 1L << 30)
        {
            IDevice logDevice = new NullDevice();
            if (!String.IsNullOrWhiteSpace(logPath))
                logDevice = new WrappedDevice(new SegmentedLocalStorageDevice(logPath, segmentSize, false, false, true, false));

            return logDevice;
        }

        /// <summary>
        /// Generate and return instance of FASTER based on specified parameters
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TContext">Context type</typeparam>
        /// <typeparam name="TFunctions">Callback Functions</typeparam>
        /// <typeparam name="TIFaster">Interface of returned FASTER instance</typeparam>
        /// <param name="indexSizeBuckets">Numer of buckets</param>
        /// <param name="logDevice">Log device</param>
        /// <param name="LogTotalSizeBytes">Total size of log (bytes)</param>
        /// <param name="LogMutableFraction">Fraction of log that is mutable</param>
        /// <param name="LogPageSizeBits">Size of each log page</param>
        /// <param name="checkpointDir">Directory for recovery checkpoints</param>
        /// <returns>Instance of FASTER</returns>
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

        /// <summary>
        /// Generate and return instance of FASTER based on specified parameters
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TContext">Context type</typeparam>
        /// <typeparam name="TFunctions">Callback Functions</typeparam>
        /// <param name="indexSizeBuckets">Numer of buckets</param>
        /// <param name="logDevice">Log device</param>
        /// <param name="functions">Instance of functions</param>
        /// <param name="LogTotalSizeBytes">Total size of log (bytes)</param>
        /// <param name="LogMutableFraction">Fraction of log that is mutable</param>
        /// <param name="LogPageSizeBits">Size of each log page</param>
        /// <param name="treatValueAsAtomic">Treat value as atomic, no auto-locking by FASTER</param>
        /// <param name="checkpointDir">Directory for recovery checkpoints</param>
        /// <returns>Instance of FASTER</returns>
        public static IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>
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

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using static FASTER.core.Roslyn.Helper;

namespace FASTER.core
{
    /// <summary>
    /// Configuration parameters for hybrid log
    /// </summary>
    public class LogParameters
    {
        /// <summary>
        /// Size of page, in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int SegmentSizeBits = 30;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int MemorySizeBits = 34;

        /// <summary>
        /// Fraction of log marked as mutable (in-place updates)
        /// </summary>
        public double MutableFraction = 0.9;
    }

    /// <summary>
    /// Checkpoint type
    /// </summary>
    public enum CheckpointType
    {
        /// <summary>
        /// Take separate snapshot of in-memory
        /// </summary>
        Snapshot,

        /// <summary>
        /// Flush current log (move read-only to tail)
        /// </summary>
        FoldOver
    }

    /// <summary>
    /// Factory to create FASTER objects
    /// </summary>
    public static class FasterFactory
    {
        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="segmentSize">Size of each chunk of the log</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, long segmentSize = 1L << 30, bool deleteOnClose = false)
        {
            IDevice logDevice = new NullDevice();
            if (String.IsNullOrWhiteSpace(logPath))
                return logDevice;
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logDevice = new ManagedLocalStorageDevice(logPath + ".log", segmentSize, true, false, deleteOnClose);
            }
#else
            {
                logDevice = new LocalStorageDevice(logPath + ".log", segmentSize, true, false, deleteOnClose);
            }
#endif
            return logDevice;
        }

        /// <summary>
        /// Create a storage device for the object log (for non-blittable objects)
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateObjectLogDevice(string logPath, bool deleteOnClose = false)
        {
            IDevice logDevice = new NullDevice();
            if (String.IsNullOrWhiteSpace(logPath))
                return logDevice;
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logDevice = new ManagedLocalStorageDevice(logPath + ".obj.log", singleSegment: false, deleteOnClose: deleteOnClose);
            }
#else
            {
                logDevice = new LocalStorageDevice(logPath + ".obj.log", singleSegment: false, deleteOnClose: deleteOnClose);
            }
#endif
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
        /// <param name="objectLogDevice">Log device for objects</param>
        /// <param name="logParameters">Log parameters</param>
        /// <param name="checkpointDir">Directory for recovery checkpoints</param>
        /// <param name="checkpointType">Type of checkpoint (snapshot or foldover)</param>
        /// <returns>Instance of FASTER</returns>
        public static TIFaster
            Create<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>(
            long indexSizeBuckets, IDevice logDevice, IDevice objectLogDevice = null,
            LogParameters logParameters = null,
            string checkpointDir = null, CheckpointType checkpointType = CheckpointType.FoldOver
            )
        {
            if (logParameters == null)
                logParameters = new LogParameters();

            return
                HashTableManager.GetFasterHashTable
                                <TKey, TValue, TInput, TOutput,
                                TContext, TFunctions, TIFaster>
                                (indexSizeBuckets, logDevice, objectLogDevice, checkpointDir,
                                logParameters.MemorySizeBits, logParameters.MutableFraction, logParameters.PageSizeBits, 
                                logParameters.SegmentSizeBits, checkpointType == CheckpointType.FoldOver);
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
        /// <param name="objectLogDevice">Log device for storing objects (only needed for non-blittable key/value types)</param>
        /// <param name="functions">Instance of functions</param>
        /// <param name="logParameters">Log parameters</param>
        /// <param name="checkpointDir">Directory for recovery checkpoints</param>
        /// <param name="checkpointType">Type of checkpoint (snapshot or foldover)</param>
        /// <param name="treatValueAsAtomic">Treat value as atomic, no auto-locking by FASTER</param>
        /// <returns>Instance of FASTER</returns>
        public static IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>
            Create<TKey, TValue, TInput, TOutput, TContext, TFunctions>
            (long indexSizeBuckets, IDevice logDevice, IDevice objectLogDevice,
            TFunctions functions,
            LogParameters logParameters = null,
            string checkpointDir = null, CheckpointType checkpointType = CheckpointType.FoldOver,
            bool treatValueAsAtomic = false)
            where TFunctions : IUserFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            return HashTableManager.GetMixedManagedFasterHashTable
                <TKey, TValue, TInput, TOutput, TContext, TFunctions>
                (indexSizeBuckets, logDevice, objectLogDevice, checkpointDir,
                functions, treatValueAsAtomic, logParameters, checkpointType);
        }
    }
}

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
    /// Configuration settings for hybrid log
    /// </summary>
    public class LogSettings
    {
        /// <summary>
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice = new NullDevice();

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice = new NullDevice();

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
    /// Checkpoint-related settings
    /// </summary>
    public class CheckpointSettings
    {
        /// <summary>
        /// Directory where checkpoints are stored
        /// </summary>
        public string CheckpointDir = "";

        /// <summary>
        /// Type of checkpoint
        /// </summary>
        public CheckpointType CheckPointType = CheckpointType.Snapshot;
    }

    /// <summary>
    /// Checkpoint type
    /// </summary>
    public enum CheckpointType
    {
        /// <summary>
        /// Take separate snapshot of in-memory portion of log (default)
        /// </summary>
        Snapshot,

        /// <summary>
        /// Flush current log (move read-only to tail)
        /// (enables incremental checkpointing, but log grows faster)
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
        /// <param name="indexSizeBuckets">Number of buckets</param>
        /// <param name="logSettings">Log Settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <returns>Instance of FASTER</returns>
        public static TIFaster
            Create<TKey, TValue, TInput, TOutput, TContext, TFunctions, TIFaster>(
            long indexSizeBuckets,
            LogSettings logSettings = null,
            CheckpointSettings checkpointSettings = null
            )
        {
            if (logSettings == null)
                logSettings = new LogSettings();
            if (checkpointSettings == null)
                checkpointSettings = new CheckpointSettings();

            return
                HashTableManager.GetFasterHashTable
                                <TKey, TValue, TInput, TOutput,
                                TContext, TFunctions, TIFaster>
                                (indexSizeBuckets, logSettings.LogDevice, logSettings.ObjectLogDevice, 
                                checkpointSettings.CheckpointDir, logSettings.MemorySizeBits, 
                                logSettings.MutableFraction, logSettings.PageSizeBits, 
                                logSettings.SegmentSizeBits, checkpointSettings.CheckPointType == CheckpointType.FoldOver);
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
        /// <param name="indexSizeBuckets">Number of buckets</param>
        /// <param name="functions">Instance of functions</param>
        /// <param name="logSettings">Log parameters</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="treatValueAsAtomic">Treat value as atomic. If true, there is no auto-locking by FASTER</param>
        /// <returns>Instance of FASTER</returns>
        public static IManagedFasterKV<TKey, TValue, TInput, TOutput, TContext>
            Create<TKey, TValue, TInput, TOutput, TContext, TFunctions>
            (long indexSizeBuckets, TFunctions functions,
            LogSettings logSettings = null,
            CheckpointSettings checkpointSettings = null,
            bool treatValueAsAtomic = true)
            where TFunctions : IUserFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            if (logSettings == null)
                logSettings = new LogSettings();
            if (checkpointSettings == null)
                checkpointSettings = new CheckpointSettings();

            return HashTableManager.GetMixedManagedFasterHashTable
                <TKey, TValue, TInput, TOutput, TContext, TFunctions>
                (indexSizeBuckets, functions, logSettings, checkpointSettings, treatValueAsAtomic);
        }
    }
}

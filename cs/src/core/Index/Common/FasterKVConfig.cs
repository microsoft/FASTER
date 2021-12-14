// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Configuration settings for hybrid log. Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
    /// </summary>
    public sealed class FasterKVConfig<Key, Value> : IDisposable
    {
        bool disposeDevices = false;
        bool deleteDirOnDispose = false;
        string baseDir;

        /// <summary>
        /// Create default configuration settings for hybrid log. Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB"). Default index size is 64MB.
        /// </summary>
        public FasterKVConfig() { }

        /// <summary>
        /// Create default configuration backed by local storage at given base directory. Use Utility.ParseSize to specify sizes 
        /// in familiar string notation (e.g., "4k" and "4 MB"). Default index size is 64MB.
        /// </summary>
        /// <param name="baseDir">Base directory (without trailing path separator)</param>
        /// <param name="deleteDirOnDispose">Whether to delete base directory on dispose</param>
        public FasterKVConfig(string baseDir, bool deleteDirOnDispose = false)
        {
            disposeDevices = true;
            this.deleteDirOnDispose = deleteDirOnDispose;
            this.baseDir = baseDir;

            LogDevice = baseDir == null ? new NullDevice() : Devices.CreateLogDevice(baseDir + "/hlog.log");
            if ((!Utility.IsBlittable<Key>() && KeyLength == null) ||
                (!Utility.IsBlittable<Value>() && ValueLength == null))
            {
                ObjectLogDevice = baseDir == null ? new NullDevice() : Devices.CreateLogDevice(baseDir + "/hlog.obj.log");
            }
            
            CheckpointDir = baseDir == null ? null : baseDir + "/checkpoints";
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (disposeDevices)
            {
                LogDevice?.Dispose();
                ObjectLogDevice?.Dispose();
                if (deleteDirOnDispose && baseDir != null)
                {
                    try { new DirectoryInfo(baseDir).Delete(true); } catch { }
                }
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var retStr = $"index: {PrettySize(IndexSize)}; log memory: {PrettySize(MemorySize)}; log page: {PrettySize(PageSize)}; log segment: {PrettySize(SegmentSize)}";
            retStr += $"; log device: {(LogDevice == null ? "null" : LogDevice.GetType().Name)}";
            retStr += $"; obj log device: {(ObjectLogDevice == null ? "null" : ObjectLogDevice.GetType().Name)}";
            retStr += $"; mutable fraction: {MutableFraction}; supports locking: {(SupportsLocking ? "yes" : "no")}";
            retStr += $"; read cache (rc): {(ReadCacheEnabled ? "yes" : "no")}";
            if (ReadCacheEnabled)
                retStr += $"; rc memory: {PrettySize(ReadCacheMemorySize)}; rc page: {PrettySize(ReadCachePageSize)}";
            return retStr;
        }

        /// <summary>
        /// Size of main hash index, in bytes. Rounds down to power of 2.
        /// </summary>
        public long IndexSize = 1L << 26;

        /// <summary>
        /// Whether FASTER takes read and write locks on records
        /// </summary>
        public bool SupportsLocking = true;

        /// <summary>
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice;

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice;

        /// <summary>
        /// Size of a page, in bytes
        /// </summary>
        public long PageSize = 1 << 25;

        /// <summary>
        /// Size of a segment (group of pages), in bytes. Rounds down to power of 2.
        /// </summary>
        public long SegmentSize = 1L << 30;

        /// <summary>
        /// Total size of in-memory part of log, in bytes. Rounds down to power of 2.
        /// </summary>
        public long MemorySize = 1L << 34;

        /// <summary>
        /// Fraction of log marked as mutable (in-place updates). Rounds down to power of 2.
        /// </summary>
        public double MutableFraction = 0.9;

        /// <summary>
        /// Copy reads to tail of log
        /// </summary>
        public CopyReadsToTail CopyReadsToTail = CopyReadsToTail.None;

        /// <summary>
        /// Whether to preallocate the entire log (pages) in memory
        /// </summary>
        public bool PreallocateLog = false;

        /// <summary>
        /// Key serializer
        /// </summary>
        public Func<IObjectSerializer<Key>> KeySerializer;

        /// <summary>
        /// Value serializer
        /// </summary>
        public Func<IObjectSerializer<Value>> ValueSerializer;

        /// <summary>
        /// Equality comparer for key
        /// </summary>
        public IFasterEqualityComparer<Key> EqualityComparer;

        /// <summary>
        /// Info for variable-length keys
        /// </summary>
        public IVariableLengthStruct<Key> KeyLength;

        /// <summary>
        /// Info for variable-length values
        /// </summary>
        public IVariableLengthStruct<Value> ValueLength;

        /// <summary>
        /// Whether read cache is enabled
        /// </summary>
        public bool ReadCacheEnabled = false;

        /// <summary>
        /// Size of a read cache page, in bytes. Rounds down to power of 2.
        /// </summary>
        public long ReadCachePageSize = 1 << 25;

        /// <summary>
        /// Total size of read cache, in bytes. Rounds down to power of 2.
        /// </summary>
        public long ReadCacheMemorySize = 1L << 34;

        /// <summary>
        /// Fraction of log head (in memory) used for second chance 
        /// copy to tail. This is (1 - MutableFraction) for the 
        /// underlying log.
        /// </summary>
        public double ReadCacheSecondChanceFraction = 0.1;

        /// <summary>
        /// Checkpoint manager
        /// </summary>
        public ICheckpointManager CheckpointManager = null;

        /// <summary>
        /// Use specified directory for storing and retrieving checkpoints
        /// using local storage device.
        /// </summary>
        public string CheckpointDir = null;

        /// <summary>
        /// Whether FASTER should remove outdated checkpoints automatically
        /// </summary>
        public bool RemoveOutdatedCheckpoints = true;

        internal long GetIndexSizeCacheLines()
        {
            long adjustedSize = PreviousPowerOf2(IndexSize);
            if (adjustedSize < 64)
                throw new FasterException($"{nameof(IndexSize)} should be at least of size one cache line (64 bytes)");
            if (IndexSize != adjustedSize)
                Trace.TraceInformation($"Warning: using lower value {adjustedSize} instead of specified {IndexSize} for {nameof(IndexSize)}");
            return adjustedSize;
        }

        internal LogSettings GetLogSettings()
        {
            return new LogSettings
            {
                CopyReadsToTail = CopyReadsToTail,
                LogDevice = LogDevice,
                ObjectLogDevice = ObjectLogDevice,
                MemorySizeBits = NumBitsPreviousPowerOf2(MemorySize),
                PageSizeBits = NumBitsPreviousPowerOf2(PageSize),
                SegmentSizeBits = NumBitsPreviousPowerOf2(SegmentSize),
                MutableFraction = MutableFraction,
                PreallocateLog = PreallocateLog,
                ReadCacheSettings = GetReadCacheSettings()
            };
        }

        private ReadCacheSettings GetReadCacheSettings()
        {
            return ReadCacheEnabled ?
                new ReadCacheSettings
                {
                    MemorySizeBits = NumBitsPreviousPowerOf2(ReadCacheMemorySize),
                    PageSizeBits = NumBitsPreviousPowerOf2(ReadCachePageSize),
                    SecondChanceFraction = ReadCacheSecondChanceFraction
                }
                : null;
        }

        internal SerializerSettings<Key, Value> GetSerializerSettings()
        {
            if (KeySerializer == null && ValueSerializer == null)
                return null;

            return new SerializerSettings<Key, Value>
            {
                keySerializer = KeySerializer,
                valueSerializer = ValueSerializer
            };
        }

        internal CheckpointSettings GetCheckpointSettings()
        {
            return new CheckpointSettings
            {
                CheckpointDir = CheckpointDir,
                CheckpointManager = CheckpointManager,
                RemoveOutdated = RemoveOutdatedCheckpoints
            };
        }

        internal VariableLengthStructSettings<Key, Value> GetVariableLengthStructSettings()
        {
            if (KeyLength == null && ValueLength == null)
                return null;

            return new VariableLengthStructSettings<Key, Value>
            {
                keyLength = KeyLength,
                valueLength = ValueLength
            };
        }

        private static int NumBitsPreviousPowerOf2(long v)
        {
            long adjustedSize = PreviousPowerOf2(v);
            if (v != adjustedSize)
                Trace.TraceInformation($"Warning: using lower value {adjustedSize} instead of specified value {v}");
            return (int)Math.Log(adjustedSize, 2);
        }

        private static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }

        /// <summary>
        /// Pretty print value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private static string PrettySize(long value)
        {
            char[] suffix = new char[] { 'K', 'M', 'G', 'T', 'P' };
            double v = value;
            int exp = 0;
            while (v - Math.Floor(v) > 0)
            {
                if (exp >= 18)
                    break;
                exp += 3;
                v *= 1024;
                v = Math.Round(v, 12);
            }

            while (Math.Floor(v).ToString().Length > 3)
            {
                if (exp <= -18)
                    break;
                exp -= 3;
                v /= 1024;
                v = Math.Round(v, 12);
            }
            if (exp > 0)
                return v.ToString() + suffix[exp / 3 - 1] + "B";
            else if (exp < 0)
                return v.ToString() + suffix[-exp / 3 - 1] + "B";
            return v.ToString() + "B";
        }
    }
}
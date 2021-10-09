// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Options when creating FASTER server
    /// </summary>
    public class ServerOptions
    {
        /// <summary>
        /// Port to run server on
        /// </summary>
        public int Port = 3278;

        /// <summary>
        /// IP address to bind server to
        /// </summary>
        public string Address = "127.0.0.1";

        /// <summary>
        /// Total log memory used in bytes (rounds down to power of 2)
        /// </summary>
        public string MemorySize = "16g";

        /// <summary>
        /// Size of each page in bytes (rounds down to power of 2)
        /// </summary>
        public string PageSize = "32m";

        /// <summary>
        /// Size of each log segment in bytes on disk (rounds down to power of 2)
        /// </summary>
        public string SegmentSize = "1g";

        /// <summary>
        /// Size of hash index in bytes (rounds down to power of 2)
        /// </summary>
        public string IndexSize = "8g";

        /// <summary>
        /// Storage directory for data (hybrid log). Runs memory-only if unspecified.
        /// </summary>
        public string LogDir = null;

        /// <summary>
        /// Storage directory for checkpoints. Uses 'checkpoints' folder under logdir if unspecified.
        /// </summary>
        public string CheckpointDir = null;

        /// <summary>
        /// Recover from latest checkpoint.
        /// </summary>
        public bool Recover = false;

        /// <summary>
        /// Enable pub/sub feature on server.
        /// </summary>
        public bool EnablePubSub = true;

        /// <summary>
        /// Constructor
        /// </summary>
        public ServerOptions()
        {
        }

        internal int MemorySizeBits()
        {
            long size = ParseSize(MemorySize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                Trace.WriteLine($"Warning: using lower log memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        internal int PageSizeBits()
        {
            long size = ParseSize(PageSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                Trace.WriteLine($"Warning: using lower page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        internal int SegmentSizeBits()
        {
            long size = ParseSize(SegmentSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                Trace.WriteLine($"Warning: using lower disk segment size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        internal int IndexSizeCachelines()
        {
            long size = ParseSize(IndexSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (adjustedSize < 64 || adjustedSize > (1L << 37)) throw new Exception("Invalid index size");
            if (size != adjustedSize)
                Trace.WriteLine($"Warning: using lower hash index size than specified (power of 2)");
            return (int)(adjustedSize / 64);
        }

        internal void GetSettings(out LogSettings logSettings, out CheckpointSettings checkpointSettings, out int indexSize)
        {
            logSettings = new LogSettings { PreallocateLog = false };

            logSettings.PageSizeBits = PageSizeBits();
            Trace.WriteLine($"[Store] Using page size of {PrettySize((long)Math.Pow(2, logSettings.PageSizeBits))}");

            logSettings.MemorySizeBits = MemorySizeBits();
            Trace.WriteLine($"[Store] Using log memory size of {PrettySize((long)Math.Pow(2, logSettings.MemorySizeBits))}");

            Trace.WriteLine($"[Store] There are {PrettySize(1 << (logSettings.MemorySizeBits - logSettings.PageSizeBits))} log pages in memory");

            logSettings.SegmentSizeBits = SegmentSizeBits();
            Trace.WriteLine($"[Store] Using disk segment size of {PrettySize((long)Math.Pow(2, logSettings.SegmentSizeBits))}");

            indexSize = IndexSizeCachelines();
            Trace.WriteLine($"[Store] Using hash index size of {PrettySize(indexSize * 64L)} ({PrettySize(indexSize)} cache lines)");

            if (LogDir == null)
                LogDir = Directory.GetCurrentDirectory();

            var device = LogDir == "" ? new NullDevice() : Devices.CreateLogDevice(LogDir + "/Store/hlog");
            logSettings.LogDevice = device;

            checkpointSettings = new CheckpointSettings
            {
                CheckPointType = CheckpointType.Snapshot,
                CheckpointDir = CheckpointDir ?? (LogDir + "/Store/checkpoints"),
                RemoveOutdated = true,
            };
        }

        internal void GetObjectStoreSettings(out LogSettings objLogSettings, out CheckpointSettings objCheckpointSettings, out int objIndexSize)
        {
            objLogSettings = new LogSettings { PreallocateLog = false };

            objLogSettings.PageSizeBits = PageSizeBits();
            Trace.WriteLine($"[Object Store] Using page size of {PrettySize((long)Math.Pow(2, objLogSettings.PageSizeBits))}");

            objLogSettings.MemorySizeBits = MemorySizeBits();
            Trace.WriteLine($"[Object Store] Using log memory size of {PrettySize((long)Math.Pow(2, objLogSettings.MemorySizeBits))}");

            Trace.WriteLine($"[Object Store] There are {PrettySize(1 << (objLogSettings.MemorySizeBits - objLogSettings.PageSizeBits))} log pages in memory");

            objLogSettings.SegmentSizeBits = SegmentSizeBits();
            Trace.WriteLine($"[Object Store] Using disk segment size of {PrettySize((long)Math.Pow(2, objLogSettings.SegmentSizeBits))}");

            objIndexSize = IndexSizeCachelines() / 64;
            Trace.WriteLine($"[Object Store] Using hash index size of {PrettySize(objIndexSize * 64L)} ({PrettySize(objIndexSize)} cache lines)");

            if (LogDir == null)
                LogDir = Directory.GetCurrentDirectory();

            var device = LogDir == "" ? new NullDevice() : Devices.CreateLogDevice(LogDir + "/ObjectStore/hlog");
            objLogSettings.LogDevice = device;
            var objdevice = LogDir == "" ? new NullDevice() : Devices.CreateLogDevice(LogDir + "/ObjectStore/hlog.obj");
            objLogSettings.ObjectLogDevice = objdevice;

            objCheckpointSettings = new CheckpointSettings
            {
                CheckPointType = CheckpointType.Snapshot,
                CheckpointDir = CheckpointDir ?? (LogDir + "/ObjectStore/checkpoints"),
                RemoveOutdated = true,
            };
        }

        private static long ParseSize(string value)
        {
            char[] suffix = new char[] { 'k', 'm', 'g', 't', 'p' };
            long result = 0;
            foreach (char c in value)
            {
                if (char.IsDigit(c))
                {
                    result = result * 10 + (byte)c - '0';
                }
                else
                {
                    for (int i = 0; i < suffix.Length; i++)
                    {
                        if (char.ToLower(c) == suffix[i])
                        {
                            result *= (long)Math.Pow(1024, i + 1);
                            return result;
                        }
                    }
                }
            }
            return result;
        }

        private static string PrettySize(long value)
        {
            char[] suffix = new char[] { 'k', 'm', 'g', 't', 'p' };
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
                return v.ToString() + suffix[exp / 3 - 1];
            else if (exp < 0)
                return v.ToString() + suffix[-exp / 3 - 1];
            return v.ToString();
        }

        private long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }
    }
}
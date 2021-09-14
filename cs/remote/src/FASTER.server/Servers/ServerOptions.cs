// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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

        internal int MemorySizeBits()
        {
            long size = ParseSize(MemorySize);
            int bits = (int)Math.Floor(Math.Log(size, 2));
            if (size != Math.Pow(2, bits))
                Trace.WriteLine($"Warning: using lower log memory size than specified (power of 2)");
            return bits;
        }

        internal int PageSizeBits()
        {
            long size = ParseSize(PageSize);
            int bits = (int)Math.Ceiling(Math.Log(size, 2));
            if (size != Math.Pow(2, bits))
                Trace.WriteLine($"Warning: using lower page size than specified (power of 2)");
            return bits;
        }

        internal int SegmentSizeBits()
        {
            long size = ParseSize(SegmentSize);
            int bits = (int)Math.Ceiling(Math.Log(size, 2));
            if (size != Math.Pow(2, bits))
                Trace.WriteLine($"Warning: using lower disk segment size than specified (power of 2)");
            return bits;
        }

        internal int IndexSizeCachelines()
        {
            long size = ParseSize(IndexSize);
            int bits = (int)Math.Ceiling(Math.Log(size, 2));
            long adjustedSize = 1L << bits;
            if (adjustedSize < 64) throw new Exception("Invalid index size");
            if (size != adjustedSize)
                Trace.WriteLine($"Warning: using lower hash index size than specified (power of 2)");
            return 1 << (bits - 6);
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

            var device = LogDir == null ? new NullDevice() : Devices.CreateLogDevice(LogDir + "Store/hlog", preallocateFile: false);
            logSettings.LogDevice = device;

            checkpointSettings = new CheckpointSettings {
                CheckPointType = CheckpointType.Snapshot, 
                CheckpointDir = CheckpointDir ?? (LogDir + "Store/checkpoints"),
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
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using System;
using FASTER.core;

namespace ServerOptions
{
    class Options
    {
        [Option("port", Required = false, Default = 3278, HelpText = "Port to run server on")]
        public int Port { get; set; }

        [Option("bind", Required = false, Default = "127.0.0.1", HelpText = "IP address to bind server to")]
        public string Address { get; set; }

        [Option('m', "memory", Required = false, Default = "16g", HelpText = "Total log memory used in bytes (rounds down to power of 2)")]
        public string MemorySize { get; set; }

        [Option('p', "page", Required = false, Default = "32m", HelpText = "Size of each page in bytes (rounds down to power of 2)")]
        public string PageSize { get; set; }

        [Option('s', "segment", Required = false, Default = "1g", HelpText = "Size of each log segment in bytes on disk (rounds down to power of 2)")]
        public string SegmentSize { get; set; }

        [Option('i', "index", Required = false, Default = "8g", HelpText = "Size of hash index in bytes (rounds down to power of 2)")]
        public string IndexSize { get; set; }

        [Option('l', "logdir", Required = false, Default = null, HelpText = "Storage directory for data (hybrid log). Runs memory-only if unspecified.")]
        public string LogDir { get; set; }

        [Option('c', "checkpointdir", Required = false, Default = null, HelpText = "Storage directory for checkpoints. Uses 'checkpoints' folder under logdir if unspecified.")]
        public string CheckpointDir { get; set; }

        [Option('r', "recover", Required = false, Default = false, HelpText = "Recover from latest checkpoint.")]
        public bool Recover { get; set; }

        [Option("pubsub", Required = false, Default = true, HelpText = "Enable pub/sub feature on server.")]
        public bool EnablePubSub { get; set; }

        public int MemorySizeBits()
        {
            long size = ParseSize(MemorySize);
            int bits = (int)Math.Floor(Math.Log2(size));
            Console.WriteLine($"Using log memory size of {PrettySize((long)Math.Pow(2, bits))}");
            if (size != Math.Pow(2, bits))
                Console.WriteLine($"Warning: using lower log memory size than specified (power of 2)");
            return bits;
        }

        public int PageSizeBits()
        {
            long size = ParseSize(PageSize);
            int bits = (int)Math.Ceiling(Math.Log2(size));
            Console.WriteLine($"Using page size of {PrettySize((long)Math.Pow(2, bits))}");
            if (size != Math.Pow(2, bits))
                Console.WriteLine($"Warning: using lower page size than specified (power of 2)");
            return bits;
        }

        public int SegmentSizeBits()
        {
            long size = ParseSize(SegmentSize);
            int bits = (int)Math.Ceiling(Math.Log2(size));
            Console.WriteLine($"Using disk segment size of {PrettySize((long)Math.Pow(2, bits))}");
            if (size != Math.Pow(2, bits))
                Console.WriteLine($"Warning: using lower disk segment size than specified (power of 2)");
            return bits;
        }

        public int IndexSizeCachelines()
        {
            long size = ParseSize(IndexSize);
            int bits = (int)Math.Ceiling(Math.Log2(size));
            long adjustedSize = 1L << bits;
            if (adjustedSize < 64) throw new Exception("Invalid index size");
            Console.WriteLine($"Using hash index size of {PrettySize(adjustedSize)} ({PrettySize(adjustedSize/64)} cache lines)");
            if (size != adjustedSize)
                Console.WriteLine($"Warning: using lower hash index size than specified (power of 2)");
            return 1 << (bits - 6);
        }

        public void GetSettings(out LogSettings logSettings, out CheckpointSettings checkpointSettings, out int indexSize)
        {
            logSettings = new LogSettings { PreallocateLog = false };

            logSettings.PageSizeBits = PageSizeBits();
            logSettings.MemorySizeBits = MemorySizeBits();
            Console.WriteLine($"There are {PrettySize(1 << (logSettings.MemorySizeBits - logSettings.PageSizeBits))} log pages in memory");
            logSettings.SegmentSizeBits = SegmentSizeBits();
            indexSize = IndexSizeCachelines();

            var device = LogDir == "" ? new NullDevice() : Devices.CreateLogDevice(LogDir + "/hlog", preallocateFile: false);
            logSettings.LogDevice = device;

            if (CheckpointDir == null && LogDir == null)
                checkpointSettings = null;
            else
                checkpointSettings = new CheckpointSettings { 
                    CheckPointType = CheckpointType.FoldOver, 
                    CheckpointDir = CheckpointDir ?? (LogDir + "/checkpoints")
                };
        }

        static long ParseSize(string value)
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

        public static string PrettySize(long value)
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
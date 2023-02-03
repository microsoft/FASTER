// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Threading;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable CS0162 // Unreachable code detected

namespace MemOnlyCache
{
    class Program
    {
        /// <summary>
        /// Total database size
        /// </summary>
        static int DbSize = 10_000_000;
        const string DbSizeArg = "--dbsize";

        /// <summary>
        /// Max key size; we choose actual size randomly
        /// </summary>
        static int MaxKeySize = 100;
        const string MaxKeySizeArg = "--keysize";

        /// <summary>
        /// Max value size; we choose actual size randomly
        /// </summary>
        static int MaxValueSize = 1000;
        const string MaxValueSizeArg = "--valuesize";

        /// <summary>
        /// Number of threads accessing FASTER instances
        /// </summary>
        static int NumThreads = 1;
        const string NumThreadsArg = "-t";

        /// <summary>
        /// Percentage of writes in incoming workload requests (remaining are reads)
        /// </summary>
        static int WritePercent = 0;
        const string WritePercentArg = "-w";

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        static bool UseUniform = false;
        const string UseUniformArg = "-u";

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        static bool UseReadCTT = true;
        const string NoReadCTTArg = "--noreadctt";

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        static bool UseReadCache = false;
        const string UseReadCacheArg = "--readcache";

        /// <summary>
        /// Skew factor (theta) of Zipf distribution
        /// </summary>
        const double Theta = 0.99;

        /// <summary>
        /// Whether to upsert the key on a cache miss
        /// </summary>
        const bool UpsertOnCacheMiss = true;

        static FasterKV<CacheKey, CacheValue> h;
        static CacheSizeTracker sizeTracker;
        static long totalReads = 0;
        static long targetSize = 1L << 30; // target size for FASTER, we vary this during the run

        // Cache hit stats
        static long statusNotFound = 0;
        static long statusFound = 0;

        const string HelpArg1 = "-?";
        const string HelpArg2 = "/?";
        const string HelpArg3 = "--help";
        static bool IsHelpArg(string arg) => arg == HelpArg1 || arg == HelpArg2 || arg == HelpArg3;

        private static bool Usage()
        {
            Console.WriteLine("Reads 'linked lists' of records for each key by backing up the previous-address chain, including showing record versions");
            Console.WriteLine("Usage:");
            Console.WriteLine($"  {DbSizeArg}: Total database size. Default = {DbSize}");
            Console.WriteLine($"  {MaxKeySizeArg}: Max key size; we choose actual size randomly. Default = {MaxKeySize}");
            Console.WriteLine($"  {MaxValueSizeArg}: Max value size; we choose actual size randomly. Default = {MaxValueSize}");
            Console.WriteLine($"  {NumThreadsArg}: Number of threads accessing FASTER instances. Default = {NumThreads}");
            Console.WriteLine($"  {WritePercentArg}: Percentage of writes in incoming workload requests (remaining are reads). Default = {WritePercent}");
            Console.WriteLine($"  {UseUniformArg}: Uniform random distribution (true) or Zipf distribution (false) of requests. Default = {UseUniform}");
            Console.WriteLine($"  {NoReadCTTArg}: Copy Reads from Immutable region to tail of log. Default = {!UseReadCTT}");
            Console.WriteLine($"  {UseReadCacheArg}: Use the ReadCache. Default = {UseReadCache}");
            Console.WriteLine($"  {HelpArg1}, {HelpArg2}, {HelpArg3}: This screen.");
            return false;
        }

        static bool GetArgs(string[] args)
        {
            for (var ii = 0; ii < args.Length; ++ii)
            {
                var arg = args[ii].ToLower();
                var val = "n/a";
                try
                {
                    if (IsHelpArg(arg))
                        return Usage();

                    // Flag args (no value)
                    if (arg == UseUniformArg)
                    {
                        UseUniform = true;
                        continue;
                    }

                    if (arg == NoReadCTTArg)
                    {
                        UseReadCTT = false;
                        continue;
                    }

                    if (arg == UseReadCacheArg)
                    {
                        UseReadCache = true;
                        continue;
                    }

                    // Args taking a value
                    if (ii >= args.Length - 1)
                    {
                        Console.WriteLine($"Error: End of arg list encountered while processing arg {arg}; expected argument");
                        return false;
                    }
                    val = args[++ii];
                    if (arg == DbSizeArg)
                    {
                        DbSize = int.Parse(val);
                        continue;
                    }
                    if (arg == MaxKeySizeArg)
                    {
                        MaxKeySize = int.Parse(val);
                        continue;
                    }
                    if (arg == NumThreadsArg)
                    {
                        NumThreads = int.Parse(val);
                        continue;
                    }
                    if (arg == WritePercentArg)
                    {
                        WritePercent = int.Parse(val);
                        continue;
                    }

                    Console.WriteLine($"Unknown option: {arg}");
                    return Usage();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: Arg {arg}, value {val} encountered exception: {ex.Message}");
                    return false;
                }
            }
            return true;
        }

        static void Main(string[] args)
        {
            // This sample shows the use of FASTER as a concurrent pure in-memory cache
            if (!GetArgs(args))
                return;

            var log = new NullDevice(); // no storage involved

            // Define settings for log
            var logSettings = new LogSettings
            {
                LogDevice = log, ObjectLogDevice = log,
                MutableFraction = 0.9, // 10% of memory log is "read-only region"
                ReadFlags = UseReadCTT ? ReadFlags.CopyReadsToTail : ReadFlags.None, // reads in read-only region are copied to tail
                PageSizeBits = 14, // Each page is sized at 2^14 bytes
                MemorySizeBits = 25, // (2^25 / 24) = ~1.39M key-value pairs (log uses 24 bytes per KV pair)
            };

            if (UseReadCache)
                logSettings.ReadCacheSettings = new() { MemorySizeBits = logSettings.MemorySizeBits, PageSizeBits = logSettings.PageSizeBits };

            // Number of records in memory, assuming class keys and values and x64 platform
            // (8-byte key + 8-byte value + 8-byte header = 24 bytes per record)
            int numRecords = (int)(Math.Pow(2, logSettings.MemorySizeBits) / 24);

            // Set hash table size targeting 1 record per bucket
            var numBucketBits = (int)Math.Ceiling(Math.Log2(numRecords)); 

            h = new FasterKV<CacheKey, CacheValue>(1L << numBucketBits, logSettings, comparer: new CacheKey());
            sizeTracker = new CacheSizeTracker(h, targetSize);

            // Initially populate store
            PopulateStore(numRecords);

            // Run continuous read/upsert workload
            ContinuousRandomWorkload();
            
            h.Dispose();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        private static void PopulateStore(int count)
        {
            using var s = h.For(new CacheFunctions(sizeTracker)).NewSession<CacheFunctions>();

            Random r = new(0);
            Console.WriteLine("Writing random keys to fill cache");

            for (int i = 0; i < count; i++)
            {
                int k = r.Next(DbSize);
                var key = new CacheKey(k, 1 + r.Next(MaxKeySize - 1));
                var value = new CacheValue(1 + r.Next(MaxValueSize - 1), (byte)key.key);
                s.Upsert(ref key, ref value);
            }
        }

        private static void ContinuousRandomWorkload()
        {
            var threads = new Thread[NumThreads];
            for (int i = 0; i < NumThreads; i++)
            {
                var x = i;
                threads[i] = new Thread(() => RandomWorkload(x));
            }
            for (int i = 0; i < NumThreads; i++)
                threads[i].Start();

            Stopwatch sw = new();
            sw.Start();
            var _lastReads = totalReads;
            var _lastTime = sw.ElapsedMilliseconds;
            int count = 0;
            while (true)
            {
                Thread.Sleep(1500);
                var tmp = totalReads;
                var tmp2 = sw.ElapsedMilliseconds;

                Console.WriteLine("Throughput: {0,8:0.00}K ops/sec; Hit rate: {1:N2}; Memory footprint: {2,11:N2}KB", (_lastReads - tmp) / (double)(_lastTime - tmp2), statusFound / (double)(statusFound + statusNotFound), sizeTracker.TotalSizeBytes / 1024.0);

                Interlocked.Exchange(ref statusFound, 0);
                Interlocked.Exchange(ref statusNotFound, 0);

                // Optional: perform GC collection to verify accurate memory reporting in task manager
                // GC.Collect();
                // GC.WaitForFullGCComplete();

                // As demo, reduce target size by 100MB each time we report memory footprint
                count++;
                if (count % 4 == 0)
                {
                    if (targetSize > 1L << 28)
                    {
                        targetSize -= 1L << 27;
                        sizeTracker.SetTargetSizeBytes(targetSize);
                    }
                    else
                    {
                        targetSize = 1L << 30;
                        sizeTracker.SetTargetSizeBytes(targetSize);
                    }
                    Console.WriteLine("**** Setting target memory: {0,11:N2}KB", targetSize / 1024.0);
                }

                _lastReads = tmp;
                _lastTime = tmp2;
            }
        }

        private static void RandomWorkload(int threadid)
        {
            Console.WriteLine("Issuing {0} random read workload of {1} reads from thread {2}", UseUniform ? "uniform" : "zipf", DbSize, threadid);

            using var session = h.For(new CacheFunctions(sizeTracker)).NewSession<CacheFunctions>();

            var rnd = new Random(threadid);
            var zipf = new ZipfGenerator(rnd, DbSize, Theta);

            CacheValue output = default;
            int localStatusFound = 0, localStatusNotFound = 0;

            int i = 0;
            while (true)
            {
                if ((i % 256 == 0) && (i > 0))
                {
                    Interlocked.Add(ref statusFound, localStatusFound);
                    Interlocked.Add(ref statusNotFound, localStatusNotFound);
                    Interlocked.Add(ref totalReads, 256);
                    localStatusFound = localStatusNotFound = 0;
                }
                int op = WritePercent == 0 ? 0 : rnd.Next(100);
                long k = UseUniform ? rnd.Next(DbSize) : zipf.Next();

                var key = new CacheKey(k, 1 + rnd.Next(MaxKeySize - 1));

                if (op < WritePercent)
                {
                    var value = new CacheValue(1 + rnd.Next(MaxValueSize - 1), (byte)key.key);
                    session.Upsert(ref key, ref value);
                }
                else
                {
                    var status = session.Read(ref key, ref output);

                    if (!status.Found)
                    {
                        if (status.IsFaulted)
                            throw new Exception("Error!");
                        localStatusNotFound++;
                        if (UpsertOnCacheMiss)
                        {
                            var value = new CacheValue(1 + rnd.Next(MaxValueSize - 1), (byte)key.key);
                            session.Upsert(ref key, ref value);
                        }
                    }
                    else
                    {
                        localStatusFound++;
                        if (output.value[0] != (byte)key.key)
                            throw new Exception("Read error!");
                    }
                }
                i++;
            }
        }
    }
}

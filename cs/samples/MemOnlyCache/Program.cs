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
        const int DbSize = 10_000_000;

        /// <summary>
        /// Max key size; we choose actual size randomly
        /// </summary>
        const int MaxKeySize = 100;

        /// <summary>
        /// Max value size; we choose actual size randomly
        /// </summary>
        const int MaxValueSize = 1000;

        /// <summary>
        /// Number of threads accessing FASTER instances
        /// </summary>
        const int kNumThreads = 1;

        /// <summary>
        /// Percentage of writes in incoming workload requests (remaining are reads)
        /// </summary>
        const int WritePercent = 0;

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        const bool UseUniform = false;

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

        static void Main()
        {
            // This sample shows the use of FASTER as a concurrent pure in-memory cache

            var log = new NullDevice(); // no storage involved

            // Define settings for log
            var logSettings = new LogSettings
            {
                LogDevice = log, ObjectLogDevice = log,
                MutableFraction = 0.9, // 10% of memory log is "read-only region"
                CopyReadsToTail = CopyReadsToTail.FromReadOnly, // reads in read-only region are copied to tail
                PageSizeBits = 14, // Each page is sized at 2^14 bytes
                MemorySizeBits = 25, // (2^25 / 24) = ~1.39M key-value pairs (log uses 24 bytes per KV pair)
            };

            // Number of records in memory, assuming class keys and values and x64 platform
            // (8-byte key + 8-byte value + 8-byte header = 24 bytes per record)
            int numRecords = (int)(Math.Pow(2, logSettings.MemorySizeBits) / 24);

            // Targeting 1 record per bucket
            var numBucketBits = (int)Math.Ceiling(Math.Log2(numRecords)); 

            h = new FasterKV<CacheKey, CacheValue>(1L << numBucketBits, logSettings, comparer: new CacheKey());
            sizeTracker = new CacheSizeTracker(h, logSettings.MemorySizeBits, 1_000_000_000);

            PopulateStore(numRecords);
            ContinuousRandomWorkload();
            
            h.Dispose();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        private static void PopulateStore(int count)
        {
            using var s = h.For(new CacheFunctions(sizeTracker)).NewSession<CacheFunctions>();

            Random r = new Random(0);
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
            var threads = new Thread[kNumThreads];
            for (int i = 0; i < kNumThreads; i++)
            {
                var x = i;
                threads[i] = new Thread(() => RandomWorkload(x));
            }
            for (int i = 0; i < kNumThreads; i++)
                threads[i].Start();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            var _lastReads = totalReads;
            var _lastTime = sw.ElapsedMilliseconds;
            while (true)
            {
                Thread.Sleep(1000);
                var tmp = totalReads;
                var tmp2 = sw.ElapsedMilliseconds;

                Console.WriteLine("Throughput: {0:0.00}K ops/sec", (_lastReads - tmp) / (double)(_lastTime - tmp2));
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

            int statusNotFound = 0;
            int statusFound = 0;
            CacheValue output = default;

            int i = 0;
            while (true)
            {
                if ((i % 256 == 0) && (i > 0))
                {
                    Interlocked.Add(ref totalReads, 256);
                    if (i % (1024 * 1024 * 4) == 0) // report after every 8M ops
                    {
                        // Optional: perform GC collection to verify accurate memory reporting in task manager
                        // GC.Collect();
                        // GC.WaitForFullGCComplete();
                        Console.WriteLine("Hit rate: {0:N2}; Memory footprint: {1:N2}KB", statusFound / (double)(statusFound + statusNotFound), sizeTracker.TotalSizeBytes / (double)1024);

                        // As demo, reduce target size by 50MB each time we report memory footprint
                        long targetSize = sizeTracker.TotalSizeBytes - 50_000_000;
                        if (targetSize > 200_000_000)
                        {
                            Console.WriteLine("Setting target size in bytes: {0:N2}", targetSize);
                            sizeTracker.SetTargetSizeBytes(targetSize);
                        }
                    }
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

                    switch (status)
                    {
                        case Status.NOTFOUND:
                            statusNotFound++;
                            if (UpsertOnCacheMiss)
                            {
                                var value = new CacheValue(1 + rnd.Next(MaxValueSize - 1), (byte)key.key);
                                session.Upsert(ref key, ref value);
                            }
                            break;
                        case Status.OK:
                            statusFound++;
                            if (output.value[0] != (byte)key.key)
                                throw new Exception("Read error!");
                            break;
                        default:
                            throw new Exception("Error!");
                    }
                }
                i++;
            }
        }
    }
}

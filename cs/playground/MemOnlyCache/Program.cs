// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Threading;

namespace MemOnlyCache
{
    class Program
    {
        /// <summary>
        /// Total database size
        /// </summary>
        const int DbSize = 10_000_000;

        /// <summary>
        /// Number of threads accessing FASTER instances
        /// </summary>
        const int kNumThreads = 8;

        /// <summary>
        /// Percentage of writes
        /// </summary>
        const int WritePercent = 5;

        static FasterKV<CacheKey, CacheValue> h;
        static long totalReads = 0;

        static void Main()
        {
            // This sample shows the use of FASTER as a concurrent pure in-memory cache

            var log = new NullDevice();

            // Define settings for log
            var logSettings = new LogSettings
            {
                LogDevice = log,
                ObjectLogDevice = log,
                PageSizeBits = 14,
                MemorySizeBits = 25, // (2^25 / 24) = ~1.39M key-value pairs
            };

            // Number of records in memory, assuming class keys and values (8-byte key + 8-byte value + 8-byte header = 24 bytes per record)
            int numRecords = (int)(Math.Pow(2, logSettings.MemorySizeBits) / 24);

            // Targeting 1 record per bucket
            var numBucketBits = (int)Math.Ceiling(Math.Log2(numRecords)); 

            h = new FasterKV<CacheKey, CacheValue>(
                1L << numBucketBits, logSettings,
                comparer: new CacheKey(0)
                );

            PopulateStore(numRecords);
            ContinuousRandomWorkload();
            
            h.Dispose();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        private static void PopulateStore(int count)
        {
            using var s = h.For(new CacheFunctions()).NewSession<CacheFunctions>();

            Random r = new Random();
            Console.WriteLine("Writing random keys to fill cache");

            for (int i = 0; i < count; i++)
            {
                int k = r.Next(DbSize);
                var key = new CacheKey(k);
                var value = new CacheValue(k);
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
            Console.WriteLine("Issuing uniform random read workload of {0} reads from thread {1}", DbSize, threadid);

            using var session = h.For(new CacheFunctions()).NewSession<CacheFunctions>();

            var rnd = new Random(threadid);

            int statusNotFound = 0;
            int statusFound = 0;
            CacheValue output = default;

            int i = 0;
            while (true)
            {
                if (i > 0 && (i % 256 == 0))
                {
                    Interlocked.Add(ref totalReads, 256);
                    if (i % (1024 * 1024 * 20) == 0)
                        Console.WriteLine("Hit rate: {0}", statusFound / (double)(statusFound + statusNotFound));
                }
                int op = rnd.Next(100);
                long k = rnd.Next(DbSize);

                var key = new CacheKey(k);

                if (op < WritePercent)
                {
                    var value = new CacheValue(k);
                    session.Upsert(ref key, ref value);
                }
                else
                {
                    var status = session.Read(ref key, ref output);

                    switch (status)
                    {
                        case Status.NOTFOUND:
                            statusNotFound++;
                            break;
                        case Status.OK:
                            statusFound++;
                            if (output.value != key.key)
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

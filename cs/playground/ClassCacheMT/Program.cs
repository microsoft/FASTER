// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClassCacheMT
{
    class Program
    {
        // Whether we use read cache in this sample
        static readonly bool useReadCache = false;
        const int max = 1000000;

        /// <summary>
        /// Numer of FASTER instances
        /// </summary>
        const int kNumTables = 1;

        /// <summary>
        /// Number of threads accessing FASTER instances
        /// </summary>
        const int kNumThreads = 6;

        static FasterKV<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions>[] h;
        static long totalReads = 0;

        static void Main(string[] args)
        {
            // This sample shows the use of FASTER as a multi-threaded (MT) cache + key-value store for 
            // C# objects. Number of caches and number of threads can be varied.

            h = new FasterKV<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions>[kNumTables];

            for (int ht = 0; ht < kNumTables; ht++)
            {
                // Create files for storing data
                // We set deleteOnClose to true, so logs will auto-delete on completion
                var log = Devices.CreateLogDevice("hlog" + ht + ".log", deleteOnClose: true);
                var objlog = Devices.CreateLogDevice("hlog" + ht + ".obj.log", deleteOnClose: true);

                // We use context to store and report latency of async operations
                var context = default(CacheContext);

                // Define settings for log
                var logSettings = new LogSettings { LogDevice = log, ObjectLogDevice = objlog };
                if (useReadCache)
                    logSettings.ReadCacheSettings = new ReadCacheSettings();

                h[ht] = new FasterKV
                    <CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions>(
                    1L << 20, new CacheFunctions(), logSettings,
                    null, // no checkpoints in this sample
                          // Provide serializers for key and value types
                    new SerializerSettings<CacheKey, CacheValue> { keySerializer = () => new CacheKeySerializer(), valueSerializer = () => new CacheValueSerializer() },
                    null
                    );
                h[ht].StartSession();
                Console.WriteLine("Table {0}:", ht);
                Console.WriteLine("   Writing keys from 0 to {0} to FASTER", max);

                Stopwatch sw = new Stopwatch();
                sw.Start();
                for (int i = 0; i < max; i++)
                {
                    if (i % 256 == 0)
                    {
                        h[ht].Refresh();
                        if (i % (1 << 19) == 0)
                        {
                            // long workingSet = Process.GetCurrentProcess().WorkingSet64;
                            // Console.WriteLine($"{i}: {workingSet / 1048576}M");
                        }
                    }
                    var key = new CacheKey(i);
                    var value = new CacheValue(i);
                    h[ht].Upsert(ref key, ref value, context, 0);
                }
                sw.Stop();
                Console.WriteLine("   Total time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
                h[ht].Log.DisposeFromMemory();
                h[ht].StopSession();
            }

            ContinuousRandomReadWorkload();

            for (int ht = 0; ht < kNumTables; ht++)
            {
                h[ht].Dispose();
            }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        private static void ContinuousRandomReadWorkload()
        {
            var threads = new Thread[kNumThreads];
            for (int i = 0; i < kNumThreads; i++)
            {
                var x = i;
                threads[i] = new Thread(() => RandomReadWorkload(x));
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
            /*
            for (int i = 0; i < kNumThreads; i++)
                threads[i].Join();
            */
        }

        
        private static void RandomReadWorkload(int threadid)
        {
            Console.WriteLine("Issuing uniform random read workload of {0} reads from thread {1}", max, threadid);

            for (int ht = 0; ht < kNumTables; ht++)
                h[ht].StartSession();

            var rnd = new Random(threadid);

            int statusPending = 0;
            var output = new CacheOutput();
            var context = new CacheContext();
            var input = default(CacheInput);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            int i = 0;
            while (true)
            {
                if (i > 0 && (i % 256 == 0))
                {
                    for (int htcnt = 0; htcnt < kNumTables; htcnt++)
                        h[htcnt].CompletePending(false);
                    Interlocked.Add(ref totalReads, 256);
                }
                long k = rnd.Next(max);

                var ht = h[rnd.Next(kNumTables)];
                var key = new CacheKey(k);
                var status = ht.Read(ref key, ref input, ref output, context, 0);

                switch (status)
                {
                    case Status.PENDING:
                        statusPending++;
                        break;
                    case Status.OK:
                        if (output.value.value != key.key)
                            throw new Exception("Read error!");
                        break;
                    default:
                        throw new Exception("Error!");
                }
                i++;
            }
            /*
            sw.Stop();
            Console.WriteLine("Total time to read {0} elements: {1:0.000} secs ({2:0.00} reads/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
            Console.WriteLine($"Reads completed with PENDING: {statusPending}");

            for (int ht = 0; ht < kNumTables; ht++)
                h[ht].StopSession();
            */
        }
    }
}

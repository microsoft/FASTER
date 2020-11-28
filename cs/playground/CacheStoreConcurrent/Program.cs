// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace CacheStoreConcurrent
{
    class Program
    {
        // Whether we use read cache in this sample
        static readonly bool useReadCache = false;
        const int numKeys = 1000000;

        /// <summary>
        /// Numer of FASTER instances
        /// </summary>
        const int kNumTables = 1;

        /// <summary>
        /// Number of threads accessing FASTER instances
        /// </summary>
        const int kNumThreads = 6;

        static FasterKV<CacheKey, CacheValue>[] h;
        static long totalReads = 0;

        static void Main()
        {
            // This sample shows the use of FASTER as a concurrent (multi-threaded) cache + key-value store.
            // Number of caches and number of threads can be varied.

            h = new FasterKV<CacheKey, CacheValue>[kNumTables];
            var path = Path.GetTempPath() + "CacheStoreConcurrent/";

            for (int ht = 0; ht < kNumTables; ht++)
            {
                // Create files for storing data
                // We set deleteOnClose to true, so logs will auto-delete on completion
                var log = Devices.CreateLogDevice(path + "hlog" + ht + ".log", deleteOnClose: true);
                var objlog = Devices.CreateLogDevice(path + "hlog" + ht + ".obj.log", deleteOnClose: true);

                // Define settings for log
                var logSettings = new LogSettings
                {
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null
                };

                // Define serializers; otherwise FASTER will use the slower DataContract
                // Needed only for class keys/values
                var serializerSettings = new SerializerSettings<CacheKey, CacheValue>
                {
                    keySerializer = () => new CacheKeySerializer(),
                    valueSerializer = () => new CacheValueSerializer()
                };
                
                h[ht] = new FasterKV
                    <CacheKey, CacheValue>(
                    1L << 20, logSettings,
                    checkpointSettings: new CheckpointSettings { CheckpointDir = path },
                    serializerSettings: serializerSettings,
                    comparer: new CacheKey(0)
                    );
                
                PopulateStore(h[ht]);

                // ******
                // Uncomment below to move entire log to disk and eliminate data from memory as 
                // well. This will serve workload entirely from disk using read cache if enabled.
                // This will *prevent* future updates to the store. The in-mem buffer is no longer 
                // allocated.
                // h[ht].Log.DisposeFromMemory();
            }

            ContinuousRandomReadWorkload();

            for (int ht = 0; ht < kNumTables; ht++)
            {
                h[ht].Dispose();
            }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        private static void PopulateStore(FasterKV<CacheKey, CacheValue> store)
        {
            // Start session with FASTER
            using var s = store.For(new CacheFunctions()).NewSession<CacheFunctions>();

            Console.WriteLine("Writing keys from 0 to {0} to FASTER", numKeys);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < numKeys; i++)
            {
                if (i % (1 << 19) == 0)
                {
                    long workingSet = Process.GetCurrentProcess().WorkingSet64;
                    Console.WriteLine($"{i}: {workingSet / 1048576}M");
                }
                var key = new CacheKey(i);
                var value = new CacheValue(i);
                s.Upsert(ref key, ref value);
            }
            sw.Stop();
            Console.WriteLine("Total time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", numKeys, sw.ElapsedMilliseconds / 1000.0, numKeys / (sw.ElapsedMilliseconds / 1000.0));
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
            Console.WriteLine("Issuing uniform random read workload of {0} reads from thread {1}", numKeys, threadid);

            var sessions = new ClientSession<CacheKey, CacheValue, CacheValue, CacheValue, CacheContext, CacheFunctions>[kNumTables];

            for (int ht = 0; ht < kNumTables; ht++)
                sessions[ht] = h[ht].NewSession<CacheValue, CacheValue, CacheContext, CacheFunctions>(new CacheFunctions());

            var rnd = new Random(threadid);

            int statusPending = 0;
            CacheValue output = default;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            int i = 0;
            while (true)
            {
                if (i > 0 && (i % 256 == 0))
                {
                    for (int htcnt = 0; htcnt < kNumTables; htcnt++)
                        sessions[htcnt].CompletePending(false);
                    Interlocked.Add(ref totalReads, 256);
                }
                long k = rnd.Next(numKeys);

                var hts = sessions[rnd.Next(kNumTables)];
                var key = new CacheKey(k);
                var status = hts.Read(ref key, ref output);

                switch (status)
                {
                    case Status.PENDING:
                        statusPending++;
                        break;
                    case Status.OK:
                        if (output.value != key.key)
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
                sessions[ht].Dispose();
            */
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;

namespace CacheStore
{
    class Program
    {
        // Whether we enable a read cache
        static readonly bool useReadCache = false;
        // Number of keys in store
        const int numKeys = 1000000;

        static void Main()
        {
            // This sample shows the use of FASTER as a cache + key-value store.
            // Keys and values can be structs or classes.
            // Use blittable structs for *much* better performance

            // Create files for storing data
            var path = Path.GetTempPath() + "ClassCache/";
            var log =  Devices.CreateLogDevice(path + "hlog.log");

            // Log for storing serialized objects; needed only for class keys/values
            var objlog = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.obj.log");

            // Define settings for log
            var logSettings = new LogSettings {
                LogDevice = log, 
                ObjectLogDevice = objlog,
                ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
                // Uncomment below for low memory footprint demo
                // PageSizeBits = 12, // (4K pages)
                // MemorySizeBits = 20 // (1M memory for main log)
            };

            // Define serializers; otherwise FASTER will use the slower DataContract
            // Needed only for class keys/values
            var serializerSettings = new SerializerSettings<CacheKey, CacheValue> {
                keySerializer = () => new CacheKeySerializer(),
                valueSerializer = () => new CacheValueSerializer()
            };

            FasterKVSettings<CacheKey, CacheValue> fkvSettings = new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                ReadCacheEnabled = true,
                // Uncomment below for low memory footprint demo
                // PageSizeBits = 12, // (4K pages)
                // MemorySizeBits = 20 // (1M memory for main log)
                CheckpointDir = path,
                KeySerializer = serializerSettings?.keySerializer,
                ValueSerializer = serializerSettings?.valueSerializer,
                EqualityComparer = new CacheKey(0)
            };

            // Create instance of store
            var store = new FasterKV<CacheKey, CacheValue>(fkvSettings);

            // Populate the store
            PopulateStore(store);

            // ******
            // Uncomment below to take checkpoint and wait for its completion
            // This is FoldOver - it will flush the log to disk and store checkpoint metadata
            // (bool success, Guid token) = store.TakeFullCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();

            // ******
            // Uncomment below to copy entire log to disk, but retain tail of log in memory
            // store.Log.Flush(true);

            // ******
            // Uncomment below to move entire log to disk and eliminate data from memory as 
            // well. This will serve workload entirely from disk using read cache if enabled.
            // This will *allow* future updates to the store. The in-mem buffer stays allocated.
            // store.Log.FlushAndEvict(true);

            // ******
            // Uncomment below to move entire log to disk and eliminate data from memory as 
            // well. This will serve workload entirely from disk using read cache if enabled.
            // This will *prevent* future updates to the store. The in-mem buffer is no longer 
            // allocated.
            // store.Log.DisposeFromMemory();

            Console.Write("Enter read workload type (0 = random reads; 1 = interactive): ");
            var workload = int.Parse(Console.ReadLine());

            if (workload == 0)
                RandomReadWorkload(store, numKeys);
            else
                InteractiveReadWorkload(store);

            // Clean up
            store.Dispose();
            log.Dispose();
            objlog.Dispose();

            // Delete the created files
            try { new DirectoryInfo(path).Delete(true); } catch { }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        private static void PopulateStore(FasterKV<CacheKey, CacheValue> store)
        {
            // Start session with FASTER
            using var s = store.For(new CacheFunctions()).NewSession<CacheFunctions>();
            Console.WriteLine("Writing keys from 0 to {0} to FASTER", numKeys);

            Stopwatch sw = new();
            sw.Start();
            for (int i = 0; i < numKeys; i++)
            {
                if (i % (1 << 19) == 0)
                {
                    GC.Collect();
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

        private static void RandomReadWorkload(FasterKV<CacheKey, CacheValue> store, int max)
        {
            // Start session with FASTER
            using var s = store.For(new CacheFunctions()).NewSession<CacheFunctions>();

            Console.WriteLine("Issuing uniform random read workload of {0} reads", max);

            var rnd = new Random(0);

            int statusPending = 0;
            var output = default(CacheValue);
            Stopwatch sw = new();
            sw.Start();

            for (int i = 0; i < max; i++)
            {
                long k = rnd.Next(max);

                var key = new CacheKey(k);
                var status = s.Read(ref key, ref output);

                if (status.IsPending)
                {
                    statusPending++;
                    if (statusPending % 100 == 0)
                        s.CompletePending(false);
                    break;
                }
                else if (status.Found)
                {
                    if (output.value != key.key)
                        throw new Exception("Read error!");
                }
                else
                    throw new Exception("Error!");
            }
            s.CompletePending(true);
            sw.Stop();
            Console.WriteLine("Total time to read {0} elements: {1:0.000} secs ({2:0.00} reads/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
            Console.WriteLine($"Reads completed with PENDING: {statusPending}");
        }

        private static void InteractiveReadWorkload(FasterKV<CacheKey, CacheValue> store)
        {
            // Start session with FASTER
            using var s = store.For(new CacheFunctions()).NewSession<CacheFunctions>();

            Console.WriteLine("Issuing interactive read workload");

            // We use context to store and report latency of async operations
            var context = new CacheContext { type = 1 };

            while (true)
            {
                Console.Write("Enter key (int), -1 to exit: ");
                int k = int.Parse(Console.ReadLine());
                if (k == -1) break;

                var output = default(CacheValue);
                var key = new CacheKey(k);

                context.ticks = Stopwatch.GetTimestamp();
                var status = s.Read(ref key, ref output, context);
                if (status.IsPending)
                {
                    s.CompletePending(true);
                }
                else if (status.Found)
                {
                    long ticks = Stopwatch.GetTimestamp();
                    if (output.value != key.key)
                        Console.WriteLine("Sync: Incorrect value {0} found, latency = {1}ms", output.value, 1000 * (ticks - context.ticks) / (double)Stopwatch.Frequency);
                    else
                        Console.WriteLine("Sync: Correct value {0} found, latency = {1}ms", output.value, 1000 * (ticks - context.ticks) / (double)Stopwatch.Frequency);
                }
                else
                {
                    long ticks = Stopwatch.GetTimestamp() - context.ticks;
                    Console.WriteLine("Sync: Value not found, latency = {0}ms", new TimeSpan(ticks).TotalMilliseconds);
                }
            }
        }
    }
}

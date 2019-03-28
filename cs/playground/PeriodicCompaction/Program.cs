// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PeriodicCompaction
{
    class Program
    {
        // Whether we use read cache in this sample
        static readonly bool useReadCache = false;

        static void Main(string[] args)
        {
            Console.WriteLine("This sample runs forever and takes up around 200MB of storage space while running");

            var context = default(CacheContext);

            var log =  Devices.CreateLogDevice("hlog.log", deleteOnClose: true);
            var objlog = Devices.CreateLogDevice("hlog.obj.log", deleteOnClose: true);

            var logSettings = new LogSettings {
                LogDevice = log, ObjectLogDevice = objlog,
                MutableFraction = 0.9, MemorySizeBits = 20, PageSizeBits = 12, SegmentSizeBits = 20
            };

            if (useReadCache)
            {
                logSettings.ReadCacheSettings = new ReadCacheSettings();
            }

            var h = new FasterKV
                <CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions>(
                1L << 20, new CacheFunctions(), logSettings,
                null,
                new SerializerSettings<CacheKey, CacheValue> { keySerializer = () => new CacheKeySerializer(), valueSerializer = () => new CacheValueSerializer() }
                );

            h.StartSession();

            const int max = 1000000;

            Console.WriteLine("Writing keys from 0 to {0} to FASTER", max);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < max; i++)
            {
                if (i % 256 == 0)
                {
                    h.Refresh();
                    if (i % (1<<19) == 0)
                    {
                        long workingSet = Process.GetCurrentProcess().WorkingSet64;
                        Console.WriteLine($"{i}: {workingSet / 1048576}M");
                    }
                }
                var key = new CacheKey(i);
                var value = new CacheValue(i);
                h.Upsert(ref key, ref value, context, 0);
            }
            sw.Stop();
            Console.WriteLine("Total time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", max, sw.ElapsedMilliseconds/1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
            Console.WriteLine("Log tail address: {0}", h.Log.TailAddress);

            // Issue mix of deletes and upserts
            Random r = new Random(3);

            while (true)
            {
                for (int iter = 0; iter < 3; iter++)
                {
                    sw.Restart();
                    for (int i = 0; i < max; i++)
                    {
                        if (i % 256 == 0)
                        {
                            h.Refresh();
                            if (i % (1 << 19) == 0)
                            {
                                long workingSet = Process.GetCurrentProcess().WorkingSet64;
                                Console.WriteLine($"{i}: {workingSet / 1048576}M");
                            }
                        }


                        var key = new CacheKey(iter == 2 ? i : r.Next(max));
                        if (iter < 2 && r.Next(2) == 0)
                        {
                            h.Delete(ref key, context, 0);
                        }
                        else
                        {
                            var value = new CacheValue(key.key);
                            h.Upsert(ref key, ref value, context, 0);
                        }
                    }
                    sw.Stop();
                    Console.WriteLine("Total time to delete/upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
                    Console.WriteLine("Log tail address: {0}", h.Log.TailAddress);
                }

                h.CompletePending(true);
                Console.WriteLine("Compacting log");
                h.Log.Compact(h.Log.HeadAddress);

                Console.WriteLine("Log begin address: {0}", h.Log.BeginAddress);
                Console.WriteLine("Log tail address: {0}", h.Log.TailAddress);
            }
        }

        private static void RandomReadWorkload(FasterKV<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions> h, int max)
        {
            Console.WriteLine("Issuing uniform random read workload of {0} reads", max);

            var rnd = new Random(0);

            int statusPending = 0;
            var output = new CacheOutput();
            var context = new CacheContext();
            var input = default(CacheInput);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            for (int i = 0; i < max; i++)
            {
                long k = rnd.Next(max);

                var key = new CacheKey(k);
                var status = h.Read(ref key, ref input, ref output, context, 0);

                switch (status)
                {
                    case Status.PENDING:
                        statusPending++;
                        if (statusPending % 1000 == 0)
                            h.CompletePending(false);
                        break;
                    case Status.OK:
                        if (output.value.value != key.key)
                            throw new Exception("Read error!");
                        break;
                    default:
                        throw new Exception("Error!");
                }
            }
            h.CompletePending(true);
            sw.Stop();
            Console.WriteLine("Total time to read {0} elements: {1:0.000} secs ({2:0.00} reads/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
            Console.WriteLine($"Reads completed with PENDING: {statusPending}");
        }

        private static void InteractiveReadWorkload(FasterKV<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions> h, int max)
        {
            Console.WriteLine("Issuing interactive read workload");

            var context = new CacheContext { type = 1 };

            while (true)
            {
                Console.Write("Enter key (int), -1 to exit: ");
                int k = int.Parse(Console.ReadLine());
                if (k == -1) break;

                var output = new CacheOutput();
                var input = default(CacheInput);
                var key = new CacheKey(k);

                context.ticks = DateTime.Now.Ticks;
                var status = h.Read(ref key, ref input, ref output, context, 0);
                switch (status)
                {
                    case Status.PENDING:
                        h.CompletePending(true);
                        break;
                    case Status.OK:
                        long ticks = DateTime.Now.Ticks - context.ticks;
                        if (output.value.value != key.key)
                            Console.WriteLine("Sync: Incorrect value {0} found, latency = {1}ms", output.value.value, new TimeSpan(ticks).TotalMilliseconds);
                        else
                            Console.WriteLine("Sync: Correct value {0} found, latency = {1}ms", output.value.value, new TimeSpan(ticks).TotalMilliseconds);
                        break;
                    default:
                        ticks = DateTime.Now.Ticks - context.ticks;
                        Console.WriteLine("Sync: Value not found, latency = {0}ms", new TimeSpan(ticks).TotalMilliseconds);
                        break;
                }
            }
        }
    }
}

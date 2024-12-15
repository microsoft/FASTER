// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;

namespace StoreLogCompaction
{
    class Program
    {
        // Whether we use read cache in this sample
        static readonly bool useReadCache = false;

        static void Main()
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

            using var h = new FasterKV<CacheKey, CacheValue>(
                1L << 20,
                logSettings,
                serializerSettings: new SerializerSettings<CacheKey, CacheValue> { keySerializer = () => new CacheKeySerializer(), valueSerializer = () => new CacheValueSerializer() }
                );

            using var s = h.For(new CacheFunctions()).NewSession<CacheFunctions>();

            const int max = 1000000;

            Console.WriteLine("Writing keys from 0 to {0} to FASTER", max);

            Stopwatch sw = new();
            sw.Start();
            for (int i = 0; i < max; i++)
            {
                if (i % 256 == 0)
                {
                    s.Refresh();
                    if (i % (1<<19) == 0)
                    {
                        long workingSet = Process.GetCurrentProcess().WorkingSet64;
                        Console.WriteLine($"{i}: {workingSet / 1048576}M");
                    }
                }
                var key = new CacheKey(i);
                var value = new CacheValue(i);
                s.Upsert(ref key, ref value, context, 0);
            }
            sw.Stop();
            Console.WriteLine("Total time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", max, sw.ElapsedMilliseconds/1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
            Console.WriteLine("Log tail address: {0}", h.Log.TailAddress);

            // Issue mix of deletes and upserts
            Random r = new(3);

            while (true)
            {
                for (int iter = 0; iter < 3; iter++)
                {
                    sw.Restart();
                    for (int i = 0; i < max; i++)
                    {
                        if (i % (1 << 19) == 0)
                        {
                            long workingSet = Process.GetCurrentProcess().WorkingSet64;
                            Console.WriteLine($"{i}: {workingSet / 1048576}M");
                        }

                        var key = new CacheKey(iter == 2 ? i : r.Next(max));
                        if (iter < 2 && r.Next(2) == 0)
                        {
                            s.Delete(ref key, context, 0);
                        }
                        else
                        {
                            var value = new CacheValue(key.key);
                            s.Upsert(ref key, ref value, context, 0);
                        }
                    }
                    sw.Stop();
                    Console.WriteLine("Time to delete/upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", max, sw.ElapsedMilliseconds / 1000.0, max / (sw.ElapsedMilliseconds / 1000.0));
                    Console.WriteLine("Log begin address: {0}", h.Log.BeginAddress);
                    Console.WriteLine("Log tail address: {0}", h.Log.TailAddress);
                }

                s.CompletePending(true);

                sw.Restart();
                s.Compact(h.Log.HeadAddress, CompactionType.Scan);
                sw.Stop();
                Console.WriteLine("Time to compact: {0:0.000} secs", sw.ElapsedMilliseconds / 1000.0);
                h.Log.Truncate();

                Console.WriteLine("Log begin address: {0}", h.Log.BeginAddress);
                Console.WriteLine("Log tail address: {0}", h.Log.TailAddress);
            }
        }
    }
}

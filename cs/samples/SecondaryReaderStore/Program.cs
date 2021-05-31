// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace SecondaryReaderStore
{
    class Program
    {
        static FasterKV<long, long> primaryStore;
        static FasterKV<long, long> secondaryStore;
        const int numOps = 3000;
        const int checkpointFreq = 500;

        static void Main()
        {
            // Create files for storing data
            var path = Path.GetTempPath() + "SecondaryReaderStore/";
            if (Directory.Exists(path))
                new DirectoryInfo(path).Delete(true);

            var log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);

            primaryStore = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            secondaryStore = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            var p = new Thread(new ThreadStart(PrimaryWriter));
            var s = new Thread(new ThreadStart(SecondaryReader));

            p.Start(); s.Start();
            p.Join(); s.Join();

            log.Dispose();
            new DirectoryInfo(path).Delete(true);
        }

        static void PrimaryWriter()
        {
            using var s1 = primaryStore.NewSession(new SimpleFunctions<long, long>());

            Console.WriteLine($"Upserting keys at primary starting from key 0");
            for (long key=0; key<numOps; key++)
            {
                if (key > 0 && key % checkpointFreq == 0)
                {
                    Console.WriteLine($"Checkpointing primary until key {key - 1}");
                    primaryStore.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
                    Console.WriteLine($"Upserting keys at primary starting from {key}");
                }

                Thread.Sleep(10);
                s1.Upsert(ref key, ref key);

            }
            Console.WriteLine($"Checkpointing primary until key {numOps - 1}");
            primaryStore.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
            Console.WriteLine("Shutting down primary");
        }

        static void SecondaryReader()
        {
            using var s1 = secondaryStore.NewSession(new SimpleFunctions<long, long>());

            long key = 0, output = 0;
            while (true)
            {
                try
                {
                    secondaryStore.Recover(undoNextVersion: false); // read-only recovery, no writing back undos
                }
                catch
                {
                    Console.WriteLine("Nothing to recover to at secondary, retrying");
                    Thread.Sleep(500);
                    continue;
                }

                while (true)
                {
                    var status = s1.Read(ref key, ref output);
                    if (status == Status.NOTFOUND)
                    {
                        Console.WriteLine($"Key {key} not found at secondary; performing recovery to catch up");
                        Thread.Sleep(500);
                        break;
                    }
                    if (key != output)
                        throw new Exception($"Invalid value {output} found for key {key} at secondary");

                    Console.WriteLine($"Successfully read key {key}, value {output} at secondary");
                    key++;
                    if (key == numOps)
                    {
                        Console.WriteLine("Shutting down secondary");
                        return;
                    }
                }
            }

        }

    }
}

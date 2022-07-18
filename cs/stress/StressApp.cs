// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;

namespace FASTER.stress
{
    public class StressApp
    {
        public async static Task Main(string[] args)
        {
            TestLoader testLoader = new(args);
            if (testLoader.error)
                return;

            var options = testLoader.Options;   // shortcut

            // Add one tester to populate the static FasterKV instance and for Checkpointing and Compaction
            var testers = new IKeyTester[options.ThreadCount + 1];
            for (var ii = 0; ii < testers.Length; ii++)
            {
                var tid = ii + 1;   // ii is incremented so don't let it be captured
                testers[ii] = options.KeyType switch
                {
                    DataType.Long => new LongKeyTester(tid, testLoader),
                    DataType.String => new StringKeyTester(tid, testLoader),
                    DataType.SpanByte => new SpanByteKeyTester(tid, testLoader),
                    _ => throw new ApplicationException(testLoader.MissingKeyTypeHandler)
                };
            }

            // Use the last tester only to set up the static instance of FasterKV (each Stressie run has one TKey and one TValue, and thus
            // one static FasterKV instance specific to those TKey/TValue types).
            var tester = testers[^1];

            // Create devices and settings for the FasterKV instance.
            using var log = Devices.CreateLogDevice(Path.Combine(testLoader.OutputDirectory, $"stress.log"), deleteOnClose: true);
            using var objlog = testLoader.HasObjects ? Devices.CreateLogDevice(Path.Combine(testLoader.OutputDirectory, $"stress.obj.log"), deleteOnClose: true) : null;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = objlog };
            CheckpointSettings checkpointSettings = testLoader.UseCheckpoints ? new() { CheckpointDir = testLoader.OutputDirectory } : null;
            PopulateLogSettings(options, RecordInfo.GetLength() + tester.GetAverageRecordSize(), logSettings);

            // Determine hash table size.
            int hashTableSize = (int)Utility.PreviousPowerOf2(options.KeyCount * 2);
            int hashTableCacheLines = hashTableSize / 64; // sizeof(HashBucket);

            // Create and populate the FasterKV instance: the KeyTester contains a ValueTester instance of the TKey/TValue types that "owns" the static FasterKV instance.
            Console.WriteLine($"Creating and populating FasterKV<{options.KeyType}, {options.ValueType}> with {options.KeyCount} records and {hashTableSize} hash table size");
            tester.Populate(hashTableCacheLines, logSettings, checkpointSettings);

            long totalOps = (long)options.IterationCount * options.KeyCount * options.ThreadCount;
            Console.WriteLine($"Performing {options.IterationCount} iterations of {options.KeyCount} key operations over {options.ThreadCount} threads (total {totalOps})");

            // The background threads are the threads that run the periodic checkpoint and compaction, if either/both of these are specified.
            int numBackgroundThreads = (testLoader.UseCheckpoints ? 1 : 0) + (testLoader.UseCompact ? 1 : 0);
            var backgroundTasks = Array.Empty<Task>();
            CancellationTokenSource ctsBackground = new();
            if (numBackgroundThreads > 0)
            {
                var iBackgroundTask = 0;
                backgroundTasks = new Task[numBackgroundThreads];
                if (testLoader.UseCheckpoints)
                    backgroundTasks[iBackgroundTask++] = testLoader.DoPeriodicCheckpoints(tester.ValueTester, ctsBackground.Token);
                if (testLoader.UseCompact)
                    backgroundTasks[iBackgroundTask++] = testLoader.DoPeriodicCompact(tester.ValueTester, ctsBackground.Token);
            }

            // The KeyTester instances that will run the test (i.e. not the last one, that we used for static FasterKV setup) will 
            // call their contained ValueTesters to initialize for the test run.
            for (var ii = 0; ii < testers.Length; ii++)
                testers[ii].ValueTester.PrepareTest();

            // And off we go!
            Console.WriteLine("Beginning test");
            var sw = Stopwatch.StartNew();
            try
            {
                var tasks = new Task[options.ThreadCount];
                Random rng = new(testLoader.Options.RandomSeed);
                var iTask = 0;
                var numAsyncThreads = testLoader.NumAsyncThreads();
                for (; iTask < options.ThreadCount; ++iTask)
                {
                    var tindex = iTask; // iTask will be incremented so don't capture it
                    if (iTask < numAsyncThreads)
                        tasks[iTask] = testers[tindex].TestAsync();
                    else
                        tasks[iTask] = Task.Factory.StartNew(() => testers[tindex].Test());
                }
                await Task.WhenAll(tasks);
            }
            catch (AggregateException agg)
            {
                foreach (var ex in agg.InnerExceptions)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            sw.Stop();

            // Test is done.
            TimeSpan ts = new(sw.ElapsedTicks);
            Console.WriteLine($"Test complete in {ts}");

            // Tell any background threads to stop.
            if (numBackgroundThreads > 0)
            {
                Console.WriteLine("Canceling background threads");
                ctsBackground.Cancel();
                await Task.WhenAll(backgroundTasks);
            }

            Console.WriteLine($"{totalOps} operations complete");
            Console.WriteLine("Disposing Device");
        }

        private static void PopulateLogSettings(Options options, long recordSize, LogSettings logSettings)
        {
            // Main log
            long space = recordSize * options.KeyCount;
            ulong logInMemSpace = (ulong)Math.Floor(space * (options.LogInMemPercent / 100.0));
            logSettings.MemorySizeBits = Utility.GetLogBase2(logInMemSpace);
            logSettings.PageSizeBits = logSettings.MemorySizeBits - options.LogPageSizeShift;
            if (logSettings.PageSizeBits < 1 || (1 << logSettings.PageSizeBits) < recordSize * 2)
                throw new ApplicationException($"Log PageSizeBits too small");

            // SegmentSizeBits were already verified >= PageSizeBits in TestLoader
            logSettings.SegmentSizeBits = logSettings.MemorySizeBits - options.LogSegmentSizeShift;

            if (options.ReadCache)
            {
                logSettings.ReadCacheSettings = new();
                ulong rcInMemSpace = (ulong)Math.Floor(space * (options.ReadCacheInMemPercent / 100.0));
                logSettings.ReadCacheSettings.PageSizeBits = Utility.GetLogBase2(rcInMemSpace);
                logSettings.ReadCacheSettings.PageSizeBits = logSettings.ReadCacheSettings.MemorySizeBits - options.ReadCachePageSizeShift;
                if (logSettings.ReadCacheSettings.PageSizeBits < 1 || (1 << logSettings.ReadCacheSettings.PageSizeBits) < recordSize * 2)
                    throw new ApplicationException($"ReadCache PageSizeBits too small");
            }
        }
    }
}

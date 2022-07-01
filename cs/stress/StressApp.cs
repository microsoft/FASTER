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

            var tester = testers[^1];
            using var log = Devices.CreateLogDevice(Path.Combine(testLoader.OutputDirectory, $"stress.log"), deleteOnClose: true);
            using var objlog = testLoader.HasObjects ? Devices.CreateLogDevice(Path.Combine(testLoader.OutputDirectory, $"stress.obj.log"), deleteOnClose: true) : null;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = objlog };
            CheckpointSettings checkpointSettings = testLoader.UseCheckpoints ? new() { CheckpointDir = testLoader.OutputDirectory } : null;
            PopulateLogSettings(options, RecordInfo.GetLength() + tester.GetAverageRecordSize(), logSettings);

            int hashTableSize = (int)Utility.PreviousPowerOf2(options.KeyCount * 2);
            int hashTableCacheLines = hashTableSize / 64; // sizeof(HashBucket);

            Console.WriteLine($"Creating and populating FasterKV<{options.KeyType}, {options.ValueType}> with {options.KeyCount} records and {hashTableSize} hash table size");
            tester.Populate(hashTableCacheLines, logSettings, checkpointSettings);

            var totalOps = options.IterationCount * options.KeyCount * options.ThreadCount;
            Console.WriteLine($"Performing {options.IterationCount} iterations of {options.KeyCount} key operations over {options.ThreadCount} threads (total {totalOps})");

            int numExtraThreads = (testLoader.UseCheckpoints ? 1 : 0) + (testLoader.UseCompact ? 1 : 0);
            var extraTasks = Array.Empty<Task>();
            CancellationTokenSource cts = new();
            if (numExtraThreads > 0)
            {
                var iExtraTask = 0;
                extraTasks = new Task[numExtraThreads];
                if (testLoader.UseCheckpoints)
                    extraTasks[iExtraTask++] = testLoader.DoPeriodicCheckpoints(tester.ValueTester, cts.Token);
                if (testLoader.UseCompact)
                    extraTasks[iExtraTask++] = testLoader.DoPeriodicCompact(tester.ValueTester, cts.Token);
            }

            for (var ii = 0; ii < testers.Length; ii++)
                testers[ii].ValueTester.PrepareTest();

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
            cts.Cancel();
            TimeSpan ts = new(sw.ElapsedTicks);
            Console.WriteLine($"Test complete in {ts}");

            if (numExtraThreads > 0)
            {
                Console.WriteLine("Canceling extra threads");
                await Task.WhenAll(extraTasks);
            }

            Console.WriteLine($"{totalOps} operations complete");
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

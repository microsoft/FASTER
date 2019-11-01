// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.devices;

namespace FasterLogSample
{
    public class Program
    {
        // Entry length can be between 1 and ((1 << FasterLogSettings.PageSizeBits) - 4)
        const int entryLength = 1 << 10;
        static readonly byte[] staticEntry = new byte[entryLength];
        static FasterLog log;
        static FasterLogScanIterator iter;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            bool sync = true;
            var device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
            log = new FasterLog(new FasterLogSettings { LogDevice = device });

            // Populate entry being inserted
            for (int i = 0; i < entryLength; i++)
            {
                staticEntry[i] = (byte)i;
            }

            if (sync)
            {
                // Log writer thread: create as many as needed
                new Thread(new ThreadStart(LogWriterThread)).Start();

                // Threads for scan, reporting, commit
                new Thread(new ThreadStart(ScanThread)).Start();
                new Thread(new ThreadStart(ReportThread)).Start();
                new Thread(new ThreadStart(CommitThread)).Start();
            }
            else
            {
                // Async version of demo: expect lower performance
                // particularly for small payload sizes

                const int NumParallelTasks = 10_000;
                ThreadPool.SetMinThreads(2 * Environment.ProcessorCount, 2 * Environment.ProcessorCount);
                TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs e) =>
                {
                    Console.WriteLine($"Unobserved task exception: {e.Exception}");
                    e.SetObserved();
                };

                Task[] tasks = new Task[NumParallelTasks];
                for (int i = 0; i < NumParallelTasks; i++)
                {
                    int local = i;
                    tasks[i] = Task.Run(() => AsyncLogWriter(local));
                }

                var scan = Task.Run(() => AsyncScan());

                // Threads for reporting, commit
                new Thread(new ThreadStart(ReportThread)).Start();
                new Thread(new ThreadStart(CommitThread)).Start();

                Task.WaitAll(tasks);
                Task.WaitAll(scan);
            }
        }


        static void LogWriterThread()
        {
            while (true)
            {
                // TryEnqueue - can be used with throttling/back-off
                // Accepts byte[] and ReadOnlySpan<byte>
                while (!log.TryEnqueue(staticEntry, out _)) ;

                // Synchronous blocking enqueue
                // Accepts byte[] and ReadOnlySpan<byte>
                // log.Enqueue(entry);

                // Batched enqueue - batch must fit on one page
                // Add this to class:
                //   static readonly ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(10);
                // while (!log.TryEnqueue(spanBatch, out _)) ;
            }
        }

        /// <summary>
        /// Async version of enqueue
        /// </summary>
        static async Task AsyncLogWriter(int id)
        {
            bool batched = false;

            await Task.Yield();

            if (!batched)
            {
                // Single commit version - append each item and wait for commit
                // Needs high parallelism (NumParallelTasks) for perf
                // Needs separate commit thread to perform regular commit
                // Otherwise we commit only at page boundaries
                while (true)
                {
                    try
                    {
                        await log.EnqueueAndWaitForCommitAsync(staticEntry);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{nameof(AsyncLogWriter)}({id}): {ex}");
                    }
                }
            }
            else
            {
                // Batched version - we enqueue many entries to memory,
                // then wait for commit periodically
                int count = 0;
                while (true)
                {
                    await log.EnqueueAsync(staticEntry);
                    if (count++ % 100 == 0)
                    {
                        await log.WaitForCommitAsync();
                    }
                }
            }
        }

        static void ScanThread()
        {
            Random r = new Random();
            byte[] result;

            using (iter = log.Scan(log.BeginAddress, long.MaxValue))
            {
                while (true)
                {
                    while (!iter.GetNext(out result, out int length))
                    {
                        // For finite end address, check if iteration ended
                        // if (iter.CurrentAddress >= endAddress) return; 
                        iter.WaitAsync().GetAwaiter().GetResult();
                    }

                    // Memory pool variant:
                    // iter.GetNext(pool, out IMemoryOwner<byte> resultMem, out int length))

                    if (Different(result, staticEntry, out int location))
                        throw new Exception("Invalid entry found");

                    // Re-insert entry with small probability
                    if (r.Next(100) < 10)
                    {
                        log.Enqueue(result);
                    }

                    // Example of random read from given address
                    // (result, _) = log.ReadAsync(iter.CurrentAddress).GetAwaiter().GetResult();

                    log.TruncateUntil(iter.NextAddress);
                }
            }

            // Example of recoverable (named) iterator:
            // using (iter = log.Scan(log.BeginAddress, long.MaxValue, "foo"))
        }

        static async Task AsyncScan()
        {
            using (iter = log.Scan(log.BeginAddress, long.MaxValue))
                await foreach ((byte[] result, int length) in iter.GetAsyncEnumerable())
                {
                    if (Different(result, staticEntry, out int location))
                        throw new Exception("Invalid entry found");
                    log.TruncateUntil(iter.NextAddress);
                }
        }

        static void ReportThread()
        {
            long lastTime = 0;
            long lastValue = log.TailAddress;
            long lastIterValue = log.BeginAddress;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            while (true)
            {
                Thread.Sleep(5000);

                var nowTime = sw.ElapsedMilliseconds;
                var nowValue = log.TailAddress;

                Console.WriteLine("Append Throughput: {0} MB/sec, Tail: {1}",
                    (double) (nowValue - lastValue) / (1000 * (nowTime - lastTime)), nowValue);
                lastValue = nowValue;

                if (iter != null)
                {
                    var nowIterValue = iter.CurrentAddress;
                    Console.WriteLine("Scan Throughput: {0} MB/sec, Iter pos: {1}",
                        (double) (nowIterValue - lastIterValue) / (1000 * (nowTime - lastTime)), nowIterValue);
                    lastIterValue = nowIterValue;
                }

                lastTime = nowTime;
            }
        }

        static void CommitThread()
        {
            //Task<LinkedCommitInfo> prevCommitTask = null;
            while (true)
            {
                Thread.Sleep(5);
                log.Commit(true);

                // Async version
                // await log.CommitAsync();

                // Async version that catches all commit failures in between
                //try
                //{
                //    prevCommitTask = await log.CommitAsync(prevCommitTask);
                //}
                //catch (CommitFailureException e)
                //{
                //    Console.WriteLine(e);
                //    prevCommitTask = e.LinkedCommitInfo.nextTcs.Task;
                //}
            }
        }

        private static bool Different(byte[] b1, byte[] b2, out int location)
        {
            location = 0;
            if (b1.Length != b2.Length) return true;
            for (location = 0; location < b1.Length; location++)
            {
                if (b1[location] != b2[location])
                {
                    return true;
                }
            }
            return false;
        }

        private struct ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private readonly int batchSize;
            public ReadOnlySpanBatch(int batchSize) => this.batchSize = batchSize;
            public ReadOnlySpan<byte> Get(int index) => staticEntry;
            public int TotalEntries() => batchSize;
        }
    }
}

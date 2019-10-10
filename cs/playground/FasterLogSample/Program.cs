// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

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
                var t1 = new Thread(new ThreadStart(ScanThread));
                var t2 = new Thread(new ThreadStart(ReportThread));
                var t3 = new Thread(new ThreadStart(CommitThread));
                t1.Start(); t2.Start(); t3.Start();
                t1.Join(); t2.Join(); t3.Join();
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

                // Threads for scan, reporting, commit
                var t1 = new Thread(new ThreadStart(ScanThread));
                var t2 = new Thread(new ThreadStart(ReportThread));
                var t3 = new Thread(new ThreadStart(CommitThread));
                t1.Start(); t2.Start(); t3.Start();
                t1.Join(); t2.Join(); t3.Join();

                Task.WaitAll(tasks);
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
                        iter.WaitAsync().GetAwaiter().GetResult();
                    }

                    // Memory pool variant:
                    // iter.GetNext(pool, out IMemoryOwner<byte> resultMem, out int length))

                    if (Different(result, staticEntry, out int location))
                    { 
                        if (result.Length != staticEntry.Length)
                            throw new Exception("Invalid entry found, expected length " + staticEntry.Length + ", actual length " + result.Length);
                        else
                            throw new Exception("Invalid entry found at offset " + location);
                    }

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
                    (nowValue - lastValue) / (1000 * (nowTime - lastTime)), nowValue);
                lastValue = nowValue;

                if (iter != null)
                {
                    var nowIterValue = iter.CurrentAddress;
                    Console.WriteLine("Scan Throughput: {0} MB/sec, Iter pos: {1}",
                        (nowIterValue - lastIterValue) / (1000 * (nowTime - lastTime)), nowIterValue);
                    lastIterValue = nowIterValue;
                }

                lastTime = nowTime;
            }
        }

        static void CommitThread()
        {
            while (true)
            {
                Thread.Sleep(5);
                log.Commit(true);

                // Async version
                // await log.CommitAsync();
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

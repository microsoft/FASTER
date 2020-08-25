// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
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

            // Populate entry being inserted
            for (int i = 0; i < entryLength; i++)
            {
                staticEntry[i] = (byte)i;
            }

            IDevice device;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
            else
                device = Devices.CreateLogDevice("/mnt/tmp/hlog/hlog.log");

            log = new FasterLog(new FasterLogSettings { LogDevice = device });

            using (iter = log.Scan(log.BeginAddress, long.MaxValue))
            {
                if (sync)
                {
                    // Log writer thread: create as many as needed
                    new Thread(new ThreadStart(LogWriterThread)).Start();
                    
                    // Threads for iterator scan: create as many as needed
                    new Thread(() => ScanThread()).Start();

                    // Threads for reporting, commit
                    new Thread(new ThreadStart(ReportThread)).Start();
                    var t = new Thread(new ThreadStart(CommitThread));
                    t.Start();
                    t.Join();
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
            byte[] result;

            while (true)
            {
                while (!iter.GetNext(out result, out _, out _))
                {
                    // For finite end address, check if iteration ended
                    // if (currentAddress >= endAddress) return; 
                    iter.WaitAsync().GetAwaiter().GetResult();
                }

                // Memory pool variant:
                // iter.GetNext(pool, out IMemoryOwner<byte> resultMem, out int length, out long currentAddress)

                if (Different(result, staticEntry))
                    throw new Exception("Invalid entry found");

                // Example of random read from given address
                // (result, _) = log.ReadAsync(iter.CurrentAddress).GetAwaiter().GetResult();

                // Truncate until start of most recently read page
                log.TruncateUntilPageStart(iter.NextAddress);

                // Truncate log until after most recently read entry
                // log.TruncateUntil(iter.NextAddress);
            }

            // Example of recoverable (named) iterator:
            // using (iter = log.Scan(log.BeginAddress, long.MaxValue, "foo"))
        }

        static async Task AsyncScan()
        {
            await foreach ((byte[] result, int length, long currentAddress, long nextAddress) in iter.GetAsyncEnumerable())
            {
                if (Different(result, staticEntry))
                    throw new Exception("Invalid entry found");
                log.TruncateUntilPageStart(iter.NextAddress);
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
                    var nowIterValue = iter.NextAddress;
                    Console.WriteLine("Scan Throughput: {0} MB/sec, Iter pos: {1}",
                        (nowIterValue - lastIterValue) / (1000 * (nowTime - lastTime)), nowIterValue);
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

        private static bool Different(ReadOnlySpan<byte> b1, ReadOnlySpan<byte> b2)
        {
            return !b1.SequenceEqual(b2);
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

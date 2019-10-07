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
        /// <param name="args"></param>
        static void Main(string[] args)
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
                // Append thread: create as many as needed
                new Thread(new ThreadStart(AppendThread)).Start();

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
                    tasks[i] = Task.Run(() => AppendAsync(local));
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


        static void AppendThread()
        {
            while (true)
            {
                // TryAppend - can be used with throttling/back-off
                // Accepts byte[] and ReadOnlySpan<byte>
                while (!log.TryAppend(staticEntry, out _)) ;

                // Synchronous blocking append
                // Accepts byte[] and ReadOnlySpan<byte>
                // log.Append(entry);

                // Batched append - batch must fit on one page
                // while (!log.TryAppend(spanBatch, out _)) ;
            }
        }

        /// <summary>
        /// Async version of append
        /// </summary>
        static async Task AppendAsync(int id)
        {
            bool batched = false;

            await Task.Yield();

            if (!batched)
            {
                // Single commit version - append each item with commit
                // Needs high parallelism (NumParallelTasks) for perf
                while (true)
                {
                    try
                    {
                        await log.AppendAsync(staticEntry);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{nameof(AppendAsync)}({id}): {ex}");
                    }
                }
            }
            else
            {
                // Group-commit version - we append many entries to memory,
                // then wait for commit periodically
                int count = 0;
                while (true)
                {
                    await log.AppendToMemoryAsync(staticEntry);
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

            long lastAddress = 0;
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
                        log.Append(result);
                    }

                    if (iter.CurrentAddress - lastAddress > 500_000_000)
                    {
                        log.TruncateUntil(iter.CurrentAddress);
                        lastAddress = iter.CurrentAddress;
                    }
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
                log.FlushAndCommit(true);

                // Async version
                // await Task.Delay(5);
                // await log.FlushAndCommitAsync();
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

        // For batch append API
        static readonly ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(10);

        private struct ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private readonly int batchSize;
            public ReadOnlySpanBatch(int batchSize) => this.batchSize = batchSize;
            public ReadOnlySpan<byte> Get(int index) => staticEntry;
            public int TotalEntries() => batchSize;
        }
    }
}

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
        const int entryLength = 16380;
        static FasterLog log;

        static void ReportThread()
        {
            long lastTime = 0;
            long lastValue = log.TailAddress;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            while (true)
            {
                Thread.Sleep(5000);
                var nowTime = sw.ElapsedMilliseconds;
                var nowValue = log.TailAddress;

                Console.WriteLine("Throughput: {0} MB/sec, Tail: {1}",
                    (nowValue - lastValue) / (1000 * (nowTime - lastTime)), nowValue);
                lastTime = nowTime;
                lastValue = nowValue;
            }
        }

        static void CommitThread()
        {
            while (true)
            {
                Thread.Sleep(5);
                log.FlushAndCommit(true);
            }
        }

        static void AppendThread()
        {
            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            while (true)
            {
                // Sync append
                log.Append(entry);

                // We also support a Span-based variant of Append

                // We also support TryAppend to allow throttling/back-off
                // (expect this to be slightly slower than the sync version)
                // Make sure you supply a "starting" logical address of 0
                // Retries must send back the current logical address.
                // 
                // long logicalAddress = 0;
                // while (!log.TryAppend(entry, ref logicalAddress)) ;
            }
        }

        static void ScanThread()
        {
            Random r = new Random();

            Thread.Sleep(5000);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            var entrySpan = new Span<byte>(entry);


            long lastAddress = 0;
            Span<byte> result;
            using (var iter = log.Scan(0, long.MaxValue))
            {
                while (true)
                {
                    while (!iter.GetNext(out result))
                    {
                        Thread.Sleep(1000);
                    }

                    if (!result.SequenceEqual(entrySpan))
                    {
                        throw new Exception("Invalid entry found at offset " + FindDiff(result, entrySpan));
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

        private static int FindDiff(Span<byte> b1, Span<byte> b2)
        {
            for (int i = 0; i < b1.Length; i++)
            {
                if (b1[i] != b2[i])
                {
                    return i;
                }
            }
            return 0;
        }

        /// <summary>
        /// Main program entry point
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            bool sync = false;
            var device = Devices.CreateLogDevice("D:\\hitesh_logs\\hlog.log", deleteOnClose: true, recoverDevice: false);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });

            if (sync)
            {

                new Thread(new ThreadStart(AppendThread)).Start();

                // Can have multiple append threads if needed
                // new Thread(new ThreadStart(AppendThread)).Start();

                var t1 = new Thread(new ThreadStart(ScanThread));
                var t2 = new Thread(new ThreadStart(ReportThread));
                var t3 = new Thread(new ThreadStart(CommitThread));
                t1.Start(); t2.Start(); t3.Start();
                t1.Join(); t2.Join(); t3.Join();
            }
            else
            {
                TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs e) =>
                {
                    Console.WriteLine($"Unobserved task exception: {e.Exception}");
                    e.SetObserved();
                };

                ThreadPool.SetMinThreads(2 * Environment.ProcessorCount, 2 * Environment.ProcessorCount);

                // Async version of demo: expect lower performance
                const int NumParallelTasks = 10_000;
                for (int i = 0; i < entryLength; i++)
                {
                    staticEntry[i] = (byte)i;
                }

                Task[] tasks = new Task[NumParallelTasks];
                for (int i = 0; i < NumParallelTasks; i++)
                {
                    int local = i;
                    tasks[i] = Task.Run(() => AppendAsync(local));
                }
                // tasks[NumParallelTasks] = Task.Run(() => CommitAsync());

                // Use threads for scan and reporting
                var t1 = new Thread(new ThreadStart(ScanThread));
                var t2 = new Thread(new ThreadStart(ReportThread));
                var t3 = new Thread(new ThreadStart(CommitThread));
                t1.Start(); t2.Start(); t3.Start();
                t1.Join(); t2.Join(); t3.Join();

                Task.WaitAll(tasks);
            }
        }

        static readonly byte[] staticEntry = new byte[entryLength];

        static async Task AppendAsync(int id)
        {
            await Task.Yield();

            // Simple version - append with commit
            // Needs very high parallelism for perf
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

            // Batched version - we append to memory, wait for commit periodically
            // int count = 0;
            // while (true)
            // {
            //     await log.AppendToMemoryAsync(entry);
            //     if (count++ % 100 == 0)
            //     {
            //         await log.WaitForCommitAsync();
            //     }
            // }
        }
        static async Task CommitAsync()
        {
            while (true)
            {
                try
                {
                    await Task.Delay(5);
                    await log.FlushAndCommitAsync();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{nameof(CommitAsync)}: {ex}");
                }
            }
        }
    }
}

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
        const int entryLength = 996;
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
                    (nowValue - lastValue) / (1000*(nowTime - lastTime)), nowValue);
                lastTime = nowTime;
                lastValue = nowValue;
            }
        }

        static void CommitThread()
        {
            while (true)
            {
                Thread.Sleep(100);
                log.FlushAndCommit(true);
            }
        }

        static void AppendThread()
        {
            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

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
                entry[i] = (byte)i;
            var entrySpan = new Span<byte>(entry);


            long lastAddress = 0;
            Span<byte> result;
            using (var iter = log.Scan(0, long.MaxValue))
            {
                while (true)
                {
                    while (!iter.GetNext(out result))
                        Thread.Sleep(1000);
                    if (!result.SequenceEqual(entrySpan))
                    {
                        throw new Exception("Invalid entry found at offset " + FindDiff(result, entrySpan));
                    }

                    // Re-insert entry with small probability
                    if (r.Next(100) < 10)
                        log.Append(result);

                    if (iter.CurrentAddress - lastAddress > 500000000)
                    {
                        log.TruncateUntil(iter.CurrentAddress);
                        lastAddress = iter.CurrentAddress;
                    }
                }
            }
        }

        private static int FindDiff(Span<byte> b1, Span<byte> b2)
        {
            for (int i=0; i<b1.Length; i++)
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
            bool sync = true;
            var device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
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
                // Async version of demo: expect lower performance
                const int NumParallelTasks = 100;
                Task[] tasks = new Task[NumParallelTasks + 1];
                for (int i = 0; i < NumParallelTasks; i++)
                {
                    tasks[i] = AppendAsync();
                }
                tasks[NumParallelTasks] = CommitAsync();


                // Use threads for scan and reporting
                var t1 = new Thread(new ThreadStart(ScanThread));
                var t2 = new Thread(new ThreadStart(ReportThread));
                t1.Start();
                t2.Start();
                t1.Join();
                t2.Join();

                Task.WaitAll(tasks);
            }
        }

        static async Task AppendAsync()
        {
            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            while (true)
            {
                await log.AppendAsync(entry);
            }
        }

        static async Task CommitAsync()
        {
            while (true)
            {
                await Task.Delay(100);
                await log.FlushAndCommitAsync();
            }
        }
    }
}

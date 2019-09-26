// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
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

                Console.WriteLine("Throughput: {0} MB/sec",
                    (nowValue - lastValue) / (1000*(nowTime - lastTime)));
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
                log.Append(entry);

                // We also support a Span-based version of Append

                // We also support TryAppend to allow throttling/back-off:
                // while (!log.TryAppend(entry, out long logicalAddress))
                // {
                //    Thread.Sleep(10);
                // }
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

        static int FindDiff(Span<byte> b1, Span<byte> b2)
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

        static void Main(string[] args)
        {
            var device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
            log = new FasterLog(new FasterLogSettings { LogDevice = device });

            new Thread(new ThreadStart(AppendThread)).Start();
            
            // Can have multiple append threads if needed
            // new Thread(new ThreadStart(AppendThread)).Start();
            
            new Thread(new ThreadStart(ScanThread)).Start();
            new Thread(new ThreadStart(ReportThread)).Start();
            new Thread(new ThreadStart(CommitThread)).Start();

            Thread.Sleep(500*1000);
        }
    }
}

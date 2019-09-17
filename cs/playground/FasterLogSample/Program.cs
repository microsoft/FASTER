// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.core.log;
using System;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FasterLogSample
{
    public class Program
    {
        const int entryLength = 100;
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

        static void AppendThread()
        {
            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            while (true)
            {
                log.Append(entry);
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
                        throw new Exception("Invalid entry found");
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

        static void Main(string[] args)
        {
            var device = Devices.CreateLogDevice("D:\\logs\\hlog.log");
            log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 29, PageSizeBits = 25 });

            new Thread(new ThreadStart(AppendThread)).Start();
            new Thread(new ThreadStart(ScanThread)).Start();
            new Thread(new ThreadStart(ReportThread)).Start();

            Thread.Sleep(500*1000);
        }
    }
}

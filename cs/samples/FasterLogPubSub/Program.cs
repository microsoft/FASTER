// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FasterLogPubSub
{
    class Program
    {
        static async Task Main()
        {
            var path = Path.GetTempPath() + "FasterLogPubSub\\";

            var device = Devices.CreateLogDevice(path + "mylog");

            var log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9 });

            using var cts = new CancellationTokenSource();

            var producer = ProducerAsync(log, cts.Token);
            var commiter = CommiterAsync(log, cts.Token);
            var consumer = ConsumerAsync(log, cts.Token);

            Console.CancelKeyPress += (o, eventArgs) =>
            {
                Console.WriteLine("Cancelling program...");
                eventArgs.Cancel = true;
                cts.Cancel();
            };

            await producer;
            await consumer;
            await commiter;

            Console.WriteLine("Finished.");

            log.Dispose();
            try { new DirectoryInfo(path).Delete(true); } catch { }
        }

        static async Task CommiterAsync(FasterLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(5000), cancellationToken);

                Console.WriteLine("Committing...");

                await log.CommitAsync();
            }
        }

        static async Task ProducerAsync(FasterLog log, CancellationToken cancellationToken)
        {
            var i = 0L;
            while (!cancellationToken.IsCancellationRequested)
            {
                Console.WriteLine($"Producing {i}");

                log.Enqueue(Encoding.UTF8.GetBytes(i.ToString()));
                log.RefreshUncommitted();

                i++;

                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        static async Task ConsumerAsync(FasterLog log, CancellationToken cancellationToken)
        {
            using var iter = log.Scan(log.BeginAddress, long.MaxValue, "foo", true, ScanBufferingMode.DoublePageBuffering, scanUncommitted: true);

            int count = 0;
            await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
            {
                Console.WriteLine($"Consuming {Encoding.UTF8.GetString(result)}");
                iter.CompleteUntil(nextAddress);
                log.TruncateUntil(nextAddress);

                // We simulate temporary slow down of data consumption
                // This will cause transient log spill to disk (observe folder on storage)
                if (count++ > 1000 && count < 1200)
                    Thread.Sleep(100);
            }
        }

    }
}

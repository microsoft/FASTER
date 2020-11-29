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
        const int commitPeriodMs = 2000;
        const int restorePeriodMs = 1000;
        static string path = Path.GetTempPath() + "FasterLogPubSub/";

        static async Task Main()
        {
            var device = Devices.CreateLogDevice(path + "mylog");

            var log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9 });

            using var cts = new CancellationTokenSource();

            var producer = ProducerAsync(log, cts.Token);
            var commiter = CommitterAsync(log, cts.Token);

            // Consumer on SAME FasterLog instance
            var consumer = ConsumerAsync(log, true, cts.Token);

            // Uncomment below to run consumer on SEPARATE read-only FasterLog instance
            // var consumer = SeparateConsumerAsync(cts.Token);
            
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

        static async Task CommitterAsync(FasterLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(commitPeriodMs), cancellationToken);

                Console.WriteLine("Committing...");

                await log.CommitAsync();
            }
        }

        static async Task ProducerAsync(FasterLog log, CancellationToken cancellationToken)
        {
            var i = 0L;
            while (!cancellationToken.IsCancellationRequested)
            {
                // Console.WriteLine($"Producing {i}");

                log.Enqueue(Encoding.UTF8.GetBytes(i.ToString()));
                log.RefreshUncommitted();

                i++;

                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        static async Task ConsumerAsync(FasterLog log, bool scanUncommitted, CancellationToken cancellationToken)
        {
            using var iter = log.Scan(log.BeginAddress, long.MaxValue, "foo", true, ScanBufferingMode.DoublePageBuffering, scanUncommitted);

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

        static async Task SeparateConsumerAsync(CancellationToken cancellationToken)
        {
            var device = Devices.CreateLogDevice(path + "mylog");
            var log = new FasterLog(new FasterLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 });
            var _ = RecoverAsync(log, cancellationToken);

            using var iter = log.Scan(log.BeginAddress, long.MaxValue);

            await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
            {
                Console.WriteLine($"Consuming {Encoding.UTF8.GetString(result)}");
                iter.CompleteUntil(nextAddress);
            }
        }

        static async Task RecoverAsync(FasterLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(restorePeriodMs), cancellationToken);

                Console.WriteLine("Restoring ...");

                log.RecoverReadOnly();
            }
        }

    }
}

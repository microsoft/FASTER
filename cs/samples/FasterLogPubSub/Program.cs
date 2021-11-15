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
            // This is two samples in one, enumerating over the same FasterLog instance that does commits, or over a separate
            // FasterLog that opens the log file read-only and continuously catches up with the first intance's commits.
            const bool sameInstance = true;
#pragma warning disable CS0162 // Unreachable code detected
            if (!sameInstance)
            {
                // Because the SAME-instance iterator illustrates truncating the log, the SEPARATE-instance may encounter EOF
                // issues if it is run after that truncation without cleaning up the directory first.
                // In all other cases, the sample should run without needing to clean up the directory.
                //if (Directory.Exists(path)) Directory.Delete(path, true);
            }

            var device = Devices.CreateLogDevice(path + "mylog");
            var log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9, RemoveOutdatedCommitFiles = sameInstance });
            using var cts = new CancellationTokenSource();

            var producer = ProducerAsync(log, cts.Token);
            var committer = CommitterAsync(log, cts.Token);

            Task consumer;
            if (sameInstance)
            {
                // Consumer on SAME FasterLog instance
                consumer = ConsumerAsync(log, true, cts.Token);
            }
            else
            {
                // Consumer on SEPARATE read-only FasterLog instance
                consumer = SeparateConsumerAsync(cts.Token);
            }
#pragma warning restore CS0162 // Unreachable code detected

            Console.CancelKeyPress += (o, eventArgs) =>
            {
                Console.WriteLine("Cancelling program...");
                eventArgs.Cancel = true;
                cts.Cancel();
            };

            await producer;
            await consumer;
            await committer;
 
            Console.WriteLine("Finished.");

            log.Dispose();
            device.Dispose();
            try { new DirectoryInfo(path).Delete(true); } catch { }
        }

        static async Task CommitterAsync(FasterLog log, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(commitPeriodMs), cancellationToken);

                    Console.WriteLine("Committing...");

                    await log.CommitAsync(token: cancellationToken);
                }
            }
            catch (OperationCanceledException) { }
            Console.WriteLine("Committer complete");
        }

        static async Task ProducerAsync(FasterLog log, CancellationToken cancellationToken)
        {
            try
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
            catch (OperationCanceledException) { }
            Console.WriteLine("Producer complete");
        }

        static async Task ConsumerAsync(FasterLog log, bool scanUncommitted, CancellationToken cancellationToken)
        {
            using var iter = log.Scan(log.BeginAddress, long.MaxValue, "foo", true, ScanBufferingMode.DoublePageBuffering, scanUncommitted);

            try
            {
                int count = 0;
                await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
                {
                    Console.WriteLine($"Same Log Consuming {Encoding.UTF8.GetString(result)}");
                    iter.CompleteUntil(nextAddress);
                    log.TruncateUntil(nextAddress);

                    // Simulate temporary slow down of data consumption
                    // This will cause transient log spill to disk (observe folder on storage)
                    if (count++ > 1000 && count < 1200)
                        Thread.Sleep(100);
                }
            }
            catch (OperationCanceledException) { }
            Console.WriteLine("Consumer complete");
        }

        // This creates a separate FasterLog over the same log file, using RecoverReadOnly to continuously update
        // to the primary FasterLog's commits.
        static async Task SeparateConsumerAsync(CancellationToken cancellationToken)
        {
            using var device = Devices.CreateLogDevice(path + "mylog");
            using var log = new FasterLog(new FasterLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 });

            try
            {
                var _ = BeginRecoverAsyncLoop(log, cancellationToken);

                // This enumerator waits asynchronously when we have reached the committed tail of the duplicate FasterLog. When RecoverReadOnly
                // reads new data committed by the primary FasterLog, it signals commit completion to let iter continue to the new tail.
                using var iter = log.Scan(log.BeginAddress, long.MaxValue);
                await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
                {
                    Console.WriteLine($"Separate Log Consuming {Encoding.UTF8.GetString(result)}");
                    iter.CompleteUntil(nextAddress);
                }
            }
            catch (OperationCanceledException) { }
            Console.WriteLine("SeparateConsumer complete");
        }

        static async Task BeginRecoverAsyncLoop(FasterLog log, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // Delay for a while before checking again.
                    await Task.Delay(TimeSpan.FromMilliseconds(restorePeriodMs), cancellationToken);

                    Console.WriteLine("Restoring Separate Log...");

                    // Recover to the last commit by the primary FasterLog instance.
                    await log.RecoverReadOnlyAsync(true, cancellationToken);
                }
            }
            catch (OperationCanceledException) { }
            Console.WriteLine("RecoverAsyncLoop complete");
        }
    }
}

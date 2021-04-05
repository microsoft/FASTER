using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using FASTER.core;

#pragma warning disable CS0162 // Unreachable code detected

namespace AsyncStress
{
    public class Program
    {
        public static async Task Main() => await ProfileStore(new FasterWrapper());

        private static async Task ProfileStore(FasterWrapper store)
        {
            Stopwatch stopWatch = new Stopwatch();
            const int numOperations = 1_000_000;
            const bool singleThreadUpsert = false;
            const bool singleThreadRead = false;
            var writeTasks = new ConcurrentDictionary<long, Task>();
            var readTasks = new ConcurrentDictionary<long, Task<(Status, int?)>>();

            // Insert
            Console.WriteLine($"    Inserting {numOperations} records ...");
            stopWatch.Start();
            if (singleThreadUpsert)
            {
                for (int i = 0; i < numOperations; i++)
                    await store.UpsertAsync(i, i);
            }
            else
            {
                Parallel.For(0, numOperations, key => writeTasks.TryAdd(key, store.UpsertAsync(key, key)));
                await Task.WhenAll(writeTasks.Values).ConfigureAwait(false);
            }
            stopWatch.Stop();
            Console.WriteLine($"    Insertion complete in {stopWatch.ElapsedMilliseconds} ms");

            // Read
            Console.WriteLine($"    Reading {numOperations} records ...");
            stopWatch.Restart();
            if (singleThreadRead)
            {
                for (int i = 0; i < numOperations; i++)
                {
                    readTasks[i] = store.ReadAsync(i);
                    await readTasks[i];
                }
            }
            else
            {
                Parallel.For(0, numOperations, key => readTasks.TryAdd(key, store.ReadAsync(key)));
                await Task.WhenAll(readTasks.Values).ConfigureAwait(false);
            }
            stopWatch.Stop();
            Console.WriteLine($"    Reads complete in {stopWatch.ElapsedMilliseconds} ms");

            // Verify
            Console.WriteLine("    Verifying read results ...");
            Parallel.For(0, numOperations, key =>
            {
                (Status status, int? result) = readTasks[key].Result;
                Assert.Equal(Status.OK, status);
                Assert.Equal(key, result);
            });

            Console.WriteLine("    Results verified");

            store.Dispose();
        }
    }
}

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using FASTER.core;

#pragma warning disable CS0162 // Unreachable code detected

namespace AsyncStress
{
    public class Program
    {
        static bool singleThreadUpsert = false;
        static bool singleThreadRead = false;

        public static async Task Main(string[] args)
        {
            if (args.Length > 0)
            {
                foreach (var arg in args)
                {
                    if (arg == "su")
                        singleThreadUpsert = true;
                    else if (arg == "sr")
                        singleThreadRead = true;
                    else
                        throw new ApplicationException($"Unknown switch: {arg}");
                }
            }
            await ProfileStore(new FasterWrapper());
        }

        private static async Task ProfileStore(FasterWrapper store)
        {
            Stopwatch stopWatch = new Stopwatch();
            const int numOperations = 1_000_000;
            var writeTasks = new Task[numOperations];
            var readTasks = new Task<(Status, int)>[numOperations];

            // Insert
            Console.WriteLine($"    Inserting {numOperations} records {(singleThreadUpsert ? "single-threaded" : "multi-threaded")} ...");
            stopWatch.Start();
            if (singleThreadUpsert)
            {
                for (int i = 0; i < numOperations; i++)
                    await store.UpsertAsync(i, i);
            }
            else
            {
                Parallel.For(0, numOperations, key => writeTasks[key] = store.UpsertAsync(key, key));
                await Task.WhenAll(writeTasks).ConfigureAwait(false);
            }
            stopWatch.Stop();
            Console.WriteLine($"    Insertion complete in {stopWatch.ElapsedMilliseconds} ms");

            // Read
            Console.WriteLine($"    Reading {numOperations} records {(singleThreadUpsert ? "single-threaded" : "multi-threaded")} ...");
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
                Parallel.For(0, numOperations, key => readTasks[key] = store.ReadAsync(key));
                await Task.WhenAll(readTasks).ConfigureAwait(false);
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

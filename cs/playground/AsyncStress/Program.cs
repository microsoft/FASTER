using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using FASTER.core;
using System.Linq;

namespace AsyncStress
{
    public class Program
    {
        enum ThreadingMode
        {
            None,           // Do not run this operation
            Single,         // Run this operation single-threaded
            ParallelAsync,  // Run this operation using Parallel.For with an Async lambda
            ParallelSync,   // Run this operation using Parallel.For with an Sync lambda and parallelism limited to numTasks
            Chunks          // Run this operation using a set number of tasks to operate on partitioned chunks
        }
        static ThreadingMode upsertThreadingMode = ThreadingMode.ParallelAsync;
        static ThreadingMode readThreadingMode = ThreadingMode.ParallelAsync;
        static int numTasks = 4;
        static int numOperations = 1_000_000;

        static void Usage()
        {
            Console.WriteLine($"Options:");
            Console.WriteLine($"    -u <mode>:      Upsert threading mode (listed below); default is {ThreadingMode.ParallelAsync}");
            Console.WriteLine($"    -r <mode>:      Read threading mode (listed below); default is {ThreadingMode.ParallelAsync}");
            Console.WriteLine($"    -t #:           Number of tasks for {ThreadingMode.ParallelSync} and {ThreadingMode.Chunks}; default is {numTasks}");
            Console.WriteLine($"    -n #:           Number of operations; default is {numOperations}");
            Console.WriteLine($"    -b #:           Use OS buffering for reads; default is {FasterWrapper.useOsReadBuffering}");
            Console.WriteLine($"    -?, /?, --help: Show this screen");
            Console.WriteLine();
            Console.WriteLine($"Threading Modes:");
            Console.WriteLine($"    None:           Do not run this operation");
            Console.WriteLine($"    Single:         Run this operation single-threaded");
            Console.WriteLine($"    ParallelAsync:  Run this operation using Parallel.For with an Async lambda");
            Console.WriteLine($"    ParallelSync:   Run this operation using Parallel.For with an Sync lambda and parallelism limited to numTasks");
            Console.WriteLine($"    Chunks:         Run this operation using a set number of tasks to operate on partitioned chunks");
        }

        public static async Task Main(string[] args)
        {
            if (args.Length > 0)
            {
                for (var ii = 0; ii < args.Length; ++ii)
                {
                    var arg = args[ii];
                    string nextArg()
                    {
                        var next = ii < args.Length - 1 && args[ii + 1][0] != '-' ? args[++ii] : string.Empty;
                        if (next.Length == 0)
                            throw new ApplicationException($"Need arg value for {arg}");
                        return next;
                    }

                    if (arg == "-u")
                        upsertThreadingMode = Enum.Parse<ThreadingMode>(nextArg(), ignoreCase: true);
                    else if (arg == "-r")
                        readThreadingMode = Enum.Parse<ThreadingMode>(nextArg(), ignoreCase: true);
                    else if (arg == "-t")
                        numTasks = int.Parse(nextArg());
                    else if (arg == "-n")
                        numOperations = int.Parse(nextArg());
                    else if (arg == "-b")
                        FasterWrapper.useOsReadBuffering = true;
                    else if (arg == "-?" || arg == "/?" || arg == "--help")
                    {
                        Usage();
                        return;
                    }
                    else
                        throw new ApplicationException($"Unknown switch: {arg}");
                }
            }
            await ProfileStore(new FasterWrapper());
        }

        private static async Task ProfileStore(FasterWrapper store)
        {
            static string threadingModeString(ThreadingMode threadingMode)
                => threadingMode switch
                {
                    ThreadingMode.Single => "Single threading",
                    ThreadingMode.ParallelAsync => "Parallel.For using async lambda",
                    ThreadingMode.ParallelSync => $"Parallel.For using sync lambda and {numTasks} tasks",
                    ThreadingMode.Chunks => $"Chunks partitioned across {numTasks} tasks",
                    _ => throw new ApplicationException("Unknown threading mode")
                };

            int chunkSize = numOperations / numTasks;

            // Insert
            if (upsertThreadingMode == ThreadingMode.None)
            {
                throw new ApplicationException("Cannot Skip Upserts");
            }
            else
            {
                Console.WriteLine($"    Inserting {numOperations} records with {threadingModeString(upsertThreadingMode)} ...");

                var sw = Stopwatch.StartNew();
                if (upsertThreadingMode == ThreadingMode.Single)
                {
                    for (int i = 0; i < numOperations; i++)
                        await store.UpsertAsync(i, i);
                }
                else if (upsertThreadingMode == ThreadingMode.ParallelAsync)
                {
                    var writeTasks = new ValueTask[numOperations];
                    Parallel.For(0, numOperations, key => writeTasks[key] = store.UpsertAsync(key, key));
                    foreach (var task in writeTasks)
                        await task;
                }
                else if (upsertThreadingMode == ThreadingMode.ParallelSync)
                {
                    // Without throttling parallelism, this ends up very slow with many threads waiting on FlushTask.
                    var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = numTasks };
                    Parallel.For(0, numOperations, parallelOptions, key => store.Upsert(key, key));
                }
                else
                {
                    Debug.Assert(upsertThreadingMode == ThreadingMode.Chunks);
                    var writeTasks = new ValueTask[numTasks];
                    for (int ii = 0; ii < numTasks; ii++)
                        writeTasks[ii] = store.UpsertChunkAsync(ii * chunkSize, chunkSize);
                    foreach (var task in writeTasks)
                        await task;
                }
                sw.Stop();
                Console.WriteLine($"    Insertion complete in {sw.ElapsedMilliseconds} ms; TailAddress = {store.TailAddress}, Pending = {store.UpsertPendingCount}");
            }

            // Read
            Console.WriteLine();
            if (readThreadingMode == ThreadingMode.None)
            {
                Console.WriteLine("    Skipping Reads");
            }
            else
            {
                Console.WriteLine($"    Reading {numOperations} records with {threadingModeString(readThreadingMode)} (OS buffering: {FasterWrapper.useOsReadBuffering}) ...");
                var readTasks = new ValueTask<(Status, int)>[numOperations];

                var sw = Stopwatch.StartNew();
                if (readThreadingMode == ThreadingMode.Single)
                {
                    for (int ii = 0; ii < numOperations; ii++)
                    {
                        readTasks[ii] = store.ReadAsync(ii);
                        await readTasks[ii];
                    }
                }
                else if (readThreadingMode == ThreadingMode.ParallelAsync)
                {
                    Parallel.For(0, numOperations, key => readTasks[key] = store.ReadAsync(key));
                    foreach (var task in readTasks)
                        await task;
                }
                else if (readThreadingMode == ThreadingMode.ParallelSync)
                { 
                    // Without throttling parallelism, this ends up very slow with many threads waiting on completion.
                    var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = numTasks };
                    Parallel.For(0, numOperations, parallelOptions, key => readTasks[key] = store.Read(key));
                    foreach (var task in readTasks)
                        await task;
                }
                else
                {
                    var chunkTasks = Enumerable.Range(0, numTasks).Select(ii => store.ReadChunkAsync(ii * chunkSize, chunkSize, readTasks)).ToArray();
                    foreach (var chunkTask in chunkTasks)
                        await chunkTask;
                    foreach (var task in readTasks)
                        await task;
                }

                sw.Stop();
                Console.WriteLine($"    Reads complete in {sw.ElapsedMilliseconds} ms");

                // Verify
                Console.WriteLine("    Verifying read results ...");
                Parallel.For(0, numOperations, key =>
                {
                    (Status status, int? result) = readTasks[key].Result;
                    Assert.Equal(Status.OK, status);
                    Assert.Equal(key, result);
                });

                Console.WriteLine("    Results verified");
            }

            store.Dispose();
        }
    }
}

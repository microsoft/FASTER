// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using FASTER.core;

namespace AsyncStress
{
    public class Program
    {
        // ThreadingMode descriptions are listed in Usage()
        enum ThreadingMode
        {
            None,
            Single,
            ParallelFor,
            Chunks
        }
        static ThreadingMode upsertThreadingMode = ThreadingMode.ParallelFor;
        static ThreadingMode readThreadingMode = ThreadingMode.ParallelFor;
        static int numChunks = 10;
        static int numOperations = 1_000_000;

        static void Usage()
        {
            Console.WriteLine($"Options:");
            Console.WriteLine($"    -u <mode>:      Upsert threading mode (listed below); default is {ThreadingMode.ParallelFor}");
            Console.WriteLine($"    -r <mode>:      Read threading mode (listed below); default is {ThreadingMode.ParallelFor}");
            Console.WriteLine($"    -c #:           Number of chunks for {ThreadingMode.Chunks}; default is {numChunks}");
            Console.WriteLine($"    -n #:           Number of operations; default is {numOperations}");
            Console.WriteLine($"    -b #:           Use OS buffering for reads; default is false");
            Console.WriteLine($"    -?, /?, --help: Show this screen");
            Console.WriteLine();
            Console.WriteLine($"Threading Modes:");
            Console.WriteLine($"    None:           Do not run this operation");
            Console.WriteLine($"    Single:         Run this operation single-threaded");
            Console.WriteLine($"    ParallelFor:    Run this operation using Parallel.For with an Async lambda");
            Console.WriteLine($"    Chunks:         Run this operation using a set number of data chunks as async tasks, each chunk runs operations serially");
        }

        public static async Task Main(string[] args)
        {
            bool useOsReadBuffering = false;

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
                    else if (arg == "-c")
                        numChunks = int.Parse(nextArg());
                    else if (arg == "-n")
                        numOperations = int.Parse(nextArg());
                    else if (arg == "-b")
                        useOsReadBuffering = true;
                    else if (arg == "-?" || arg == "/?" || arg == "--help")
                    {
                        Usage();
                        return;
                    }
                    else
                        throw new ApplicationException($"Unknown switch: {arg}");
                }
            }

            // Store with value types, no object log
            // await ProfileStore(new FasterWrapper<long, long>(useOsReadBuffering), e => (long)e, e => (long)e);

            // Store with reference types, using object log
            // await ProfileStore(new FasterWrapper<string, string>(useOsReadBuffering), e => $"key {e}", e => $"value {e}");

            // Store with reference or value types, no object log (store serialized bytes)
            await ProfileStore(new SerializedFasterWrapper<string, string>(useOsReadBuffering), e => $"key {e}", e => $"value {e}");
        }

        private static async Task ProfileStore<TStore, TKey, TValue>(TStore store, Func<int, TKey> keyGen, Func<int, TValue> valueGen)
            where TStore : IFasterWrapper<TKey, TValue>
        {
            static string threadingModeString(ThreadingMode threadingMode)
                => threadingMode switch
                {
                    ThreadingMode.Single => "Single threading",
                    ThreadingMode.ParallelFor => "Parallel.For issuing async operations",
                    ThreadingMode.Chunks => $"Chunks partitioned across {numChunks} tasks",
                    _ => throw new ApplicationException("Unknown threading mode")
                };

            Console.WriteLine("    Creating database");
            (TKey, TValue)[] database = new (TKey, TValue)[numOperations];
            TKey[] keys = new TKey[numOperations];
            for (int i = 0; i < numOperations; i++)
            {
                database[i] = (keyGen(i), valueGen(i));
                keys[i] = database[i].Item1;
            }
            Console.WriteLine("    Creation complete");

            Assert.True(numOperations % numChunks == 0, $"Number of operations {numOperations} should be a multiple of number of chunks {numChunks}");

            int chunkSize = numOperations / numChunks;

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
                        await store.UpsertAsync(database[i].Item1, database[i].Item2);
                }
                else if (upsertThreadingMode == ThreadingMode.ParallelFor)
                {
                    var writeTasks = new ValueTask[numOperations];
                    Parallel.For(0, numOperations, i => writeTasks[i] = store.UpsertAsync(database[i].Item1, database[i].Item2));
                    foreach (var task in writeTasks)
                        await task;
                }
                else if (upsertThreadingMode == ThreadingMode.Chunks)
                {
                    var chunkTasks = new ValueTask[numChunks];
                    for (int i = 0; i < numChunks; i++)
                        chunkTasks[i] = store.UpsertChunkAsync(database, i * chunkSize, chunkSize);
                    foreach (var chunkTask in chunkTasks)
                        await chunkTask;
                }
                else
                    throw new InvalidOperationException($"Invalid threading mode {upsertThreadingMode}");

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
                Console.WriteLine($"    Reading {numOperations} records with {threadingModeString(readThreadingMode)} (OS buffering: {store.UseOsReadBuffering}) ...");
                (Status, TValue)[] results = new (Status, TValue)[numOperations];

                var sw = Stopwatch.StartNew();
                if (readThreadingMode == ThreadingMode.Single)
                {
                    for (int i = 0; i < numOperations; i++)
                    {
                        var result = await store.ReadAsync(database[i].Item1);
                        results[i] = result;
                    }
                }
                else if (readThreadingMode == ThreadingMode.ParallelFor)
                {
                    var readTasks = new ValueTask<(Status, TValue)>[numOperations];
                    Parallel.For(0, numOperations, i => readTasks[i] = store.ReadAsync(database[i].Item1));
                    for (int i = 0; i < numOperations; i++)
                        results[i] = await readTasks[i];
                }
                else if(readThreadingMode == ThreadingMode.Chunks)
                {
                    var chunkTasks = new ValueTask<(Status, TValue)[]>[numChunks];
                    for (int i = 0; i < numChunks; i++)
                        chunkTasks[i] = store.ReadChunkAsync(keys, i * chunkSize, chunkSize);
                    for (int i = 0; i < numChunks; i++)
                    {
                        var result = await chunkTasks[i];
                        Array.Copy(result, 0, results, i * chunkSize, chunkSize);
                    }
                }
                else
                    throw new InvalidOperationException($"Invalid threading mode {readThreadingMode}");

                sw.Stop();
                Console.WriteLine($"    Reads complete in {sw.ElapsedMilliseconds} ms");

                // Verify
                Console.WriteLine("    Verifying read results ...");
                Parallel.For(0, numOperations, i =>
                {
                    Assert.Equal(Status.OK, results[i].Item1);
                    Assert.Equal(database[i].Item2, results[i].Item2);
                });

                Console.WriteLine("    Results verified");
            }

            store.Dispose();
        }
    }
}

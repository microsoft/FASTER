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

        // StorageType descriptions are listed in Usage()
        enum StorageType
        {
            Value,
            Reference,
            Span,
        }

        static ThreadingMode insertThreadingMode = ThreadingMode.ParallelFor;
        static ThreadingMode operationThreadingMode = ThreadingMode.ParallelFor;
        static StorageType storageType = StorageType.Value;
        static bool useRmw = false;
        static bool useUpsert = false;
        static int numChunks = 10;
        static int numOperations = 1_000_000;

        static void Usage()
        {
            Console.WriteLine($"Options:");
            Console.WriteLine($"    -u <mode>:      Initial pop(u)lation threading mode (listed below); default is {ThreadingMode.ParallelFor}");
            Console.WriteLine($"    -r <mode>:      Ope(r)ation threading mode (listed below); default is {ThreadingMode.ParallelFor}");
            Console.WriteLine($"    -s:             (s)torage type:");
            Console.WriteLine($"                        Value:          Use long keys and values to illustrate blittable types with no object log");
            Console.WriteLine($"                        Reference:      Use string keys and values to illustrate reference types with object log");
            Console.WriteLine($"                        Span:           Use SpanByte keys and values to illustrate store-serialized reference or value types with no object log");
            Console.WriteLine($"    -c #:           Number of (c)hunks for {ThreadingMode.Chunks}; default is {numChunks}");
            Console.WriteLine($"    -n #:           (n)umber of operations; default is {numOperations}");
            Console.WriteLine($"    -b:             Use OS (b)uffering for reads; default is false");
            Console.WriteLine($"    --rmw:          Use RMW for both initial population and operations; default is false (populate with Upsert, then do Reads)");
            Console.WriteLine($"    --upsert:       Use Upsert for both initial population and operations; default is false (populate with Upsert, then do Reads)");
            Console.WriteLine($"    --large:        Use large log memory (default is false; operations are done on readonly pages)");
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
            bool useLargeLog = false;

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
                        insertThreadingMode = Enum.Parse<ThreadingMode>(nextArg(), ignoreCase: true);
                    else if (arg == "-r")
                        operationThreadingMode = Enum.Parse<ThreadingMode>(nextArg(), ignoreCase: true);
                    else if (arg == "-s")
                        storageType = Enum.Parse<StorageType>(nextArg(), ignoreCase: true);
                    else if (arg == "-c")
                        numChunks = int.Parse(nextArg());
                    else if (arg == "-n")
                        numOperations = int.Parse(nextArg());
                    else if (arg == "-b")
                        useOsReadBuffering = true;
                    else if (arg == "--rmw")
                        useRmw = true;
                    else if (arg == "--upsert")
                        useUpsert = true;
                    else if (arg == "--large")
                        useLargeLog = true;
                    else if (arg == "-?" || arg == "/?" || arg == "--help")
                    {
                        Usage();
                        return;
                    }
                    else
                        throw new ApplicationException($"Unknown switch: {arg}");
                }
                if (useRmw && useUpsert)
                    throw new ApplicationException("Cannot specify both RMW-only and Upsert-only");
            }

            await (storageType switch
            {
                StorageType.Value => ProfileStore(new FasterWrapper<long, long>(isRefType: false, useLargeLog, useOsReadBuffering), e => (long)e, e => (long)e),
                StorageType.Reference => ProfileStore(new FasterWrapper<string, string>(isRefType: true, useLargeLog, useOsReadBuffering), e => $"key {e}", e => $"value {e}"),
                StorageType.Span => ProfileStore(new SerializedFasterWrapper<string, string>(useLargeLog, useOsReadBuffering), e => $"key {e}", e => $"value {e}"),
                _ => throw new ApplicationException($"Unknown storageType {storageType}")
            });
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
                    _ => throw new ApplicationException($"Unknown threading mode {threadingMode}")
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

            Assert.True(numOperations % numChunks == 0, $"Number of operations {numOperations:N0} should be a multiple of number of chunks {numChunks}");

            int chunkSize = numOperations / numChunks;

            async ValueTask doUpdates()
            {
                if (insertThreadingMode == ThreadingMode.Single)
                {
                    if (useRmw)
                    {
                        for (int i = 0; i < numOperations; i++)
                            await store.RMWAsync(database[i].Item1, database[i].Item2);
                    }
                    else
                    {
                        for (int i = 0; i < numOperations; i++)
                            await store.UpsertAsync(database[i].Item1, database[i].Item2);
                    }
                }
                else if (insertThreadingMode == ThreadingMode.ParallelFor)
                {
                    var writeTasks = new ValueTask[numOperations];
                    if (useRmw)
                        Parallel.For(0, numOperations, i => writeTasks[i] = store.RMWAsync(database[i].Item1, database[i].Item2));
                    else
                        Parallel.For(0, numOperations, i => writeTasks[i] = store.UpsertAsync(database[i].Item1, database[i].Item2));
                    foreach (var task in writeTasks)
                        await task;
                }
                else if (insertThreadingMode == ThreadingMode.Chunks)
                {
                    var chunkTasks = new ValueTask[numChunks];
                    if (useRmw)
                    {
                        for (int i = 0; i < numChunks; i++)
                            chunkTasks[i] = store.RMWChunkAsync(database, i * chunkSize, chunkSize);
                    }
                    else
                    {
                        for (int i = 0; i < numChunks; i++)
                            chunkTasks[i] = store.UpsertChunkAsync(database, i * chunkSize, chunkSize);
                    }
                    foreach (var chunkTask in chunkTasks)
                        await chunkTask;
                }
                else
                    throw new InvalidOperationException($"Invalid threading mode {insertThreadingMode}");
            }

            // Insert
            if (insertThreadingMode == ThreadingMode.None)
            {
                throw new ApplicationException("Cannot Skip initial population");
            }
            else
            {
                Console.WriteLine($"    Inserting {numOperations:N0} records via {(useRmw ? "RMW" : "Upsert")} with {threadingModeString(insertThreadingMode)} ...");

                var sw = Stopwatch.StartNew();
                await doUpdates();
                sw.Stop();
                Console.WriteLine($"    Insertion complete in {sw.ElapsedMilliseconds / 1000.0:N3} sec; TailAddress = {store.TailAddress}, Pending = {store.PendingCount:N0}");
            }

            store.ClearPendingCount();

            // Read
            Console.WriteLine();
            if (operationThreadingMode == ThreadingMode.None)
            {
                Console.WriteLine("    Skipping Operations");
            }
            else
            {
                var opString = (useRmw, useUpsert) switch
                {
                    (true, _) => "RMW",
                    (_, true) => "Upsert",
                    _ => "Read"
                };
                Console.WriteLine($"    Performing {numOperations:N0} {opString}s with {threadingModeString(operationThreadingMode)} (OS buffering: {store.UseOsReadBuffering}) ...");
                (Status, TValue)[] results = new (Status, TValue)[numOperations];

                var sw = Stopwatch.StartNew();

                if (useRmw || useUpsert)
                {
                    // Just update with the same values.
                    await doUpdates();

                    sw.Stop();
                    Console.WriteLine($"    Operations complete in {sw.ElapsedMilliseconds / 1000.0:N3} sec; TailAddress = {store.TailAddress}, Pending = {store.PendingCount:N0}");
                    Console.WriteLine("    Operation was not Read so skipping verification");
                }
                else
                {
                    if (operationThreadingMode == ThreadingMode.Single)
                    {
                        for (int i = 0; i < numOperations; i++)
                            results[i] = await store.ReadAsync(database[i].Item1);
                    }
                    else if (operationThreadingMode == ThreadingMode.ParallelFor)
                    {
                        var readTasks = new ValueTask<(Status, TValue)>[numOperations];
                        Parallel.For(0, numOperations, i => readTasks[i] = store.ReadAsync(database[i].Item1));
                        for (int i = 0; i < numOperations; i++)
                            results[i] = await readTasks[i];
                    }
                    else if (operationThreadingMode == ThreadingMode.Chunks)
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
                        throw new InvalidOperationException($"Invalid threading mode {operationThreadingMode}");

                    sw.Stop();
                    Console.WriteLine($"    {opString}s complete in {sw.ElapsedMilliseconds / 1000.0:N3} sec");

                    Console.WriteLine("    Verifying read results ...");
                    Parallel.For(0, numOperations, i =>
                    {
                        Assert.Equal(Status.OK, results[i].Item1);
                        Assert.Equal(database[i].Item2, results[i].Item2);
                    });
                    Console.WriteLine("    Results verified");
                }
            }

            store.Dispose();
        }
    }
}

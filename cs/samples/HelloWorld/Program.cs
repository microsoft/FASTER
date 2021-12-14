// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;

namespace HelloWorld
{
    /// <summary>
    /// This is a basic sample of FasterKV using value types
    /// </summary>
    public class Program
    {
        static void Main()
        {
            InMemorySample();
            DiskSample();
            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        static void InMemorySample()
        {
            Console.WriteLine("In-Memory Sample:\n");

            long key = 1, value = 1, output = 0;

            // Create a default config (null path indicates in-memory only)
            // Uses default config parameters. Update config fields to tune parameters.
            using var config = new FasterKVConfig<long, long>(null);
            Console.WriteLine($"FasterKV config:\n{config}\n");

            using var store = new FasterKV<long, long>(config);

            // Create functions for callbacks; we use a standard in-built function in this sample.
            // You can write your own by extending this or FunctionsBase.
            // In this in-built function, read-modify-writes will perform value merges via summation.
            var funcs = new SimpleFunctions<long, long>((a, b) => a + b);

            // Each logical sequence of calls to FASTER is associated with a FASTER session.
            // No concurrency allowed within a single session
            using var session = store.NewSession(funcs);

            // (1) Upsert and read back upserted value
            session.Upsert(ref key, ref value);

            // Reads are served back from memory and return synchronously
            var status = session.Read(ref key, ref output);
            if (status == Status.OK && output == value)
                Console.WriteLine("(1) Success!");
            else
                Console.WriteLine("(1) Error!");

            /// (2) Delete key, read to verify deletion
            session.Delete(ref key);

            status = session.Read(ref key, ref output);
            if (status == Status.NOTFOUND)
                Console.WriteLine("(2) Success!");
            else
                Console.WriteLine("(2) Error!");

            // (4) Perform two read-modify-writes (summation), verify result
            key = 2;
            long input1 = 25, input2 = 27;

            session.RMW(ref key, ref input1);
            session.RMW(ref key, ref input2);

            status = session.Read(ref key, ref output);

            if (status == Status.OK && output == input1 + input2)
                Console.WriteLine("(3) Success!");
            else
                Console.WriteLine("(3) Error!");


            // (5) Perform TryAdd using RMW and custom IFunctions
            using var tryAddSession = store.NewSession(new TryAddFunctions<long, long>());
            key = 3; input1 = 30; input2 = 31;

            // First TryAdd - success; status should be NOTFOUND (does not already exist)
            status = tryAddSession.RMW(ref key, ref input1);

            // Second TryAdd - failure; status should be OK (already exists)
            var status2 = tryAddSession.RMW(ref key, ref input2);

            // Read, result should be input1 (first TryAdd)
            var status3 = session.Read(ref key, ref output);

            if (status == Status.NOTFOUND && status2 == Status.OK && status3 == Status.OK && output == input1)
                Console.WriteLine("(4) Success!");
            else
                Console.WriteLine("(4) Error!");
        }

        static void DiskSample()
        {
            Console.WriteLine("\nDisk Sample:\n");

            long key = 1, value = 1, output = 0;

            // Create FasterKV config based on specified base directory path.
            using var config = new FasterKVConfig<long, long>("./database") { TryRecoverLatest = true };
            Console.WriteLine($"FasterKV config:\n{config}\n");

            // Create store using specified config
            using var store = new FasterKV<long, long>(config);

            // Create functions for callbacks; we use a standard in-built function in this sample.
            // You can write your own by extending this or FunctionsBase.
            // In this in-built function, read-modify-writes will perform value merges via summation.
            var funcs = new SimpleFunctions<long, long>((a, b) => a + b);

            // Each logical sequence of calls to FASTER is associated with a FASTER session.
            // No concurrency allowed within a single session
            using var session = store.NewSession(funcs);

            if (store.RecoveredVersion == 1) // did not recover
            {
                Console.WriteLine("Clean start; upserting key-value pair");

                // (1) Upsert and read back upserted value
                session.Upsert(ref key, ref value);

                // Take checkpoint so data is persisted for recovery
                Console.WriteLine("Taking full checkpoint");
                store.TakeFullCheckpoint(out _, CheckpointType.Snapshot);
                store.CompleteCheckpointAsync().GetAwaiter().GetResult();
            }
            else
            {
                Console.WriteLine($"Recovered store to version {store.RecoveredVersion}");
            }

            // Reads are served back from memory and return synchronously
            var status = session.Read(ref key, ref output);
            if (status == Status.OK && output == value)
                Console.WriteLine("(1) Success!");
            else
                Console.WriteLine("(1) Error!");

            // (2) Force flush record to disk and evict from memory, so that next read is served from disk
            store.Log.FlushAndEvict(true);

            // Reads from disk will return PENDING status, result available via either asynchronous IFunctions callback
            // or on this thread via CompletePendingWithOutputs, shown below
            status = session.Read(ref key, ref output);
            if (status == Status.PENDING)
            {
                session.CompletePendingWithOutputs(out var iter, true);
                while (iter.Next())
                {
                    if (iter.Current.Status == Status.OK && iter.Current.Output == value)
                        Console.WriteLine("(2) Success!");
                    else
                        Console.WriteLine("(2) Error!");
                }
                iter.Dispose();
            }
            else
                Console.WriteLine("(2) Error!");

            /// (3) Delete key, read to verify deletion
            session.Delete(ref key);

            status = session.Read(ref key, ref output);
            if (status == Status.NOTFOUND)
                Console.WriteLine("(3) Success!");
            else
                Console.WriteLine("(3) Error!");

            // (4) Perform two read-modify-writes (summation), verify result
            key = 2;
            long input1 = 25, input2 = 27;

            session.RMW(ref key, ref input1);
            session.RMW(ref key, ref input2);

            status = session.Read(ref key, ref output);

            if (status == Status.OK && output == input1 + input2)
                Console.WriteLine("(4) Success!");
            else
                Console.WriteLine("(4) Error!");


            // (5) Perform TryAdd using RMW and custom IFunctions
            using var tryAddSession = store.NewSession(new TryAddFunctions<long, long>());
            key = 3; input1 = 30; input2 = 31;

            // First TryAdd - success; status should be NOTFOUND (does not already exist)
            status = tryAddSession.RMW(ref key, ref input1);

            // Second TryAdd - failure; status should be OK (already exists)
            var status2 = tryAddSession.RMW(ref key, ref input2);

            // Read, result should be input1 (first TryAdd)
            var status3 = session.Read(ref key, ref output);

            if (status == Status.NOTFOUND && status2 == Status.OK && status3 == Status.OK && output == input1)
                Console.WriteLine("(5) Success!");
            else
                Console.WriteLine("(5) Error!");
        }
    }
}

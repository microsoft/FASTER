// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FASTER.client;
using FASTER.common;

namespace FixedLenClient
{
    /// <summary>
    /// Client to interact with FASTER server for fixed-length keys and values
    /// (FixedLenServer). Uses 8-byte keys and 8-byte values.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            string ip = "127.0.0.1";
            int port = 3278;

            if (args.Length > 0 && args[0] != "-")
                ip = args[0];
            if (args.Length > 1 && args[1] != "-")
                port = int.Parse(args[1]);

            using var client = new FasterKVClient<long, long>(ip, port);

            // Create a session to FasterKV server
            // Sessions are mono-threaded, similar to normal FasterKV sessions
            using var session = client.NewSession(new Functions());

            // Explicit version of NewSession call, where you provide all types, callback functions, and serializer
            // using var session = client.NewSession<long, long, long, Functions, BlittableParameterSerializer<long, long, long, long>>(new Functions(), new BlittableParameterSerializer<long, long, long, long>());

            // Samples using sync client API
            SyncSamples(session);

            // Samples using async client API
            AsyncSamples(session).Wait();

            Console.WriteLine("Success!");
        }

        static void SyncSamples(ClientSession<long, long, long, long, byte, Functions, BlittableParameterSerializer<long, long, long, long>> session)
        {
            for (int i = 0; i < 1000; i++)
                session.Upsert(i, i + 10000);

            // Flushes partially filled batches, does not wait for responses
            session.Flush();

            // Read key 23, result arrives via ReadCompletionCallback
            session.Read(23);
            session.CompletePending(true);

            // Measure read latency
            double micro = 0;
            for (int i = 0; i < 1000; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                session.Read(23);

                // CompletePending flushes and waits for responses
                // Responses are received on the callback function - see Functions.cs
                session.CompletePending(true);
                sw.Stop();
                if (i > 0)
                    micro += 1000000 * sw.ElapsedTicks / (double)Stopwatch.Frequency;
            }
            Console.WriteLine("Average latency for sync Read: {0} microsecs", micro / (1000-1));

            session.RMW(23, 25);
            session.RMW(23, 25);
            session.CompletePending(true);

            // We use a different context here, to verify the different read result in callback function - see Functions.cs
            session.Read(23, userContext: 1);
            session.CompletePending(true);

            for (int i = 100; i < 200; i++)
                session.Upsert(i, i + 10000);

            session.Flush();

            session.CompletePending(true);
        }

        static async Task AsyncSamples(ClientSession<long, long, long, long, byte, Functions, BlittableParameterSerializer<long, long, long, long>> session)
        {
            for (int i = 0; i < 1000; i++)
                session.Upsert(i, i + 10000);

            // By default, we flush async operations as soon as they are issued
            // To instead flush manually, set forceFlush = false in calls below
            var (status, output) = await session.ReadAsync(23);
            if (status != Status.OK || output != 23 + 10000)
                throw new Exception("Error!");

            // Measure read latency
            double micro = 0;
            for (int i = 0; i < 1000; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                _ = await session.ReadAsync(23);
                sw.Stop();
                if (i > 0)
                    micro += 1000000 * sw.ElapsedTicks / (double)Stopwatch.Frequency;
            }
            Console.WriteLine("Average latency for async Read: {0} microsecs", micro / (1000 - 1));

            await session.DeleteAsync(25);

            (status, _) = await session.ReadAsync(25);
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");

            (status, _) = await session.ReadAsync(9999);
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");

            await session.DeleteAsync(9998);

            status = await session.RMWAsync(9998, 10);
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");

            (status, output) = await session.ReadAsync(9998);
            if (status != Status.OK || output != 10)
                throw new Exception("Error!");

            status = await session.RMWAsync(9998, 10);
            if (status != Status.OK)
                throw new Exception("Error!");

            (status, output) = await session.ReadAsync(9998);
            if (status != Status.OK || output != 20)
                throw new Exception("Error!");
        }
    }
}

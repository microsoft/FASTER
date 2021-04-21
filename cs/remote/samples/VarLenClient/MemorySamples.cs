// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Threading.Tasks;
using FASTER.client;

namespace VarLenClient
{
    /// <summary>
    /// We demonstrate a client that talks to a VarLen server using a predefined type (VarLenType) 
    /// that conforms to the expected data format of [length][data]. The size of the type is
    /// pre-defined to be 12 bytes in VarLenType.
    /// </summary>
    class MemorySamples
    {
        public void Run(string ip, int port)
        {
            using var client1 = new FasterKVClient<ReadOnlyMemory<int>, ReadOnlyMemory<int>>(ip, port);
            var session = client1.NewSession(new MemoryFunctions());

            SyncMemorySamples(session);
            AsyncMemorySamples(session).Wait();
        }

        private void SyncMemorySamples(ClientSession<ReadOnlyMemory<int>, ReadOnlyMemory<int>, ReadOnlyMemory<int>, (IMemoryOwner<int>, int), byte, MemoryFunctions, MemoryParameterSerializer<int>> session)
        {
            var key = new Memory<int>(new int[100]);
            var value = new Memory<int>(new int[100]);

            for (int i = 0; i < 100; i++)
            {
                key.Span.Fill(i);
                value.Span.Fill(i + 10000);
                session.Upsert(key, value);
            }

            // Flushes partially filled batches, does not wait for responses
            session.Flush();

            key.Span.Fill(23);
            value.Span.Fill(0);
            session.Read(key);
            session.CompletePending(true);

            for (int i = 100; i < 200; i++)
            {
                key.Span.Fill(i);
                value.Span.Fill(i + 10000);
                session.Upsert(key, value);
            }

            session.Flush();

            session.CompletePending(true);
        }
        
        async Task AsyncMemorySamples(ClientSession<ReadOnlyMemory<int>, ReadOnlyMemory<int>, ReadOnlyMemory<int>, (IMemoryOwner<int>, int), byte, MemoryFunctions, MemoryParameterSerializer<int>> session)
        {
            // By default, we flush async operations as soon as they are issued
            // To instead flush manually, set forceFlush = false in calls below
            var key = new Memory<int>(new int[100]);
            var value = new Memory<int>(new int[100]);

            key.Span.Fill(25);
            value.Span.Fill(25 + 10000);

            session.Upsert(key, value);

            var (status, output) = await session.ReadAsync(key);
            if (status != Status.OK || !output.Item1.Memory.Span.Slice(0, output.Item2).SequenceEqual(value.Span))
                throw new Exception("Error!");

            await session.DeleteAsync(key);

            (status, _) = await session.ReadAsync(key);
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");

            key.Span.Fill(9999);

            (status, _) = await session.ReadAsync(key);
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");
        }
    }
}

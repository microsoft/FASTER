// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Text;
using FASTER.client;

namespace VarLenClient
{
    /// <summary>
    /// Variable length byte arrays as key value pairs
    /// </summary>
    class MemoryBenchmark
    {
        public void Run(string ip, int port)
        {
            using var client1 = new FasterKVClient<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>(ip, port);
            var session = client1.NewSession(new MemoryFunctionsByte()); // uses protocol WireFormat.DefaultVarLenKV by default

            SyncMemoryBenchmark(session);
        }

        private void SyncMemoryBenchmark(ClientSession<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, ReadOnlyMemory<byte>, (IMemoryOwner<byte>, int), byte, MemoryFunctionsByte, MemoryParameterSerializer<byte>> session)
        {
            const int BatchSize = 10000;
            const int NumBatches = 1000;

            var getBatch = new ReadOnlyMemory<byte>[BatchSize];
            for (int k = 0; k < BatchSize; k++)
            {
                getBatch[k] = new ReadOnlyMemory<byte>(Encoding.ASCII.GetBytes(k.ToString()));
            }

            for (int k = 0; k < BatchSize; k++)
            {
                session.Upsert(ref getBatch[k], ref getBatch[k]);
            }
            session.CompletePending(true);

            var input = new ReadOnlyMemory<byte>(new byte[0]);
            Stopwatch sw = new Stopwatch();
            (IMemoryOwner<byte>, int) output = default;

            sw.Start();
            for (int b = 0; b < NumBatches; b++)
            {
                for (int c=0; c<BatchSize; c++)
                    session.Read(ref getBatch[c], ref input, ref output);
                session.Flush();
            }
            session.CompletePending(true);
            sw.Stop();

            Console.WriteLine("Total time: {0}ms for {1} gets", sw.ElapsedMilliseconds, NumBatches * BatchSize);
        }
    }
}

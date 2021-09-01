// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using FASTER.client;

namespace VarLenClient
{
    /// <summary>
    /// Variable length byte arrays as key value pairs
    /// </summary>
     class SpanByteBenchmark
    {
        public void Run(string ip, int port)
        {
            using var client1 = new FasterKVClient<SpanByte, SpanByte>(ip, port);
            var session = client1.NewSession(new SpanByteFunctionsClient()); // uses protocol WireFormat.DefaultVarLenKV by default

            SyncMemoryBenchmark(session);
        }
        

        private unsafe void SyncMemoryBenchmark(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, SpanByteFunctionsClient, SpanByteClientSerializer> session)
        {
            const int BatchSize = 10000;
            const int NumBatches = 1000;

            var getBatch = new SpanByte[BatchSize];
            for (int k = 0; k < BatchSize; k++)
            {
                Span<byte> bytes = Encoding.ASCII.GetBytes(k.ToString());
                fixed(byte* b = bytes)
                {
                    getBatch[k] = SpanByte.FromPointer(b, bytes.Length);
                }
            }

            for (int k = 0; k < BatchSize; k++)
            {
                session.Upsert(ref getBatch[k], ref getBatch[k]);
            }
            session.CompletePending(true);

            SpanByte input = new SpanByte();
            Stopwatch sw = new Stopwatch();
            SpanByteAndMemory output = new SpanByteAndMemory();

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
     public class SpanByteFunctionsClient : SpanByteFunctionsBase
    {
        private int count = 0;
        public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status)
        {
            try
            {
                if (ctx == 0 && key.AsSpan().SequenceEqual(output.SpanByte.AsSpan()))
                {
                    if (status != Status.OK)
                        Console.Write(++count);
                }
                else
                {
                    throw new Exception("Unexpected user context");
                }
            }
            finally
            {
                output.Memory.Dispose();
            }
        }
    }
}

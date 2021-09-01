using FASTER.client;
using FASTER.server;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.remote.test
{
    class SpanByteClientTests
    {
        VarLenServer server;
        SpanByteClient client;

        [SetUp]
        public void Setup()
        {
            server = TestUtils.CreateVarLenServer(TestContext.CurrentContext.TestDirectory + "/SpanByteTests");
            server.Start();
            client = new SpanByteClient();
        }

        [TearDown]
        public void TearDown()
        {
            client.Dispose();
            server.Dispose();
        }

        [Test]
        public unsafe void UpsertReadTest()
        {
            const int BatchSize = 100;
            const int NumBatches = 1;
            
            using var session = client.GetSession();

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
            SpanByteAndMemory output = new SpanByteAndMemory();

            for (int b = 0; b < NumBatches; b++)
            {
                for (int c=0; c < BatchSize; c++)
                    session.Read(ref getBatch[c], ref input, ref output);
                session.Flush();
            }
            session.CompletePending(true);
        }
    }
}

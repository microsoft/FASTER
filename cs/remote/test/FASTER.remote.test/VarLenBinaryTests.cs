using System;
using System.Collections.Generic;
using FASTER.server;
using NUnit.Framework;

namespace FASTER.remote.test
{
    [TestFixture]
    public class VarLenBinaryTests
    {
        VarLenServer server;
        VarLenMemoryClient client;

        [SetUp]
        public void Setup()
        {
            server = TestUtils.CreateVarLenServer(TestContext.CurrentContext.TestDirectory + "/VarLenBinaryTests", disablePubSub: true);
            server.Start();
            client = new VarLenMemoryClient();
        }

        [TearDown]
        public void TearDown()
        {
            client.Dispose();
            server.Dispose();
        }

        [Test]
        public void UpsertReadTest()
        {
            Random r = new Random(23);
            using var session = client.GetSession();
            var key = new Memory<int>(new int[2 + r.Next(50)]);
            var value = new Memory<int>(new int[1 + r.Next(50)]);
            key.Span[0] = r.Next(100);
            key.Span[1] = value.Length;
            value.Span.Fill(key.Span[0]);

            session.Upsert(key, value);
            session.CompletePending(true);
            session.Read(key, userContext: key.Span[0]);
            session.CompletePending(true);
        }
        
        [Test]
        public void UpsertReadTestHeavy()
        {
            Random r = new Random(23);
            using var session = client.GetSession();
            List<(Memory<int>, Memory<int>)> kv = new();
            for (var i = 0; i < 10000; i++)
            {
                var key = new Memory<int>(new int[2 + r.Next(10)]);
                var value = new Memory<int>(new int[1 + r.Next(10)]);
                key.Span[0] = r.Next(100);
                key.Span[1] = value.Length;
                value.Span.Fill(key.Span[0]);
                kv.Add((key, value));
            }

            foreach (var (key, value) in kv)
            {
                session.Upsert(key, value);
                session.CompletePending(false);
            }
            session.CompletePending(true);

            foreach (var (key, value) in kv)
            {
                session.Read(key, userContext: key.Span[0]);
                session.CompletePending(true);
            }
        }

    }
}

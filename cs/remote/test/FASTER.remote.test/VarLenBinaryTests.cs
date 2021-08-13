using System;
using NUnit.Framework;

namespace FASTER.remote.test
{
    [TestFixture]
    public class VarLenBinaryTests
    {
        VarLenServer  server;
        VarLenMemoryClient client;

        [SetUp]
        public void Setup()
        {
            server = new VarLenServer(TestContext.CurrentContext.TestDirectory + "/VarLenBinaryTests");
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
    }
}

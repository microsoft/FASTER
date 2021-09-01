using System;
using System.Text;
using FASTER.client;
using NUnit.Framework;
using FASTER.server;

namespace FASTER.remote.test
{
    using SpanByteClientSerializer = FASTER.client.SpanByteClientSerializer;
    [TestFixture]
    public class SpanBytePubSubTests
    {
        VarLenServer server;
        SpanByteClient client;
        SpanByte key;
        SpanByte upsertKey;
        SpanByte value;

        [SetUp]
        public void Setup()
        {
            server = TestUtils.CreateVarLenServer(TestContext.CurrentContext.TestDirectory + "/SpanBytePubSubTests", enablePubSub: true);
            server.Start();
            client = new SpanByteClient();
            
            Random r = new Random(23);

            
            var keyBytes = Encoding.ASCII.GetBytes(r.Next(100).ToString());
            var valueBytes = Encoding.ASCII.GetBytes(r.Next(100).ToString());
            unsafe
            {
                fixed (byte* bytes = keyBytes)
                {
                    key = SpanByte.FromPointer(bytes, key.Length);
                    upsertKey = SpanByte.FromPointer(bytes, key.Length);
                }
                fixed (byte* bytes = valueBytes)
                {
                    value = SpanByte.FromPointer(bytes, key.Length);
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            client.Dispose();
            server.Dispose();
        }

        [Test]
        public void SubscribeKVTest()
        {
            var functions = new SpanByteClientFunctions();
            using var session = client.GetSession(functions);
            using var subSession = client.GetSession(functions);
            subSession.SubscribeKV(key);
            subSession.CompletePending(true);
            session.Upsert(key, value);
            session.CompletePending(true);

            functions.WaitSubscribe();
        }

        [Test]
        public void PSubscribeKVTest()
        {
            var functions = new SpanByteClientFunctions();
            using var session = client.GetSession(functions);
            using var subSession = client.GetSession(functions);

            subSession.PSubscribeKV(key);
            subSession.CompletePending(true);
            session.Upsert(upsertKey, value);
            session.CompletePending(true);

            functions.WaitSubscribe();
        }

        [Test]
        public void SubscribeTest()
        {
            var functions = new SpanByteClientFunctions();
            using var session = client.GetSession(functions);
            using var subSession = client.GetSession(functions);
            subSession.Subscribe(key);
            subSession.CompletePending(true);
            session.Publish(key, value);
            session.CompletePending(true);

            functions.WaitSubscribe();
        }

        [Test]
        public void PSubscribeTest()
        {
            var functions = new SpanByteClientFunctions();
            using var session = client.GetSession(functions);
            using var subSession = client.GetSession(functions);

            subSession.PSubscribe(key);
            subSession.CompletePending(true);
            session.Publish(upsertKey, value);
            session.CompletePending(true);

            functions.WaitSubscribe();
        }

    }
}
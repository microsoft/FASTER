using NUnit.Framework;

namespace FASTER.remote.test
{
    [TestFixture]
    public class FixedLenBinaryPubSubTests
    {
        FixedLenServer<long, long> server;
        FixedLenClient<long, long> client;

        [SetUp]
        public void Setup()
        {
            server = new FixedLenServer<long, long>(TestContext.CurrentContext.TestDirectory + "/FixedLenBinaryTests", (a, b) => a + b, useBroker: true);
            client = new FixedLenClient<long, long>();
        }

        [TearDown]
        public void TearDown()
        {
            client.Dispose();
            server.Dispose();
        }

        [Test]
        public void SubscribeTest()
        {
            var session = client.GetSession();
            var subSession = client.GetSession();

            subSession.SubscribeKV(10);
            subSession.CompletePending(true);
            session.Upsert(10, 23);
            session.CompletePending(true);

            System.Threading.Thread.Sleep(1000);
        }

        [Test]
        public void PrefixSubscribeTest()
        {
            var session = client.GetSession();
            var subSession = client.GetSession();

            subSession.PSubscribeKV(10);
            subSession.CompletePending(true);
            session.Upsert(10, 23);
            session.CompletePending(true);

            System.Threading.Thread.Sleep(1000);
        }
    }
}

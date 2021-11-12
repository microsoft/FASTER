// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    class ManualOperationsTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;
        const int numThreads = 12;

        private FasterKV<int, int> fkv;
        private ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(Path.Combine(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 });
            session = fkv.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fkv?.Dispose();
            fkv = null;
            log?.Dispose();
            log = null;

            // Clean up log 
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
            {
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
            }
        }

        [Test]
        [Category(TestUtils.ManualOpsTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemoryLockTest()
        {
            Populate();
        }
    }
}

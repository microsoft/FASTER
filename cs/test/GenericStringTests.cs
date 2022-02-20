// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class GenericStringTests
    {
        private FasterKV<string, string> fht;
        private ClientSession<string, string, string, string, Empty, MyFuncs> session;
        private IDevice log, objlog;
        private string path;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            TestUtils.DeleteDirectory(path);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void StringBasicTest([Values] TestUtils.DeviceType deviceType)
        {
            string logfilename = path + "GenericStringTests" + deviceType.ToString() + ".log";
            string objlogfilename = path + "GenericStringTests" + deviceType.ToString() + ".obj.log";

            log = TestUtils.CreateTestDevice(deviceType, logfilename);
            objlog = TestUtils.CreateTestDevice(deviceType, objlogfilename);

            fht = new FasterKV<string, string>(
                    1L << 20, // size of hash table in #cache lines; 64 bytes per cache line
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9, SegmentSizeBits = 22 } // log device
                    );

            session = fht.For(new MyFuncs()).NewSession<MyFuncs>();

            const int totalRecords = 200;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = $"{i}";
                var _value = $"{i}"; ;
                session.Upsert(ref _key, ref _value, Empty.Default, 0);
            }
            session.CompletePending(true);
            Assert.AreEqual(totalRecords, fht.EntryCount);

            for (int i = 0; i < totalRecords; i++)
            {
                string input = default;
                string output = default;
                var key = $"{i}";
                var value = $"{i}";

                var status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
                if (status.Pending)
                {
                    session.CompletePendingWithOutputs(out var outputs, wait:true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value, output);
            }
        }

        class MyFuncs : SimpleFunctions<string, string>
        {
            public override void ReadCompletionCallback(ref string key, ref string input, ref string output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key, output);
            }
        }
    }
}

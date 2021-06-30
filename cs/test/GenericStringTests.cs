// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

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
            //** #142980 - Blob not exist exception in Dispose so use Try \ Catch to make sure tests run without issues 
            try
            {

                session?.Dispose();
                session = null;
                fht?.Dispose();
                fht = null;
                log?.Dispose();
                log = null;
                objlog?.Dispose();
                objlog = null;
            }
            catch { }

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

            //*#*#*# TO DO: Figure Out why this DeviceType fails - For FASTER Log, need to add 'LogCommitDir = path' to log settings   *#*#*#
            if (deviceType == TestUtils.DeviceType.LocalMemory)
            {
                return;
            }

            fht = new FasterKV<string, string>(
                    1L << 20, // size of hash table in #cache lines; 64 bytes per cache line
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9, SegmentSizeBits = 22 } // log device
                    );

            session = fht.For(new MyFuncs()).NewSession<MyFuncs>();


            const int totalRecords = 500;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = $"{i}";
                var _value = $"{i}"; ;
                session.Upsert(ref _key, ref _value, Empty.Default, 0);
            }
            session.CompletePending(true);
            Assert.IsTrue(fht.EntryCount == totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                string input = default;
                string output = default;
                var key = $"{i}";
                var value = $"{i}";

                if (session.Read(ref key, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    session.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(output == value,$"Output failure. Output:{output} and value: {value}");
                }
            }
        }

        class MyFuncs : SimpleFunctions<string, string>
        {
            public override void ReadCompletionCallback(ref string key, ref string input, ref string output, Empty ctx, Status status)
            {
                Assert.IsTrue(output == key, $"Output failure in call back. Output:{output} and key: {key}");
            }
        }
    }
}

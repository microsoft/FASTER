// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;

namespace FASTER.test
{

    [TestFixture]
    internal class GenericStringTests
    {
        private FasterKV<string, string> fht;
        private ClientSession<string, string, string, string, Empty, MyFuncs> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/GenericStringTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/GenericStringTests.obj.log", deleteOnClose: true);

            fht
                = new FasterKV<string, string>(
                    1L << 20, // size of hash table in #cache lines; 64 bytes per cache line
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 } // log device
                    );

            session = fht.For(new MyFuncs()).NewSession<MyFuncs>();
        }

        [TearDown]
        public void TearDown()
        {
            session.Dispose();
            fht.Dispose();
            fht = null;
            log.Dispose();
            objlog.Dispose();
        }


        [Test]
        [Category("FasterKV")]
        public void StringBasicTest()
        {
            const int totalRecords = 2000;
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
                    Assert.IsTrue(output == value);
                }
            }
        }

        class MyFuncs : SimpleFunctions<string, string>
        {
            public override void ReadCompletionCallback(ref string key, ref string input, ref string output, Empty ctx, Status status)
            {
                Assert.IsTrue(output == key);
            }
        }
    }
}

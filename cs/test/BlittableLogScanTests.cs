// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class BlittableFASTERScanTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;
        const int totalRecords = 2000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BlittableFASTERScanTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public void BlittableDiskWriteScan()
        {
            using var session = fht.For(new Functions()).NewSession<Functions>();

            var s = fht.Log.Subscribe(new LogObserver());

            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            fht.Log.FlushAndEvict(true);

            var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.SinglePageBuffering);

            int val = 0;
            while (iter.GetNext(out _, out KeyStruct key, out ValueStruct value))
            {
                Assert.IsTrue(key.kfield1 == val);
                Assert.IsTrue(key.kfield2 == val + 1);
                Assert.IsTrue(value.vfield1 == val);
                Assert.IsTrue(value.vfield2 == val + 1);
                val++;
            }
            Assert.IsTrue(totalRecords == val);

            iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.DoublePageBuffering);

            val = 0;
            while (iter.GetNext(out RecordInfo recordInfo, out KeyStruct key, out ValueStruct value))
            {
                Assert.IsTrue(key.kfield1 == val);
                Assert.IsTrue(key.kfield2 == val + 1);
                Assert.IsTrue(value.vfield1 == val);
                Assert.IsTrue(value.vfield2 == val + 1);
                val++;
            }
            Assert.IsTrue(totalRecords == val);

            s.Dispose();
        }

        class LogObserver : IObserver<IFasterScanIterator<KeyStruct, ValueStruct>>
        {
            int val = 0;

            public void OnCompleted()
            {
                Assert.IsTrue(val == totalRecords);
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(IFasterScanIterator<KeyStruct, ValueStruct> iter)
            {
                while (iter.GetNext(out _, out KeyStruct key, out ValueStruct value))
                {
                    Assert.IsTrue(key.kfield1 == val);
                    Assert.IsTrue(key.kfield2 == val + 1);
                    Assert.IsTrue(value.vfield1 == val);
                    Assert.IsTrue(value.vfield2 == val + 1);
                    val++;
                }
            }
        }
    }
}

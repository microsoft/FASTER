// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class GenericFASTERScanTests
    {
        private FasterKV<MyKey, MyValue> fht;
        private IDevice log, objlog;
        const int totalRecords = 250;
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
        public void DiskWriteScanBasicTest([Values] TestUtils.DeviceType deviceType)
        {
            log = TestUtils.CreateTestDevice(deviceType, $"{path}DiskWriteScanBasicTest_{deviceType}.log");
            objlog = TestUtils.CreateTestDevice(deviceType, $"{path}DiskWriteScanBasicTest_{deviceType}.obj.log");
            fht = new (128,
                      logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 },
                      serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                      );

            using var session = fht.For(new MyFunctions()).NewSession<MyFunctions>();
            using var s = fht.Log.Subscribe(new LogObserver());

            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                session.Upsert(ref _key, ref _value, Empty.Default, 0);
                if (i % 100 == 0)
                    fht.Log.FlushAndEvict(true);
            }
            fht.Log.FlushAndEvict(true);

            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.SinglePageBuffering))
            {
                int val;
                for (val = 0; iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value); ++val)
                {
                    Assert.AreEqual(val, key.key, $"log scan 1: key");
                    Assert.AreEqual(val, value.value, $"log scan 1: value");
                }
                Assert.AreEqual(val, totalRecords, $"log scan 1: totalRecords");
            }

            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.DoublePageBuffering))
            {
                int val;
                for (val = 0; iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value); ++val)
                {
                    Assert.AreEqual(val, key.key, $"log scan 2: key");
                    Assert.AreEqual(val, value.value, $"log scan 2: value");
                }
                Assert.AreEqual(val, totalRecords, $"log scan 2: totalRecords");
            }
        }

        class LogObserver : IObserver<IFasterScanIterator<MyKey, MyValue>>
        {
            int val = 0;

            public void OnCompleted()
            {
                Assert.AreEqual(val == totalRecords, $"LogObserver.OnCompleted: totalRecords");
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(IFasterScanIterator<MyKey, MyValue> iter)
            {
                while (iter.GetNext(out _, out MyKey key, out MyValue value))
                {
                    Assert.AreEqual(val, key.key, $"LogObserver.OnNext: key");
                    Assert.AreEqual(val, value.value, $"LogObserver.OnNext: value");
                    val++;
                }
            }
        }
    }
}

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
        const int totalRecords = 50;
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
        public void DiskWriteScanBasicTest([Values] TestUtils.DeviceType deviceType)
        {


#if WINDOWS
            //*#*#*# Bug #143131 - fht.Log.Scan failing to get proper value on EmulatedAzure only  *#*#*#
            if (deviceType == TestUtils.DeviceType.EmulatedAzure)
            {
                return;
            }
#endif


            string logfilename = path + "DiskWriteScanBasicTest" + deviceType.ToString() + ".log";
            string objlogfilename = path + "DiskWriteScanBasicTest" + deviceType.ToString() + ".obj.log";

            log = TestUtils.CreateTestDevice(deviceType, logfilename);
            objlog = TestUtils.CreateTestDevice(deviceType, objlogfilename);
            fht = new FasterKV<MyKey, MyValue>
                           (128,
                           logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 },
                           checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                           serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                           );

            using var session = fht.For(new MyFunctions()).NewSession<MyFunctions>();

            var s = fht.Log.Subscribe(new LogObserver());

            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                session.Upsert(ref _key, ref _value, Empty.Default, 0);
                if (i % 100 == 0) fht.Log.FlushAndEvict(true);
            }
            fht.Log.FlushAndEvict(true);
            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.SinglePageBuffering))
            {

                int val = 0;
                while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
                {
                    Assert.IsTrue(key.key == val,$"log scan 1: key.key: {key.key} should = val: {val}");
                    Assert.IsTrue(value.value == val, $"log scan 1: value.key: {value.value} should = val: {val}");
                    val++;
                }
                Assert.IsTrue(totalRecords == val, $"log scan 2: totalRecords: {totalRecords} should = val: {val}");
            }

            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.DoublePageBuffering))
            {
                int val = 0;
                while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
                {
                    Assert.IsTrue(key.key == val, $"log scan 2: key.key: {key.key} should = val: {val}");
                    Assert.IsTrue(value.value == val, $"log scan 2: value.key: {value.value} should = val: {val}");
                    val++;
                }
                Assert.IsTrue(totalRecords == val, $"log scan 2: totalRecords: {totalRecords} should = val: {val}");
            }

            s.Dispose();
        }

        class LogObserver : IObserver<IFasterScanIterator<MyKey, MyValue>>
        {
            int val = 0;

            public void OnCompleted()
            {
                Assert.IsTrue(val == totalRecords, $"OnCompleted: totalRecords: {totalRecords} should = val: {val}");
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(IFasterScanIterator<MyKey, MyValue> iter)
            {
                while (iter.GetNext(out _, out MyKey key, out MyValue value))
                {
                    Assert.IsTrue(key.key == val,$"OnNext: key.key: {key.key} should = val: {val}");
                    Assert.IsTrue(value.value == val, $"OnNext: value.value: {value.value} should = val: {val}");
                    val++;
                }
            }
        }
    }
}

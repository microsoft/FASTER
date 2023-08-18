// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;
using static FASTER.test.BlittableFASTERScanTests;
using static FASTER.test.TestUtils;

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
            path = MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(path, wait: true);
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

            DeleteDirectory(path);
        }

        internal struct GenericPushScanTestFunctions : IScanIteratorFunctions<MyKey, MyValue>
        {
            internal long numRecords;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords);

            public bool SingleReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords)
            {
                Assert.AreEqual(numRecords, key.key, $"log scan 1: key");
                Assert.AreEqual(numRecords, value.value, $"log scan 1: value");

                ++numRecords;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DiskWriteScanBasicTest([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, $"{path}DiskWriteScanBasicTest_{deviceType}.log");
            objlog = CreateTestDevice(deviceType, $"{path}DiskWriteScanBasicTest_{deviceType}.obj.log");
            fht = new (128,
                      logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 },
                      serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() },
                      lockingMode: scanIteratorType == ScanIteratorType.Pull ? LockingMode.None : LockingMode.Standard
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

            GenericPushScanTestFunctions scanIteratorFunctions = new();

            void scanAndVerify(ScanBufferingMode sbm)
            {
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = fht.Log.Scan(start, fht.Log.TailAddress, sbm);
                    while (iter.GetNext(out var recordInfo))
                        scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default);
                }
                else
                    Assert.IsTrue(fht.Log.Scan(ref scanIteratorFunctions, start, fht.Log.TailAddress, sbm), "Failed to complete push iteration");

                Assert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            scanAndVerify(ScanBufferingMode.SinglePageBuffering);
            scanAndVerify(ScanBufferingMode.DoublePageBuffering);
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

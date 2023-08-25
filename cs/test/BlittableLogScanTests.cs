// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;
using static FASTER.test.TestUtils;

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
            DeleteDirectory(MethodTestDir, wait:true);
            log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 }, lockingMode: LockingMode.None);
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        internal struct BlittablePushScanTestFunctions : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            internal long numRecords;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords);

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords)
            {
                Assert.AreEqual(numRecords, key.kfield1);
                Assert.AreEqual(numRecords + 1, key.kfield2);
                Assert.AreEqual(numRecords, value.vfield1);
                Assert.AreEqual(numRecords + 1, value.vfield2);

                ++numRecords;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public void BlittableDiskWriteScan([Values] ScanIteratorType scanIteratorType)
        {
            using var session = fht.For(new Functions()).NewSession<Functions>();

            using var s = fht.Log.Subscribe(new LogObserver());
            var start = fht.Log.TailAddress;

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            fht.Log.FlushAndEvict(true);

            BlittablePushScanTestFunctions scanIteratorFunctions = new();
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

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public void BlittableScanJumpToBeginAddressTest()
        {
            using var session = fht.For(new Functions()).NewSession<Functions>();

            const int numRecords = 200;
            const int numTailRecords = 10;
            long shiftBeginAddressTo = 0;
            int shiftToKey = 0;
            for (int i = 0; i < numRecords; i++)
            {
                if (i == numRecords - numTailRecords)
                {
                    shiftBeginAddressTo = fht.Log.TailAddress;
                    shiftToKey = i;
                }
                var key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key, ref value, Empty.Default, 0);
            }

            using var iter = fht.Log.Scan(fht.Log.HeadAddress, fht.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                Assert.IsTrue(iter.GetNext(out var recordInfo));
                Assert.AreEqual(i, iter.GetKey().kfield1);
                Assert.AreEqual(i, iter.GetValue().vfield1);
            }

            fht.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            { 
                Assert.IsTrue(iter.GetNext(out var recordInfo));
                if (i == 0)
                    Assert.AreEqual(fht.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                Assert.AreEqual(expectedKey, iter.GetKey().kfield1);
                Assert.AreEqual(expectedKey, iter.GetValue().vfield1);
            }
        }

        class LogObserver : IObserver<IFasterScanIterator<KeyStruct, ValueStruct>>
        {
            int val = 0;

            public void OnCompleted()
            {
                Assert.AreEqual(totalRecords, val);
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(IFasterScanIterator<KeyStruct, ValueStruct> iter)
            {
                while (iter.GetNext(out _, out KeyStruct key, out ValueStruct value))
                {
                    Assert.AreEqual(val, key.kfield1);
                    Assert.AreEqual(val + 1, key.kfield2);
                    Assert.AreEqual(val, value.vfield1);
                    Assert.AreEqual(val + 1, value.vfield2);
                    val++;
                }
            }
        }
    }
}

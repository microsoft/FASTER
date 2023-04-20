// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using static FASTER.test.BlittableIterationTests;
using System.Collections.Generic;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class VarLenBlittableIterationTests
    {
        private FasterKV<Key, VLValue> fht;
        private IDevice log;
        private string path;

        // Note: We always set value.length to 2, which includes both VLValue members; we are not exercising the "Variable Length" aspect here.
        private const int ValueLength = 2;

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
            DeleteDirectory(path);
        }

        internal struct VarLenBlittablePushIterationTestFunctions : IScanIteratorFunctions<Key, VLValue>
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref Key key, ref VLValue value, RecordMetadata recordMetadata, long numberOfRecords)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords);

            public bool SingleReader(ref Key key, ref VLValue value, RecordMetadata recordMetadata, long numberOfRecords)
            {
                if (keyMultToValue > 0)
                    Assert.AreEqual(key.key * keyMultToValue, value.field1);
                return stopAt != ++numRecords;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenBlittableIterationBasicTest([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, $"{path}{deviceType}.log");
            fht = new FasterKV<Key, VLValue>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 },
                 lockingMode: scanIteratorType == ScanIteratorType.Pull ? LockingMode.None : LockingMode.Standard);

            using var session = fht.NewSession(new VLFunctions());
            VarLenBlittablePushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 500;
            var start = fht.Log.TailAddress;

            void iterateAndVerify(int keyMultToValue, int expectedRecs)
            {
                scanIteratorFunctions.keyMultToValue = keyMultToValue;
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = session.Iterate();
                    while (iter.GetNext(out var recordInfo))
                        scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default);
                }
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                Assert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Note: We always set value.length to 2, which includes both VLValue members; we are not exercising the "Variable Length" aspect here.

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = ValueLength };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = 2 * i, length = ValueLength };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = ValueLength };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = ValueLength };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new Key { key = i };
                session.Delete(ref key1);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = 3 * i, length = ValueLength };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(3, totalRecords);

            fht.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenBlittableIterationPushStopTest([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, $"{path}{deviceType}.log");
            fht = new FasterKV<Key, VLValue>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 });

            using var session = fht.NewSession(new VLFunctions());
            VarLenBlittablePushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            void scanAndVerify(int stopAt, bool useScan)
            {
                scanIteratorFunctions.numRecords = 0;
                scanIteratorFunctions.stopAt = stopAt;
                if (useScan)
                    Assert.IsFalse(fht.Log.Scan(ref scanIteratorFunctions, start, fht.Log.TailAddress), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    Assert.IsFalse(session.Iterate(ref scanIteratorFunctions), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                Assert.AreEqual(stopAt, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = ValueLength };
                session.Upsert(ref key1, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void VarLenBlittableIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] LockingMode lockingMode, [Values] ScanMode scanMode)
        {
            log = Devices.CreateLogDevice($"{path}lock_test.log");
            fht = new FasterKV<Key, VLValue>
                 // Must be large enough to contain all records in memory to exercise locking
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 25, PageSizeBits = 19, SegmentSizeBits = 22 });

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = fht.NewSession(new VLFunctions());
                VarLenBlittablePushIterationTestFunctions scanIteratorFunctions = new();
                if (scanMode == ScanMode.Scan)
                    Assert.IsTrue(fht.Log.Scan(ref scanIteratorFunctions, start, fht.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                Assert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                using var session = fht.NewSession(new VLFunctions());
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new Key { key = i };
                    var value = new VLValue { field1 = (tid + 1) * i, length = ValueLength };
                    session.Upsert(ref key1, ref value);
                }
            }

            { // Initial population
                using var session = fht.NewSession(new VLFunctions());
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new Key { key = i };
                    var value = new VLValue { field1 = i, length = ValueLength };
                    session.Upsert(ref key1, ref value);
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            var numThreads = scanThreads + updateThreads;
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t < scanThreads)
                    tasks.Add(Task.Factory.StartNew(() => LocalScan(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalUpdate(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}

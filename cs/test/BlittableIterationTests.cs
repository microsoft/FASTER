// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class BlittableIterationTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;
        private string path;

        [SetUp]
        public void Setup()
        {
            path = MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(path, wait:true);
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

        internal struct BlittablePushIterationTestFunctions : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords)
            {
                if (keyMultToValue > 0)
                    Assert.AreEqual(key.kfield1 * keyMultToValue, value.vfield1);
                return stopAt != ++numRecords;
            }

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords);
            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnException(Exception exception, long numberOfRecords) { }
            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void BlittableIterationBasicTest([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, $"{path}{deviceType}.log");
            fht = new FasterKV<KeyStruct, ValueStruct>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 },
                 lockingMode: scanIteratorType == ScanIteratorType.Pull ? LockingMode.None : LockingMode.Standard);

            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();
            BlittablePushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 500;

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

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 2 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords/2; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i+=2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                session.Delete(ref key1);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 3 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(3, totalRecords);

            fht.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void BlittableIterationPushStopTest()
        {
            log = Devices.CreateLogDevice($"{path}stop_test.log");
            fht = new FasterKV<KeyStruct, ValueStruct>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 });

            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();
            BlittablePushIterationTestFunctions scanIteratorFunctions = new();

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
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void BlittableIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] LockingMode lockingMode, [Values] ScanMode scanMode)
        {
            log = Devices.CreateLogDevice($"{path}lock_test.log");
            fht = new FasterKV<KeyStruct, ValueStruct>(1L << 20,
                 // Must be large enough to contain all records in memory to exercise locking
                 new LogSettings { LogDevice = log, MemorySizeBits = 25, PageSizeBits = 20, SegmentSizeBits = 22 },
                 lockingMode: lockingMode);

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();
                BlittablePushIterationTestFunctions scanIteratorFunctions = new();
                if (scanMode == ScanMode.Scan)
                    Assert.IsTrue(fht.Log.Scan(ref scanIteratorFunctions, start, fht.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                Assert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = (tid + 1) * i, vfield2 = i + 1 };
                    session.Upsert(ref key1, ref value, 0);
                }
            }

            { // Initial population
                using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
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

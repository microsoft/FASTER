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
    internal class GenericIterationTests
    {
        private FasterKV<MyKey, MyValue> fht;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            // Tests call InternalSetup()
        }

        private void InternalSetup(ScanIteratorType scanIteratorType, bool largeMemory)
        {
            // Default lockingMode for this iterator type.
            var lockingMode = scanIteratorType == ScanIteratorType.Pull ? LockingMode.None : LockingMode.Standard;
            InternalSetup(lockingMode, largeMemory);
        }

        private void InternalSetup(LockingMode lockingMode, bool largeMemory)
        {
            // Broke this out as we have different requirements by test.
            log = Devices.CreateLogDevice(MethodTestDir + "/GenericIterationTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/GenericIterationTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = largeMemory ? 25 : 14, PageSizeBits = largeMemory ? 20 : 9 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() },
                lockingMode: lockingMode);
            session = fht.For(new MyFunctionsDelete()).NewSession<MyFunctionsDelete>();
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

            DeleteDirectory(MethodTestDir);
        }

        internal struct GenericPushIterationTestFunctions : IScanIteratorFunctions<MyKey, MyValue>
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, nextAddress);

            public bool SingleReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
            {
                if (keyMultToValue > 0)
                    Assert.AreEqual(key.key * keyMultToValue, value.value);
                return stopAt != ++numRecords;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void GenericIterationBasicTest([Values] ScanIteratorType scanIteratorType)
        {
            InternalSetup(scanIteratorType, largeMemory: false);
            GenericPushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            void iterateAndVerify(int keyMultToValue, int expectedRecs)
            {
                scanIteratorFunctions.keyMultToValue = keyMultToValue;
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = session.Iterate();
                    while (iter.GetNext(out var recordInfo))
                        scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default, default);
                }
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                Assert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 2 * i };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                session.Delete(ref key1);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 3 * i };
                session.Upsert(ref key1, ref value);
            }
            iterateAndVerify(3, totalRecords);

            fht.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void GenericIterationPushStopTest()
        {
            InternalSetup(ScanIteratorType.Push, largeMemory: false);
            GenericPushIterationTestFunctions scanIteratorFunctions = new();

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
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void GenericIterationPushLockTest([Values(1, 4)] int scanThreads, [Values(1, 4)] int updateThreads, [Values] LockingMode lockingMode, [Values] ScanMode scanMode)
        {
            InternalSetup(lockingMode, largeMemory: true);

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = fht.For(new MyFunctionsDelete()).NewSession<MyFunctionsDelete>();
                GenericPushIterationTestFunctions scanIteratorFunctions = new();

                if (scanMode == ScanMode.Scan)
                    Assert.IsTrue(fht.Log.Scan(ref scanIteratorFunctions, start, fht.Log.TailAddress), $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");
                Assert.AreEqual(totalRecords, scanIteratorFunctions.numRecords);
            }

            void LocalUpdate(int tid)
            {
                using var session = fht.For(new MyFunctionsDelete()).NewSession<MyFunctionsDelete>();
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new MyKey { key = i };
                    var value = new MyValue { value = (tid + 1) * i };
                    session.Upsert(ref key1, ref value);
                }
            }

            { // Initial population
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new MyKey { key = i };
                    var value = new MyValue { value = i };
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

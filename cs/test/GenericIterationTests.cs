// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
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

            log ??= Devices.CreateLogDevice(MethodTestDir + "/GenericIterationTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/GenericIterationTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
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

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, nextAddress);

            public bool SingleReader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
            {
                if (keyMultToValue > 0)
                    Assert.AreEqual(key.key * keyMultToValue, value.value);

                ++numRecords;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public void GenericIterationBasicTest([Values] ScanIteratorType scanIteratorType)
        {
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
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), "Failed to complete push iteration");

                Assert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 2 * i };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Delete(ref key1, 0);
            }

            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 3 * i };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(3, totalRecords);
        }
    }
}

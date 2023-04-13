// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class VarLenBlittableIterationTests
    {
        private FasterKV<Key, VLValue> fht;
        private IDevice log;
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
            DeleteDirectory(path);
        }

        internal struct VarLenBlittablePushIterationTestFunctions : IScanIteratorFunctions<Key, VLValue>
        {
            internal int keyMultToValue;
            internal long numRecords;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref Key key, ref VLValue value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, nextAddress);

            public bool SingleReader(ref Key key, ref VLValue value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
            {
                if (keyMultToValue > 0)
                    Assert.AreEqual(key.key * keyMultToValue, value.field1);

                ++numRecords;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void VarLenBlittableIterationBasicTest([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            string filename = path + "VarLenBlittableIterationTest1" + deviceType.ToString() + ".log";
            log = CreateTestDevice(deviceType, filename);
            fht = new FasterKV<Key, VLValue>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 });

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
                        scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default, default);
                }
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), "Failed to complete push iteration");

                Assert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Note: We always set value.length to 2, which includes both VLValue members; we are not exercising the "Variable Length" aspect here.

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = 2 };
                session.Upsert(ref key1, ref value);
            }

            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = 2 * i, length = 2 };
                session.Upsert(ref key1, ref value);
            }

            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = 2 };
                session.Upsert(ref key1, ref value);
            }

            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = 2 };
                session.Upsert(ref key1, ref value);
            }

            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = i, length = 2 };
                session.Delete(ref key1);
            }

            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new Key { key = i };
                var value = new VLValue { field1 = 3 * i, length = 2 };
                session.Upsert(ref key1, ref value);
            }

            iterateAndVerify(3, totalRecords);
        }
    }
}

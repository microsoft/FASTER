// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
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

        internal struct PushBlittableScanFunctions : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            internal int keyMultToValue;
            internal long numRecords;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress) 
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, nextAddress);

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
            {
                if (keyMultToValue > 0)
                    Assert.AreEqual(key.kfield1 * keyMultToValue, value.vfield1);

                ++numRecords;
                return true;
            }
            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void BlittableIterationTest1([Values] DeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            string filename = path + "BlittableIterationTest1" + deviceType.ToString() + ".log";
            log = CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 });

            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();
            PushBlittableScanFunctions scanIteratorFunctions = new();

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
                    {
                        ++scanIteratorFunctions.numRecords;
                        if (keyMultToValue > 0)
                            Assert.AreEqual(iter.GetKey().kfield1 * keyMultToValue, iter.GetValue().vfield1);
                    }
                }
                else
                    Assert.IsTrue(session.Iterate(ref scanIteratorFunctions), "Failed to complete push iteration");

                Assert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 2 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords/2; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i+=2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Delete(ref key1, 0);
            }

            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 3 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            iterateAndVerify(3, totalRecords);
        }
    }
}

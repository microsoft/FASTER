// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

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
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait:true);

            log = Devices.CreateLogDevice(path + "/BlittableIterationTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public void BlittableIterationTest1([Values] TestUtils.DeviceType deviceType)
        {

            string filename = path + "BlittableIterationTest1" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                 (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9, SegmentSizeBits = 22 });

#if WINDOWS
            //*#*#*# TO DO: Figure Out why this DeviceType fails  *#*#*#
            if (deviceType == TestUtils.DeviceType.EmulatedAzure)
            {
                return;
            }
#endif

            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();

            const int totalRecords = 500;
            var start = fht.Log.TailAddress;

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            int count = 0;
            var iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().vfield1 == iter.GetKey().kfield1);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 2 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            count = 0;
            iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().vfield1 == iter.GetKey().kfield1 * 2);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = totalRecords/2; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            count = 0;
            iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i+=2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            count = 0;
            iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Delete(ref key1, 0);
            }

            count = 0;
            iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 3 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0);
            }

            count = 0;
            iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().vfield1 == iter.GetKey().kfield1 * 3);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

        }
    }
}

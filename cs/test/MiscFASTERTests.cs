// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class MiscFASTERTests
    {
        private FasterKV<int, MyValue> fht;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/MiscFASTERTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/MiscFASTERTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<int, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<int, MyValue> { valueSerializer = () => new MyValueSerializer() }
                );
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void MixedTest1()
        {
            using var session = fht.For(new MixedFunctions()).NewSession<MixedFunctions>();

            int key = 8999998;
            var input1 = new MyInput { value = 23 };
            MyOutput output = new MyOutput();

            session.RMW(ref key, ref input1, Empty.Default, 0);

            int key2 = 8999999;
            var input2 = new MyInput { value = 24 };
            session.RMW(ref key2, ref input2, Empty.Default, 0);

            session.Read(ref key, ref input1, ref output, Empty.Default, 0);
            Assert.IsTrue(output.value.value == input1.value);

            session.Read(ref key2, ref input2, ref output, Empty.Default, 0);
            Assert.IsTrue(output.value.value == input2.value);
        }

        [Test]
        [Category("FasterKV")]
        public void MixedTest2()
        {
            using var session = fht.For(new MixedFunctions()).NewSession<MixedFunctions>();

            for (int i = 0; i < 2000; i++)
            {
                var value = new MyValue { value = i };
                session.Upsert(ref i, ref value, Empty.Default, 0);
            }

            var key2 = 23;
            var input = new MyInput();
            MyOutput g1 = new MyOutput();
            var status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status == Status.PENDING)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            Assert.IsTrue(g1.value.value == 23);

            key2 = 99999;
            status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status == Status.PENDING)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.NOTFOUND);
            }
        }

        [Test]
        [Category("FasterKV")]
        public void ShouldCreateNewRecordIfConcurrentWriterReturnsFalse()
        {
            var copyOnWrite = new FunctionsCopyOnWrite();

            // FunctionsCopyOnWrite
            var log = default(IDevice);
            try
            {
                log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog1.log", deleteOnClose: true);
                using var fht = new FasterKV<KeyStruct, ValueStruct>
                    (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
                using var session = fht.NewSession(copyOnWrite);

                var key = default(KeyStruct);
                var value = default(ValueStruct);

                key = new KeyStruct() { kfield1 = 1, kfield2 = 2 };
                value = new ValueStruct() { vfield1 = 1000, vfield2 = 2000 };

                session.Upsert(ref key, ref value, Empty.Default, 0);

                value = new ValueStruct() { vfield1 = 1001, vfield2 = 2002 };
                session.Upsert(ref key, ref value, Empty.Default, 0);

                var recordCount = 0;
                using (var iterator = fht.Log.Scan(fht.Log.BeginAddress, fht.Log.TailAddress))
                {
                    while (iterator.GetNext(out var info))
                    {
                        recordCount++;
                    }
                }

                Assert.AreEqual(1, copyOnWrite.ConcurrentWriterCallCount, 2);
                Assert.AreEqual(2, recordCount);
            }
            finally
            {
                if (log != null)
                    log.Dispose();
            }
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.IO;
using NUnit.Framework;

namespace FASTER.test
{

    [TestFixture]
    internal class MiscFASTERTests
    {
        private FasterKV<int, MyValue, MyInput, MyOutput, Empty, MixedFunctions> fht;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\MiscFASTERTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\MiscFASTERTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<int, MyValue, MyInput, MyOutput, Empty, MixedFunctions>
                (128, new MixedFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<int, MyValue> { valueSerializer = () => new MyValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Dispose();
            objlog.Dispose();
        }


        [Test]
        public void MixedTest1()
        {
            using var session = fht.NewSession();

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
        public void MixedTest2()
        {
            using var session = fht.NewSession();

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
        public void ShouldCreateNewRecordIfConcurrentWriterReturnsFalse()
        {
            var copyOnWrite = new FunctionsCopyOnWrite();

            // FunctionsCopyOnWrite
            var log = default(IDevice);
            try
            {
                log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
                using var fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>
                    (128, copyOnWrite, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
                using var session = fht.NewSession();

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

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
    internal class NeedCopyUpdateTests
    {
        private FasterKV<int, RMWValue> fht;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\NeedCopyUpdateTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\NeedCopyUpdateTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<int, RMWValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<int, RMWValue> { valueSerializer = () => new RMWValueSerializer() }
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
        public void TryAddTest()
        {
            using var session = fht.For(new TryAddTestFunctions()).NewSession<TryAddTestFunctions>();

            Status status;
            var key = 1;
            var value1 = new RMWValue { value = 1 };
            var value2 = new RMWValue { value = 2 };

            status = session.RMW(ref key, ref value1); // NOTFOUND + InitialUpdated
            Assert.IsTrue(status == Status.NOTFOUND);
            Assert.IsTrue(value1.flag); // InitialUpdater is called

            status = session.RMW(ref key, ref value2); // OK + first InPlaceUpdated
            Assert.IsTrue(status == Status.OK);

            fht.Log.Flush(true);
            status = session.RMW(ref key, ref value2); // OK + CopyUpdater
            Assert.IsTrue(status == Status.OK);

            fht.Log.FlushAndEvict(true);
            status = session.RMW(ref key, ref value2, Status.OK, 0); // PENDING + CopyUpdater + OK
            Assert.IsTrue(status == Status.PENDING);
            session.CompletePending(true);

            // Test stored value. Should be value1
            var output = new RMWValue();
            status = session.Read(ref key, ref value1, ref output, Status.OK, 0);
            Assert.IsTrue(status == Status.PENDING);
            session.CompletePending(true);

            status = session.Delete(ref key);
            Assert.IsTrue(status == Status.OK);
            session.CompletePending(true);
            fht.Log.FlushAndEvict(true);
            status = session.RMW(ref key, ref value2, Status.NOTFOUND, 0); // PENDING + InitialUpdated + NOTFOUND
            Assert.IsTrue(status == Status.PENDING);
            session.CompletePending(true);
        }
    }

    internal class RMWValue
    {
        public int value;
        public bool flag;
    }

    internal class RMWValueSerializer : BinaryObjectSerializer<RMWValue>
    {
        public override void Serialize(ref RMWValue value)
        {
            writer.Write(value.value);
        }

        public override void Deserialize(out RMWValue value)
        {
            value = new RMWValue
            {
                value = reader.ReadInt32()
            };
        }
    }

    internal class TryAddTestFunctions : SimpleFunctions<int, RMWValue, Status>
    {
        public override void InitialUpdater(ref int key, ref RMWValue input, ref RMWValue value)
        {
            input.flag = true;
            base.InitialUpdater(ref key, ref input, ref value);
        }

        public override bool NeedCopyUpdate(ref int key, ref RMWValue input, ref RMWValue oldValue) => false;

        public override void CopyUpdater(ref int key, ref RMWValue input, ref RMWValue oldValue, ref RMWValue newValue)
        {
            Assert.Fail("CopyUpdater");
        }

        public override bool InPlaceUpdater(ref int key, ref RMWValue input, ref RMWValue value) => false;

        public override void RMWCompletionCallback(ref int key, ref RMWValue input, Status ctx, Status status)
        {
            Assert.IsTrue(status == ctx);

            if (status == Status.NOTFOUND)
                Assert.IsTrue(input.flag); // InitialUpdater is called.
        }

        public override void ReadCompletionCallback(ref int key, ref RMWValue input, ref RMWValue output, Status ctx, Status status)
        {
            Assert.IsTrue(input.value == output.value);
        }
    }
}
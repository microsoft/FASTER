// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/NeedCopyUpdateTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/NeedCopyUpdateTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<int, RMWValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                serializerSettings: new SerializerSettings<int, RMWValue> { valueSerializer = () => new RMWValueSerializer() }
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
        public void TryAddTest()
        {
            using var session = fht.For(new TryAddTestFunctions()).NewSession<TryAddTestFunctions>();

            Status status;
            var key = 1;
            var value1 = new RMWValue { value = 1 };
            var value2 = new RMWValue { value = 2 };

            status = session.RMW(ref key, ref value1); // InitialUpdater + NOTFOUND
            Assert.IsTrue(status.IsNotFound);
            Assert.IsTrue(value1.flag); // InitialUpdater is called

            status = session.RMW(ref key, ref value2); // InPlaceUpdater + OK
            Assert.IsTrue(status.IsInPlaceUpdate);

            fht.Log.Flush(true);
            status = session.RMW(ref key, ref value2); // NeedCopyUpdate + OK
            Assert.IsTrue(status.IsInPlaceUpdate);

            fht.Log.FlushAndEvict(true);
            status = session.RMW(ref key, ref value2, new(StatusCode.OK), 0); // PENDING + NeedCopyUpdate + OK
            Assert.IsTrue(status.IsPending);
            session.CompletePending(true);

            // Test stored value. Should be value1
            var output = new RMWValue();
            status = session.Read(ref key, ref value1, ref output, new(StatusCode.OK), 0);
            Assert.IsTrue(status.IsPending);
            session.CompletePending(true);

            status = session.Delete(ref key);
            Assert.IsTrue(status.IsFound);
            session.CompletePending(true);
            fht.Log.FlushAndEvict(true);
            status = session.RMW(ref key, ref value2, new(StatusCode.NotFound | StatusCode.NewRecord), 0); // PENDING + InitialUpdater + NOTFOUND
            Assert.IsTrue(status.IsPending);
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

    internal class TryAddTestFunctions : TryAddFunctions<int, RMWValue, Status>
    {
        public override void InitialUpdater(ref int key, ref RMWValue input, ref RMWValue value, ref RMWValue output, ref RecordInfo recordInfo, long address)
        {
            input.flag = true;
            base.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
        }

        public override void CopyUpdater(ref int key, ref RMWValue input, ref RMWValue oldValue, ref RMWValue newValue, ref RMWValue output, ref RecordInfo recordInfo, long address)
        {
            Assert.Fail("CopyUpdater");
        }

        public override void RMWCompletionCallback(ref int key, ref RMWValue input, ref RMWValue output, Status ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(ctx, status);

            if (status.IsNotFound)
                Assert.IsTrue(input.flag); // InitialUpdater is called.
        }

        public override void ReadCompletionCallback(ref int key, ref RMWValue input, ref RMWValue output, Status ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(output.value, input.value);
        }
    }
}
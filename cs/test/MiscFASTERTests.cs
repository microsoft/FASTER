// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using static FASTER.test.TestUtils;

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
            MyOutput output = new();

            session.RMW(ref key, ref input1, Empty.Default, 0);

            int key2 = 8999999;
            var input2 = new MyInput { value = 24 };
            session.RMW(ref key2, ref input2, Empty.Default, 0);

            session.Read(ref key, ref input1, ref output, Empty.Default, 0);
            Assert.AreEqual(input1.value, output.value.value);

            session.Read(ref key2, ref input2, ref output, Empty.Default, 0);
            Assert.AreEqual(input2.value, output.value.value);
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
            MyInput input = new();
            MyOutput g1 = new();
            var status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePendingWithOutputs(out var outputs, wait:true);
                (status, _) = GetSinglePendingResult(outputs);
            }
            Assert.IsTrue(status.Found);

            Assert.AreEqual(23, g1.value.value);

            key2 = 99999;
            status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, _) = GetSinglePendingResult(outputs);
            }
            Assert.IsFalse(status.Found);
        }

        [Test]
        [Category("FasterKV")]
        public void ForceRCUAndRecover([Values(UpdateOp.Upsert, UpdateOp.Delete)] UpdateOp updateOp)
        {
            var copyOnWrite = new FunctionsCopyOnWrite();

            // FunctionsCopyOnWrite
            var log = default(IDevice);
            FasterKV<KeyStruct, ValueStruct> fht = default;
            ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, IFunctions<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>> session = default;

            try
            {
                var checkpointDir = MethodTestDir + $"/checkpoints";
                log = Devices.CreateLogDevice(MethodTestDir + "/hlog1.log", deleteOnClose: true);
                fht = new FasterKV<KeyStruct, ValueStruct>
                    (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir },
                    lockingMode: LockingMode.None);

                session = fht.NewSession(copyOnWrite);

                var key = default(KeyStruct);
                var value = default(ValueStruct);
                var input = default(InputStruct);
                var output = default(OutputStruct);

                key = new KeyStruct() { kfield1 = 1, kfield2 = 2 };
                value = new ValueStruct() { vfield1 = 1000, vfield2 = 2000 };

                var status = session.Upsert(ref key, ref input, ref value, ref output, out RecordMetadata recordMetadata1);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                // ConcurrentWriter and InPlaceUpater return false, so we create a new record.
                RecordMetadata recordMetadata2;
                value = new ValueStruct() { vfield1 = 1001, vfield2 = 2002 };
                if (updateOp == UpdateOp.Upsert)
                { 
                    status = session.Upsert(ref key, ref input, ref value, ref output, out recordMetadata2);
                    Assert.AreEqual(1, copyOnWrite.ConcurrentWriterCallCount);
                    Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
                else
                { 
                    status = session.RMW(ref key, ref input, ref output, out recordMetadata2);
                    Assert.AreEqual(1, copyOnWrite.InPlaceUpdaterCallCount);
                    Assert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());
                }
                Assert.Greater(recordMetadata2.Address, recordMetadata1.Address);

                using (var iterator = fht.Log.Scan(fht.Log.BeginAddress, fht.Log.TailAddress))
                {
                    Assert.True(iterator.GetNext(out var info));    // We should only get the new record...
                    Assert.False(iterator.GetNext(out info));       // ... the old record was Sealed.
                }
                status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, status.ToString());

                fht.TryInitiateFullCheckpoint(out Guid token, CheckpointType.Snapshot);
                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                session.Dispose();
                fht.Dispose();

                fht = new FasterKV<KeyStruct, ValueStruct>
                    (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir },
                    lockingMode: LockingMode.None);

                fht.Recover(token);
                session = fht.NewSession(copyOnWrite);

                using (var iterator = fht.Log.Scan(fht.Log.BeginAddress, fht.Log.TailAddress))
                {
                    Assert.True(iterator.GetNext(out var info));    // We should get both records...
                    Assert.True(iterator.GetNext(out info));        // ... the old record was Unsealed by Recovery.
                }
                status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, status.ToString());
            }
            finally
            {
                session?.Dispose();
                fht?.Dispose();
                log?.Dispose();
            }
        }
    }
}

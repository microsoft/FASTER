// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class GenericLogCompactionTests
    {
        private FasterKV<MyKey, MyValue> fht;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private IDevice log, objlog;
        private string path;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait:true);

            if (TestContext.CurrentContext.Test.Arguments.Length == 0)
            {
                // Default log creation
                log = Devices.CreateLogDevice(path + "/GenericLogCompactionTests.log", deleteOnClose: true);
                objlog = Devices.CreateLogDevice(path + "/GenericLogCompactionTests.obj.log", deleteOnClose: true);

                fht = new FasterKV<MyKey, MyValue>
                    (128,
                    logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                    checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                    serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                    );
            }
            else
            {
                // For this class, deviceType is the only parameter. Using this to illustrate the approach; NUnit doesn't provide metadata for arguments,
                // so for multi-parameter tests it is probably better to stay with the "separate SetUp method" approach.
                var deviceType = (TestUtils.DeviceType)TestContext.CurrentContext.Test.Arguments[0];

                log = TestUtils.CreateTestDevice(deviceType, $"{path}LogCompactBasicTest_{deviceType}.log");
                objlog = TestUtils.CreateTestDevice(deviceType, $"{path}LogCompactBasicTest_{deviceType}.obj.log");

                fht = new FasterKV<MyKey, MyValue>
                    (128,
                    logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9, SegmentSizeBits = 22 },
                    checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                    serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                    );
            }
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

            TestUtils.DeleteDirectory(path);
        }

        // Basic test that where shift begin address to untilAddress after compact
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void LogCompactBasicTest([Values] TestUtils.DeviceType deviceType, [Values] CompactionType compactionType)
        {
            MyInput input = new MyInput();

            const int totalRecords = 500;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 250)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.ShiftBeginAddress(compactUntil);
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read all keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new MyOutput();

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    Assert.IsTrue(completedOutputs.Next());
                    Assert.AreEqual(Status.OK, completedOutputs.Current.Status);
                    output = completedOutputs.Current.Output;
                    Assert.IsFalse(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }
        }

        // Basic test where DO NOT shift begin address to untilAddress after compact 
        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        [Category("Smoke")]
        public void LogCompactNotShiftBeginAddrTest([Values] CompactionType compactionType)
        {
            MyInput input = new MyInput();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            // Do not shift begin to until address ... verify that is the case and verify all the keys
            compactUntil = session.Compact(compactUntil, compactionType);
            Assert.IsFalse(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new MyOutput();

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    Assert.AreEqual(Status.OK, status);
                    Assert.AreEqual(value.value, output.value.value);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        public void LogCompactTestNewEntries([Values] CompactionType compactionType)
        {
            MyInput input = new MyInput();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            fht.Log.Flush(true);

            var tail = fht.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.ShiftBeginAddress(compactUntil);
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);
            Assert.AreEqual(tail, fht.Log.TailAddress);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    Assert.AreEqual(Status.OK, status);
                    Assert.AreEqual(value.value, output.value.value);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        [Category("Smoke")]
        public void LogCompactAfterDeleteTest([Values] CompactionType compactionType)
        {
            MyInput input = new MyInput();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new MyKey { key = j };
                    session.Delete(ref key1, 0, 0);
                }
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.ShiftBeginAddress(compactUntil);
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    if (ctx == 0)
                    {
                        Assert.AreEqual(Status.OK, status);
                        Assert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        Assert.AreEqual(Status.NOTFOUND, status);
                    }
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]

        public void LogCompactBasicCustomFctnTest([Values] CompactionType compactionType)
        {
            MyInput input = new MyInput();

            const int totalRecords = 2000;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            fht.Log.ShiftBeginAddress(compactUntil);
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                {
                    session.CompletePending(true);
                }
                else
                {
                    if (ctx == 0)
                    {
                        Assert.AreEqual(Status.OK, status);
                        Assert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        Assert.AreEqual(Status.NOTFOUND, status);
                    }
                }
            }
        }

        // Same as basic test of Custom Functions BUT this will NOT shift begin address to untilAddress after compact
        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]

        public void LogCompactCustomFctnNotShiftBeginTest([Values] CompactionType compactionType)
        {
            MyInput input = new MyInput();

            const int totalRecords = 2000;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            Assert.AreNotEqual(compactUntil, fht.Log.BeginAddress);

            // Verified that begin address not changed so now compact and change Begin to untilAddress
            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            fht.Log.ShiftBeginAddress(compactUntil);
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                {
                    session.CompletePending(true);
                }
                else
                {
                    if (ctx == 0)
                    {
                        Assert.AreEqual(Status.OK, status);
                        Assert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        Assert.AreEqual(Status.NOTFOUND, status);
                    }
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]

        public void LogCompactCopyInPlaceCustomFctnTest([Values] CompactionType compactionType)
        {
            // Update: irrelevant as session compaction no longer uses Copy/CopyInPlace
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = fht.For(new MyFunctionsDelete()).NewSession<MyFunctionsDelete>();

            var key = new MyKey { key = 100 };
            var value = new MyValue { value = 20 };

            session.Upsert(ref key, ref value, 0, 0);

            fht.Log.Flush(true);

            value = new MyValue { value = 21 };
            session.Upsert(ref key, ref value, 0, 0);

            fht.Log.Flush(true);

            var compactionFunctions = new Test2CompactionFunctions();
            var compactUntil = session.Compact(fht.Log.TailAddress, compactionType, compactionFunctions);
            fht.Log.ShiftBeginAddress(compactUntil);

            var input = default(MyInput);
            var output = default(MyOutput);
            var status = session.Read(ref key, ref input, ref output, 0, 0);
            if (status == Status.PENDING)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }
        }

        private class Test2CompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public bool IsDeleted(ref MyKey key, ref MyValue value) => false;
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public bool IsDeleted(ref MyKey key, ref MyValue value) => value.value % 2 != 0;
        }

    }
}

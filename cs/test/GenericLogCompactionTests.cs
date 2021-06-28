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

            log = Devices.CreateLogDevice(path + "/GenericLogCompactionTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(path + "/GenericLogCompactionTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
            session = fht.For(new MyFunctionsDelete()).NewSession<MyFunctionsDelete>();
        }

        [TearDown]
        public void TearDown()
        {
            //** Bug #142980 - FasterKV: Blob Doesn't exist exception when .Dispose() when running Emulator.  -- Work around so doesn't crash during clean up is put try catch around
            try
            {
                session?.Dispose();
                session = null;
                fht?.Dispose();
                fht = null;
                log?.Dispose();
                log = null;
                objlog?.Dispose();
                objlog = null;
            }
            catch { }

            TestUtils.DeleteDirectory(path);

        }

        // Basic test that where shift begin address to untilAddress after compact
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void LogCompactBasicTest([Values] TestUtils.DeviceType deviceType)
        {
            // Reset all the log and fht values since using all deviceType
            string filename = path + "LogCompactBasicTest" + deviceType.ToString() + ".log";
            string objfilename = path + "LogCompactBasicTest_obj" + deviceType.ToString() + ".log";

            log = TestUtils.CreateTestDevice(deviceType, filename);
            objlog = TestUtils.CreateTestDevice(deviceType, objfilename);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9, SegmentSizeBits = 22 },  
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );

            session = fht.For(new MyFunctionsDelete()).NewSession<MyFunctionsDelete>();

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

            compactUntil = session.Compact(compactUntil, true);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read all keys - all should be present
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
                    Assert.IsTrue(status == Status.OK,"Found status:"+status.ToString());
                    Assert.IsTrue(output.value.value == value.value, "output value:"+output.value.value.ToString()+" value:"+value.value.ToString());
                }
            }
        }

        // Basic test where DO NOT shift begin address to untilAddress after compact 
        [Test]
        [Category("FasterKV")]
        public void LogCompactNotShiftBeginAddrTest()
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
            compactUntil = session.Compact(compactUntil, false);
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
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.value.value == value.value);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public void LogCompactTestNewEntries()
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
            compactUntil = session.Compact(compactUntil, true);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);
            Assert.IsTrue(fht.Log.TailAddress == tail);

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
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.value.value == value.value);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public void LogCompactAfterDeleteTest()
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

            compactUntil = session.Compact(compactUntil, true);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

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
                        Assert.IsTrue(status == Status.OK);
                        Assert.IsTrue(output.value.value == value.value);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.NOTFOUND);
                    }
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public void LogCompactBasicCustomFctnTest()
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

            compactUntil = session.Compact(compactUntil, true, default(EvenCompactionFunctions));
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

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
                        Assert.IsTrue(status == Status.OK);
                        Assert.IsTrue(output.value.value == value.value);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.NOTFOUND);
                    }
                }
            }
        }

        // Same as basic test of Custom Functions BUT this will NOT shift begin address to untilAddress after compact
        [Test]
        [Category("FasterKV")]
        public void LogCompactCustomFctnNotShiftBeginTest()
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

            compactUntil = session.Compact(compactUntil, false, default(EvenCompactionFunctions));
            Assert.IsFalse(fht.Log.BeginAddress == compactUntil);

            // Verified that begin address not changed so now compact and change Begin to untilAddress
            compactUntil = session.Compact(compactUntil, true, default(EvenCompactionFunctions));
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

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
                        Assert.IsTrue(status == Status.OK);
                        Assert.IsTrue(output.value.value == value.value);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.NOTFOUND);
                    }
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public void LogCompactCopyInPlaceCustomFctnTest()
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
            session.Compact(fht.Log.TailAddress, true, compactionFunctions);

            Assert.IsFalse(compactionFunctions.CopyCalled);

            var input = default(MyInput);
            var output = default(MyOutput);
            var status = session.Read(ref key, ref input, ref output, 0, 0);
            if (status == Status.PENDING)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(output.value.value == value.value);
            }
        }

        private class Test2CompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public bool CopyCalled;

            public void Copy(ref MyValue src, ref MyValue dst, IVariableLengthStruct<MyValue> valueLength)
            {
                if (src.value == 21)
                    CopyCalled = true;
                dst = src;
            }

            public bool CopyInPlace(ref MyValue src, ref MyValue dst, IVariableLengthStruct<MyValue> valueLength)
            {
                return false;
            }

            public bool IsDeleted(in MyKey key, in MyValue value)
            {
                return false;
            }
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public void Copy(ref MyValue src, ref MyValue dst, IVariableLengthStruct<MyValue> valueLength)
            {
                dst = src;
            }

            public bool CopyInPlace(ref MyValue src, ref MyValue dst, IVariableLengthStruct<MyValue> valueLength)
            {
                dst = src;
                return true;
            }

            public bool IsDeleted(in MyKey key, in MyValue value)
            {
                return value.value % 2 != 0;
            }
        }

    }
}

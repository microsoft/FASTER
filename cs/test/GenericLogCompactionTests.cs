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
    internal class GenericLogCompactionTests
    {
        private FasterKV<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> fht;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericLogCompactionTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericLogCompactionTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete>
                (128, new MyFunctionsDelete(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
            session = fht.NewSession();
        }

        [TearDown]
        public void TearDown()
        {
            session.Dispose();
            fht.Dispose();
            fht = null;
            log.Close();
            objlog.Close();
        }

        [Test]
        public void GenericLogCompactionTest1()
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

            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

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
        public void GenericLogCompactionTest2()
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
            fht.Log.Compact(compactUntil);
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
        public void GenericLogCompactionTest3()
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

            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
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
        public void GenericLogCompactionCustomFunctionsTest1()
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

            fht.Log.Compact(default(EvenCompactionFunctions), compactUntil);
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
        public void GenericLogCompactionCustomFunctionsTest2()
        {
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = fht.NewSession();

            var key = new MyKey { key = 100 };
            var value = new MyValue { value = 20 };

            session.Upsert(ref key, ref value, 0, 0);

            fht.Log.Flush(true);

            value = new MyValue { value = 21 };
            session.Upsert(ref key, ref value, 0, 0);

            fht.Log.Flush(true);

            var compactionFunctions = new Test2CompactionFunctions();
            fht.Log.Compact(compactionFunctions, fht.Log.TailAddress);

            Assert.IsTrue(compactionFunctions.CopyCalled);

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

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
    internal class ObjectFASTERTests
    {
        private FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions> fht;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\ObjectFASTERTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\ObjectFASTERTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions>
                (128, new MyFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Dispose();
        }

        [Test]
        public void ObjectInMemWriteRead()
        {
            using var session = fht.NewSession();

            var key1 = new MyKey { key = 9999999 };
            var value = new MyValue { value = 23 };

            MyInput input = null;
            MyOutput output = new MyOutput();

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            Assert.IsTrue(output.value.value == value.value);
        }

        [Test]
        public void ObjectInMemWriteRead2()
        {
            using var session = fht.NewSession();

            var key1 = new MyKey { key = 8999998 };
            var input1 = new MyInput { value = 23 };
            MyOutput output = new MyOutput();

            session.RMW(ref key1, ref input1, Empty.Default, 0);

            var key2 = new MyKey { key = 8999999 };
            var input2 = new MyInput { value = 24 };
            session.RMW(ref key2, ref input2, Empty.Default, 0);

            session.Read(ref key1, ref input1, ref output, Empty.Default, 0);

            Assert.IsTrue(output.value.value == input1.value);

            session.Read(ref key2, ref input2, ref output, Empty.Default, 0);
            Assert.IsTrue(output.value.value == input2.value);

        }


        [Test]
        public void ObjectDiskWriteRead()
        {
            using var session = fht.NewSession();

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key, ref value, Empty.Default, 0);
                // fht.ShiftReadOnlyAddress(fht.LogTailAddress);
            }

            var key2 = new MyKey { key = 23 };
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

            key2 = new MyKey { key = 99999 };
            status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status == Status.PENDING)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.NOTFOUND);
            }

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 1 };
                status = session.RMW(ref key1, ref input, Empty.Default, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
            }

            for (int i = 0; i < 2000; i++)
            {
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    session.CompletePending(true);
                }
                else
                {
                    if (i < 100)
                    {
                        Assert.IsTrue(output.value.value == value.value + 1);
                        Assert.IsTrue(output.value.value == value.value + 1);
                    }
                    else
                    {
                        Assert.IsTrue(output.value.value == value.value);
                        Assert.IsTrue(output.value.value == value.value);
                    }
                }
            }

        }

        [Test]
        public async Task AsyncObjectDiskWriteRead()
        {
            using var session = fht.NewSession();

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                await session.UpsertAsync(ref key, ref value, Empty.Default);
            }

            var key1 = new MyKey { key = 1989 };
            var input = new MyInput();
            var readResult = await session.ReadAsync(ref key1, ref input, Empty.Default);
            var result = readResult.CompleteRead();
            Assert.IsTrue(result.Item1 == Status.OK);
            Assert.IsTrue(result.Item2.value.value == 1989);

            var key2 = new MyKey { key = 23 };
            readResult = await session.ReadAsync(ref key2, ref input, Empty.Default);
            result = readResult.CompleteRead();

            Assert.IsTrue(result.Item1 == Status.OK);
            Assert.IsTrue(result.Item2.value.value == 23);

            var key3 = new MyKey { key = 9999 };
            readResult = await session.ReadAsync(ref key3, ref input, Empty.Default);
            result = readResult.CompleteRead();

            Assert.IsTrue(result.Item1 == Status.NOTFOUND);

            // Update last 100 using RMW in memory
            for (int i = 1900; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                input = new MyInput { value = 1 };
                await session.RMWAsync(ref key, ref input, Empty.Default);

            }

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key = new MyKey { key = i };
                input = new MyInput { value = 1 };
                await session.RMWAsync(ref key, ref input, Empty.Default);
            }

            for (int i = 0; i < 2000; i++)
            {
                var output = new MyOutput();
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };

                readResult = await session.ReadAsync(ref key, ref input, Empty.Default);
                result = readResult.CompleteRead();
                Assert.IsTrue(result.Item1 == Status.OK);
                if (i < 100 || i >= 1900)
                    Assert.IsTrue(result.Item2.value.value == value.value + 1);
                else
                    Assert.IsTrue(result.Item2.value.value == value.value);
            }
        }
    }
}

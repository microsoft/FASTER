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
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlogscan.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlogscan.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete>
                (128, new MyFunctionsDelete(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
            fht.StartSession();
        }

        [TearDown]
        public void TearDown()
        {
            fht.StopSession();
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
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                fht.Upsert(ref key1, ref value, 0, 0);
            }

            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new MyOutput();

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = fht.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
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
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                fht.Upsert(ref key1, ref value, 0, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                fht.Upsert(ref key1, ref value, 0, 0);
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

                var status = fht.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
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
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                fht.Upsert(ref key1, ref value, 0, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new MyKey { key = j };
                    fht.Delete(ref key1, 0, 0);
                }
            }

            var tail = fht.Log.TailAddress;
            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = fht.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
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
    }
}

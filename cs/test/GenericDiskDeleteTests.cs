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
    internal class GenericDiskDeleteTests
    {
        private FasterKV<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> fht;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericDiskDeleteTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericDiskDeleteTests.obj.log", deleteOnClose: true);

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
        public void GenericDiskDeleteTest1()
        {
            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                fht.Upsert(ref _key, ref _value, 0, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                if (fht.Read(ref key1, ref input, ref output, 0, 0) == Status.PENDING)
                {
                    fht.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(output.value.value == value.value);
                }
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                fht.Delete(ref key1, 0, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = fht.Read(ref key1, ref input, ref output, 1, 0);
                
                if (status == Status.PENDING)
                {
                    fht.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.NOTFOUND);
                }
            }

            
            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.SinglePageBuffering))
            {
                int val = 0;
                while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
                {
                    if (recordInfo.Tombstone)
                        val++;
                }
                Assert.IsTrue(totalRecords == val);
            }
            
        }

        [Test]
        public void GenericDiskDeleteTest2()
        {
            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                fht.Upsert(ref _key, ref _value, 0, 0);
            }

            var key100 = new MyKey { key = 100 };
            var value100 = new MyValue { value = 100 };
            var key200 = new MyKey { key = 200 };
            var value200 = new MyValue { value = 200 };

            fht.Delete(ref key100, 0, 0);

            var input = new MyInput { value = 1000 };
            var output = new MyOutput();
            var status = fht.Read(ref key100, ref input, ref output, 1, 0);
            Assert.IsTrue(status == Status.NOTFOUND);

            status = fht.Upsert(ref key100, ref value100, 0, 0);
            Assert.IsTrue(status == Status.OK);

            status = fht.Read(ref key100, ref input, ref output, 0, 0);
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.value.value == value100.value);

            fht.Delete(ref key100, 0, 0);
            fht.Delete(ref key200, 0, 0);

            // This RMW should create new initial value, since item is deleted
            status = fht.RMW(ref key200, ref input, 1, 0);
            Assert.IsTrue(status == Status.NOTFOUND);

            status = fht.Read(ref key200, ref input, ref output, 0, 0);
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.value.value == input.value);

            // Delete key 200 again
            fht.Delete(ref key200, 0, 0);

            // Eliminate all records from memory
            for (int i = 201; i < 2000; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                fht.Upsert(ref _key, ref _value, 0, 0);
            }
            status = fht.Read(ref key100, ref input, ref output, 1, 0);
            Assert.IsTrue(status == Status.PENDING);
            fht.CompletePending(true);

            // This RMW should create new initial value, since item is deleted
            status = fht.RMW(ref key200, ref input, 1, 0);
            Assert.IsTrue(status == Status.PENDING);
            fht.CompletePending(true);

            status = fht.Read(ref key200, ref input, ref output, 0, 0);
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.value.value == input.value);
        }
    }
}

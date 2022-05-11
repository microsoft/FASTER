// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class GenericDiskDeleteTests
    {
        private FasterKV<MyKey, MyValue> fht;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericDiskDeleteTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericDiskDeleteTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
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

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public void DiskDeleteBasicTest1()
        {
            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                session.Upsert(ref _key, ref _value, 0, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                if (session.Read(ref key1, ref input, ref output, 0, 0).IsPending)
                {
                    session.CompletePending(true);
                }
                else
                {
                    Assert.AreEqual(value.value, output.value.value);
                }
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                session.Delete(ref key1, 0, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };

                var status = session.Read(ref key1, ref input, ref output, 1, 0);
                
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, _) = GetSinglePendingResult(outputs);
                }
                Assert.IsFalse(status.Found);
            }


            using var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.SinglePageBuffering);
            int val = 0;
            while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
            {
                if (recordInfo.Tombstone)
                    val++;
            }
            Assert.AreEqual(val, totalRecords);
        }


        [Test]
        [Category("FasterKV")]
        public void DiskDeleteBasicTest2()
        {
            const int totalRecords = 2000;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                session.Upsert(ref _key, ref _value, 0, 0);
            }

            var key100 = new MyKey { key = 100 };
            var value100 = new MyValue { value = 100 };
            var key200 = new MyKey { key = 200 };

            session.Delete(ref key100, 0, 0);

            var input = new MyInput { value = 1000 };
            var output = new MyOutput();
            var status = session.Read(ref key100, ref input, ref output, 1, 0);
            Assert.IsFalse(status.Found, status.ToString());

            status = session.Upsert(ref key100, ref value100, 0, 0);
            Assert.IsTrue(!status.Found, status.ToString());

            status = session.Read(ref key100, ref input, ref output, 0, 0);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(value100.value, output.value.value);

            session.Delete(ref key100, 0, 0);
            session.Delete(ref key200, 0, 0);

            // This RMW should create new initial value, since item is deleted
            status = session.RMW(ref key200, ref input, 1, 0);
            Assert.IsFalse(status.Found);

            status = session.Read(ref key200, ref input, ref output, 0, 0);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(input.value, output.value.value);

            // Delete key 200 again
            session.Delete(ref key200, 0, 0);

            // Eliminate all records from memory
            for (int i = 201; i < 2000; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                session.Upsert(ref _key, ref _value, 0, 0);
            }
            status = session.Read(ref key100, ref input, ref output, 1, 0);
            Assert.IsTrue(status.IsPending);
            session.CompletePending(true);

            // This RMW should create new initial value, since item is deleted
            status = session.RMW(ref key200, ref input, 1, 0);
            Assert.IsTrue(status.IsPending);
            session.CompletePending(true);

            status = session.Read(ref key200, ref input, ref output, 0, 0);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(input.value, output.value.value);
        }
    }
}

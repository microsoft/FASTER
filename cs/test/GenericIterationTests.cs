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
    internal class GenericIterationTests
    {
        private FasterKV<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> fht;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericIterationTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericIterationTests.obj.log", deleteOnClose: true);

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
            log.Dispose();
            objlog.Dispose();
        }

        [Test]
        public void GenericIterationTest1()
        {
            using var session = fht.NewSession();

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            int count = 0;
            var iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().value == iter.GetKey().key);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);


            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 2 * i };
                session.Upsert(ref key1, ref value, 0, 1);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().value == iter.GetKey().key * 2);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);


            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Delete(ref key1, 0, 0);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords / 2);


            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = 3 * i };
                session.Upsert(ref key1, ref value, 0, 1);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().value == iter.GetKey().key * 3);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

        }
    }
}

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
    internal class ObjectReadCacheTests
    {
        private FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions> fht;
        private IDevice log, objlog;
        
        [SetUp]
        public void Setup()
        {
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 10 };
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog2.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog2.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions>
                (128, new MyFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings },
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
        }

        [Test]
        public void ObjectDiskWriteReadCache()
        {
            MyInput input = default(MyInput);

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                fht.Upsert(ref key, ref value, Empty.Default, 0);
            }
            fht.CompletePending(true);

            // Evict all records from main memory of hybrid log
            fht.ShiftHeadAddress(fht.LogTailAddress, true);

            // Read 100 keys - all should be served from disk, populating the read cache
            for (int i = 0; i < 100; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.PENDING);
                fht.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 0; i < 100; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(output.value.value == value.value);
            }

            // Evict the read cache entirely
            fht.ReadCache.ShiftHeadAddress(fht.ReadCache.GetTailAddress());

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 0; i < 100; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.PENDING);
                fht.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 0; i < 100; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(output.value.value == value.value);
            }


            // Upsert to overwrite the read cache
            for (int i = 0; i < 50; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            /*
            // RMW to overwrite the read cache
            for (int i = 50; i < 100; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 0 };
                var status = fht.RMW(ref key1, ref input, Empty.Default, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
            }*/

        }
    }
}

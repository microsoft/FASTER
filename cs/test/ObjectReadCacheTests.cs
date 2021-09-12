// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class ObjectReadCacheTests
    {
        private FasterKV<MyKey, MyValue> fht;
        private IDevice log, objlog;
        
        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 10 };
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/ObjectReadCacheTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/ObjectReadCacheTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
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
        public void ObjectDiskWriteReadCache()
        {
            using var session = fht.NewSession(new MyFunctions());

            MyInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Evict all records from main memory of hybrid log
            fht.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            fht.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }


            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 1 };
                var status = session.RMW(ref key1, ref input, Empty.Default, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
            }

            // Read the 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }
        }

        [Test]
        [Category("FasterKV")]
        public void ObjectDiskWriteReadCache2()
        {
            using var session = fht.NewSession(new MyFunctions());

            MyInput input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Dispose the hybrid log from memory entirely
            fht.Log.DisposeFromMemory();

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }

            // Evict the read cache entirely
            fht.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                MyOutput output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(value.value, output.value.value);
            }
        }
    }
}

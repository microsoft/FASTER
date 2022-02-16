// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.ReadCacheTests
{
    [TestFixture]
    internal class NativeReadCacheTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 10 };
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/NativeReadCacheTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (1L<<20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings });
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void NativeDiskWriteReadCache()
        {
            using var session = fht.NewSession(new Functions());

            InputStruct input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Evict all records from main memory of hybrid log
            fht.Log.FlushAndEvict(true);

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsFound);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Evict the read cache entirely
            fht.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsFound);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
            
            // Upsert to overwrite the read cache
            for (int i = 1900; i < 1950; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i + 1, vfield2 = i + 2 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            // RMW to overwrite the read cache
            for (int i = 1950; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = 1, ifield2 = 1 };
                var status = session.RMW(ref key1, ref input, ref output, Empty.Default, 0);
                if (status.IsPending)
                {
                    session.CompletePending(true);
                }
                else
                {
                    Assert.AreEqual(i + 1, output.value.vfield1);
                    Assert.AreEqual(i + 2, output.value.vfield2);
                }
            }

            // Read 100 keys
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i + 1, vfield2 = i + 2 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsFound);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }

        [Test]
        [Category("FasterKV")]
        public void NativeDiskWriteReadCache2()
        {
            using var session = fht.NewSession(new Functions());

            InputStruct input = default;

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Dispose the hybrid log from memory entirely
            fht.Log.DisposeFromMemory();

            // Read 2000 keys - all should be served from disk, populating and evicting the read cache FIFO
            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read last 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsFound);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Evict the read cache entirely
            fht.ReadCache.FlushAndEvict(true);

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsPending);
                session.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 1900; i < 2000; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status.IsFound);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }
    }
}

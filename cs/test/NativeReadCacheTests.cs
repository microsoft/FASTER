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
    internal class NativeReadCacheTests
    {
        private FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 10 };
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hybridlog_native.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions>
                (1L<<20, new Functions(), new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings });
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
        public void NativeDiskWriteReadCache()
        {
            InputStruct input = default(InputStruct);

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            fht.CompletePending(true);

            // Evict all records from main memory of hybrid log
            fht.ShiftHeadAddress(fht.LogTailAddress, true);

            // Read 100 keys - all should be served from disk, populating the read cache
            for (int i = 0; i < 100; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.PENDING);
                fht.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 0; i < 100; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }

            // Evict the read cache entirely
            fht.ReadCache.ShiftHeadAddress(fht.ReadCache.GetTailAddress());

            // Read 100 keys - all should be served from disk, populating cache
            for (int i = 0; i < 100; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.PENDING);
                fht.CompletePending(true);
            }

            // Read 100 keys - all should be served from cache
            for (int i = 0; i < 100; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }

            
            // Upsert to overwrite the read cache
            for (int i = 0; i < 100; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }
        }
    }
}

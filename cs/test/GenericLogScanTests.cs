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
    internal class GenericFASTERScanTests
    {
        private FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions> fht;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlogscan.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlogscan.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions>
                (128, new MyFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 9 },
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
        public void GenericDiskWriteScan()
        {
            const int totalRecords = 2000;
            var start = fht.LogTailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                fht.Upsert(ref _key, ref _value, Empty.Default, 0);
                if (i % 100 == 0) fht.ShiftHeadAddress(fht.LogTailAddress, true);
            }
            fht.ShiftHeadAddress(fht.LogTailAddress, true);
            var iter = fht.LogScan(start, fht.LogTailAddress, ScanBufferingMode.SinglePageBuffering);

            int val = 0;
            while (iter.GetNext(out MyKey key, out MyValue value))
            {
                Assert.IsTrue(key.key == val);
                Assert.IsTrue(value.value == val);
                val++;
            }
            Assert.IsTrue(totalRecords == val);

            iter = fht.LogScan(start, fht.LogTailAddress, ScanBufferingMode.DoublePageBuffering);

            val = 0;
            while (iter.GetNext(out MyKey key, out MyValue value))
            {
                Assert.IsTrue(key.key == val);
                Assert.IsTrue(value.value == val);
                val++;
            }
            Assert.IsTrue(totalRecords == val);

        }
    }
}

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
        const int totalRecords = 2000;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericFASTERScanTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\GenericFASTERScanTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions>
                (128, new MyFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 9 },
                checkpointSettings: new CheckpointSettings { CheckPointType = CheckpointType.FoldOver },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Close();
            objlog.Close();
        }


        [Test]
        public void GenericDiskWriteScan()
        {
            using var session = fht.NewSession();

            var s = fht.Log.Subscribe(new LogObserver());

            var start = fht.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                session.Upsert(ref _key, ref _value, Empty.Default, 0);
                if (i % 100 == 0) fht.Log.FlushAndEvict(true);
            }
            fht.Log.FlushAndEvict(true);
            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.SinglePageBuffering))
            {

                int val = 0;
                while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
                {
                    Assert.IsTrue(key.key == val);
                    Assert.IsTrue(value.value == val);
                    val++;
                }
                Assert.IsTrue(totalRecords == val);
            }

            using (var iter = fht.Log.Scan(start, fht.Log.TailAddress, ScanBufferingMode.DoublePageBuffering))
            {
                int val = 0;
                while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
                {
                    Assert.IsTrue(key.key == val);
                    Assert.IsTrue(value.value == val);
                    val++;
                }
                Assert.IsTrue(totalRecords == val);
            }

            s.Dispose();
        }

        class LogObserver : IObserver<IFasterScanIterator<MyKey, MyValue>>
        {
            int val = 0;

            public void OnCompleted()
            {
                Assert.IsTrue(val == totalRecords);
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(IFasterScanIterator<MyKey, MyValue> iter)
            {
                while (iter.GetNext(out _, out MyKey key, out MyValue value))
                {
                    Assert.IsTrue(key.key == val);
                    Assert.IsTrue(value.value == val);
                    val++;
                }
                iter.Dispose();
            }
        }
    }
}

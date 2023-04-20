// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;
using FASTER.devices;

namespace FASTER.test
{
    [TestFixture]
    internal class BasicStorageFASTERTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;

        [Test]
        [Category("FasterKV")]
        public void LocalStorageWriteRead()
        {
            TestDeviceWriteRead(Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BasicDiskFASTERTests.log", deleteOnClose: true));
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void PageBlobWriteRead()
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            TestDeviceWriteRead(new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "BasicDiskFASTERTests", logger: TestUtils.TestLoggerFactory.CreateLogger("asd")));
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void PageBlobWriteReadWithLease()
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            TestDeviceWriteRead(new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "BasicDiskFASTERTests",null,true,true, logger: TestUtils.TestLoggerFactory.CreateLogger("asd")));
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void TieredWriteRead()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            IDevice tested;
            IDevice localDevice = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BasicDiskFASTERTests.log", deleteOnClose: true, capacity: 1 << 30);
            if (TestUtils.IsRunningAzureTests)
            {
                IDevice cloudDevice = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "BasicDiskFASTERTests", logger: TestUtils.TestLoggerFactory.CreateLogger("asd"));
                tested = new TieredStorageDevice(1, localDevice, cloudDevice);
            }
            else
            {
                // If no Azure is enabled, just use another disk
                IDevice localDevice2 = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BasicDiskFASTERTests2.log", deleteOnClose: true, capacity: 1 << 30);
                tested = new TieredStorageDevice(1, localDevice, localDevice2);

            }
            TestDeviceWriteRead(tested);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ShardedWriteRead()
        {
            IDevice localDevice1 = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BasicDiskFASTERTests1.log", deleteOnClose: true, capacity: 1 << 30);
            IDevice localDevice2 = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BasicDiskFASTERTests2.log", deleteOnClose: true, capacity: 1 << 30);
            var device = new ShardedStorageDevice(new UniformPartitionScheme(512, localDevice1, localDevice2));
            TestDeviceWriteRead(device);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void OmitSegmentIdTest([Values] TestUtils.DeviceType deviceType)
        {
            var filename = TestUtils.MethodTestDir + "/test.log";
            var omit = false;
            for (var ii = 0; ii < 2; ++ii) 
            { 
                using IDevice device = TestUtils.CreateTestDevice(deviceType, filename, omitSegmentIdFromFilename: omit);
                var storageBase = (StorageDeviceBase)device;
                var segmentFilename = storageBase.GetSegmentFilename(filename, 0);
                if (omit)
                    Assert.AreEqual(filename, segmentFilename);
                else
                    Assert.AreEqual(filename + ".0", segmentFilename);
                omit = true;
            }
        }

        void TestDeviceWriteRead(IDevice log)
        {
            fht = new FasterKV<KeyStruct, ValueStruct>
                       (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10 });
            
            var session = fht.NewSession(new Functions());

            InputStruct input = default;

            for (int i = 0; i < 700; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = 1, ifield2 = 1 };
                var status = session.RMW(ref key1, ref input, Empty.Default, 0);
                if (status.IsPending)
                    session.CompletePending(true);
            }


            for (int i = 0; i < 700; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    session.CompletePending(true);
                }
                else
                {
                    if (i < 100)
                    {
                        Assert.AreEqual(value.vfield1 + 1, output.value.vfield1);
                        Assert.AreEqual(value.vfield2 + 1, output.value.vfield2);
                    }
                    else
                    {
                        Assert.AreEqual(value.vfield1, output.value.vfield1);
                        Assert.AreEqual(value.vfield2, output.value.vfield2);
                    }
                }
            }

            session.Dispose();
            fht.Dispose();
            fht = null;
            log.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}

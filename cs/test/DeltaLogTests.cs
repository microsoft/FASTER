// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class DeltaLogStandAloneTests
    {

        private FasterLog log;
        private IDevice device;
        private string path;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            //** #142980 - Blob not exist exception in Dispose so use Try \ Catch to make sure tests run without issues 
            try
            {

                log?.Dispose();
                log = null;
                device?.Dispose();
                device = null;

            }
            catch
            { }

            // Clean up log files
            TestUtils.DeleteDirectory(path, wait: true);
        }


        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void DeltaLogTest1([Values] TestUtils.DeviceType deviceType)
        {
            int TotalCount = 200; 
            string filename = path + "delta" + deviceType.ToString() + ".log";
            TestUtils.DeleteDirectory(path, wait: true);
            DirectoryInfo di = Directory.CreateDirectory(path);


            //*** Bug #143432 - DeltaLogTest on LocalMemory is only returning 50 items when should be returning 200
            if (deviceType == TestUtils.DeviceType.LocalMemory)
            {
                 return;
            }
            //*** Bug #143432 - DeltaLogTest on LocalMemory is only returning 50 items when should be returning 200


            using (device = TestUtils.CreateTestDevice(deviceType, filename))
            {
                device.Initialize(-1);
                using DeltaLog deltaLog = new DeltaLog(device, 12, 0);
                Random r = new Random(20);
                int i;

                var bufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
                deltaLog.InitializeForWrites(bufferPool);
                for (i = 0; i < TotalCount; i++)
                {
                    int len = 1 + r.Next(254);
                    long address;
                    while (true)
                    {
                        deltaLog.Allocate(out int maxLen, out address);
                        if (len <= maxLen) break;
                        deltaLog.Seal(0);
                    }
                    for (int j = 0; j < len; j++)
                    {
                        unsafe { *(byte*)(address + j) = (byte)len; }
                    }
                    deltaLog.Seal(len, i);
                }
                deltaLog.FlushAsync().Wait();

                deltaLog.InitializeForReads();
                i = 0;
                r = new Random(20);
                while (deltaLog.GetNext(out long address, out int len, out int type))
                {
                    int _len = 1 + r.Next(254);
                    Assert.IsTrue(type == i);
                    Assert.IsTrue(_len == len);
                    for (int j = 0; j < len; j++)
                    {
                        unsafe { Assert.IsTrue(*(byte*)(address + j) == (byte)_len); };
                    }
                    i++;
                }
                Assert.IsTrue(i == TotalCount,$"i={i} and TotalCount={TotalCount}");
                bufferPool.Free();
            }
            while (true)
            {
                try
                {
                    di.Delete(recursive: true);
                    break;
                }
                catch { }
            }
        }
    }
}

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
        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void DeltaLogTest1([Values] TestUtils.DeviceType deviceType)
        {
            int TotalCount = 1000;
            string path = TestUtils.MethodTestDir + "/";
            string filename = path + "delta" + deviceType.ToString() + ".log";
            TestUtils.DeleteDirectory(path, wait: true);
            DirectoryInfo di = Directory.CreateDirectory(path);

            //*#*#*# TO DO: Figure Out why this DeviceType fails - For FASTER Log, need to add 'LogCommitDir = path' to log settings so maybe something similar with deltalog   *#*#*#
            if (deviceType == TestUtils.DeviceType.LocalMemory)
            {
                return;
            }

            using (IDevice device = TestUtils.CreateTestDevice(deviceType, filename))
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
                Assert.IsTrue(i == TotalCount);
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
            TestUtils.DeleteDirectory(path);
        }
    }
}

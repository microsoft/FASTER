// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.devices;
using NUnit.Framework;

namespace FASTER.test
{

    [TestFixture]
    internal class DeviceFasterLogTests
    {
        const int entryLength = 100;
        const int numEntries = 100000;
        private FasterLog log;
        public const string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
        public const string TEST_CONTAINER = "test";

        private IDevice device;
        private string commitPath;

        [Test]
        public void PageBlobFasterLogTest1([Values] LogChecksumType logChecksum)
        {
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                var device = new AzureStorageDevice(EMULATED_STORAGE_STRING, $"{TEST_CONTAINER}", "PageBlobFasterLogTest1", "fasterlog.log", deleteOnClose: true);
                var checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(EMULATED_STORAGE_STRING),
                    new DefaultCheckpointNamingScheme($"{TEST_CONTAINER}/PageBlobFasterLogTest1"));
                FasterLogTest1(logChecksum, device, checkpointManager);
                device.Close();
                checkpointManager.PurgeAll();
                checkpointManager.Dispose();
            }
        }

        private void FasterLogTest1(LogChecksumType logChecksum, IDevice device, ILogCommitManager logCommitManager)
        {
            log = new FasterLog(new FasterLogSettings { PageSizeBits = 20, SegmentSizeBits = 20, LogDevice = device, LogChecksum = logChecksum, LogCommitManager = logCommitManager });

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }
            log.Commit(true);

            using (var iter = log.Scan(0, long.MaxValue))
            {
                int count = 0;
                while (iter.GetNext(out byte[] result, out int length, out long currentAddress))
                {
                    count++;
                    Assert.IsTrue(result.SequenceEqual(entry));
                    if (count % 100 == 0)
                        log.TruncateUntil(iter.NextAddress);
                }
                Assert.IsTrue(count == numEntries);
            }

            log.Dispose();
        }
    }
}

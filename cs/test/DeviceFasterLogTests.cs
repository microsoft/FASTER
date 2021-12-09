// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Linq;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.devices;
using NUnit.Framework;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;

namespace FASTER.test
{
    [TestFixture]
    internal class DeviceFasterLogTests
    {
        const int entryLength = 100;
        const int numEntries = 1000;
        private FasterLog log;
        static readonly byte[] entry = new byte[100];

        [Test]
        [Category("FasterLog")]
        public async ValueTask PageBlobFasterLogTest1([Values] LogChecksumType logChecksum, [Values]FasterLogTestBase.IteratorType iteratorType)
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            var device = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, $"{TestUtils.AzureTestContainer}", TestUtils.AzureTestDirectory, "fasterlog.log", deleteOnClose: true);
            var checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                new DefaultCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            await FasterLogTest1(logChecksum, device, checkpointManager, iteratorType);
            device.Dispose();
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }

        [Test]
        [Category("FasterLog")]
        public async ValueTask PageBlobFasterLogTestWithLease([Values] LogChecksumType logChecksum, [Values] FasterLogTestBase.IteratorType iteratorType)
        {
            // Set up the blob manager so can set lease to it
            TestUtils.IgnoreIfNotRunningAzureTests();
            CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            var cloudBlobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer blobContainer = cloudBlobClient.GetContainerReference("test-container");
            blobContainer.CreateIfNotExists();
            var mycloudBlobDir = blobContainer.GetDirectoryReference(@"BlobManager/MyLeaseTest1");

            var blobMgr = new DefaultBlobManager(true, mycloudBlobDir);
            var device = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, $"{TestUtils.AzureTestContainer}", TestUtils.AzureTestDirectory, "fasterlogLease.log", deleteOnClose: true, underLease: true, blobManager: blobMgr);

            var checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                new DefaultCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            await FasterLogTest1(logChecksum, device, checkpointManager, iteratorType);
            device.Dispose();
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
            blobContainer.Delete();
        }


        [Test]
        [Category("FasterLog")]
        public void BasicHighLatencyDeviceTest()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Create devices \ log for test for in memory device
            using var device = new LocalMemoryDevice(1L << 28, 1L << 25, 2, latencyMs: 20);
            using var LocalMemorylog = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 80, MemorySizeBits = 20, GetMemory = null, SegmentSizeBits = 80, MutableFraction = 0.2, LogCommitManager = null });

            int entryLength = 10;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
                LocalMemorylog.Enqueue(entry);
            }

            // Commit to the log
            LocalMemorylog.Commit(true);

            // Read the log just to verify was actually committed
            int currentEntry = 0;
            using var iter = LocalMemorylog.Scan(0, 100_000_000);
            while (iter.GetNext(out byte[] result, out _, out _))
            {
                Assert.IsTrue(result[currentEntry] == currentEntry, "Fail - Result[" + currentEntry.ToString() + "]: is not same as " + currentEntry.ToString());
                currentEntry++;
            }
        }

        private async ValueTask FasterLogTest1(LogChecksumType logChecksum, IDevice device, ILogCommitManager logCommitManager, FasterLogTestBase.IteratorType iteratorType)
        {
            var logSettings = new FasterLogSettings { PageSizeBits = 20, SegmentSizeBits = 20, LogDevice = device, LogChecksum = logChecksum, LogCommitManager = logCommitManager };
            log = FasterLogTestBase.IsAsync(iteratorType) ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }
            log.Commit(true);

            // MoveNextAsync() would hang at TailAddress, waiting for more entries (that we don't add).
            // Note: If this happens and the test has to be canceled, there may be a leftover blob from the log.Commit(), because
            // the log device isn't Dispose()d; the symptom is currently a numeric string format error in DefaultCheckpointNamingScheme.
            using (var iter = log.Scan(0, log.TailAddress))
            {
                var counter = new FasterLogTestBase.Counter(log);

                switch (iteratorType)
                {
                    case FasterLogTestBase.IteratorType.AsyncByteVector:
                        await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable())
                        {
                            Assert.IsTrue(result.SequenceEqual(entry));
                            counter.IncrementAndMaybeTruncateUntil(nextAddress);
                        }
                        break;
                    case FasterLogTestBase.IteratorType.AsyncMemoryOwner:
                        await foreach ((IMemoryOwner<byte> result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared))
                        {
                            Assert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                            result.Dispose();
                            counter.IncrementAndMaybeTruncateUntil(nextAddress);
                        }
                        break;
                    case FasterLogTestBase.IteratorType.Sync:
                        while (iter.GetNext(out byte[] result, out _, out _))
                        {
                            Assert.IsTrue(result.SequenceEqual(entry));
                            counter.IncrementAndMaybeTruncateUntil(iter.NextAddress);
                        }
                        break;
                    default:
                        Assert.Fail("Unknown IteratorType");
                        break;
                }
                Assert.IsTrue(counter.count == numEntries);
            }

            log.Dispose();
        }
    }
}

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
        const int numEntries = 100000;
        private FasterLog log;

        [Test]
        [Category("FasterLog")]
        public async ValueTask PageBlobFasterLogTest1([Values] LogChecksumType logChecksum, [Values]FasterLogTestBase.IteratorType iteratorType)
        {
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                var device = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, $"{TestUtils.AzureMethodTestContainer}", "checkpoints", "fasterlog.log", deleteOnClose: true);
                var checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                    new DefaultCheckpointNamingScheme($"{TestUtils.AzureMethodTestContainer}/checkpoints"));
                await FasterLogTest1(logChecksum, device, checkpointManager, iteratorType);
                device.Dispose();
                checkpointManager.PurgeAll();
                checkpointManager.Dispose();
            }
        }

        [Test]
        [Category("FasterLog")]
        public async ValueTask PageBlobFasterLogTestWithLease([Values] LogChecksumType logChecksum, [Values] FasterLogTestBase.IteratorType iteratorType)
        {
            // Need this environment variable set AND Azure Storage Emulator running
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                // Set up the blob manager so can set lease to it
                CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
                var cloudBlobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer blobContainer = cloudBlobClient.GetContainerReference("test-container");
                blobContainer.CreateIfNotExists();
                var mycloudBlobDir = blobContainer.GetDirectoryReference(@"BlobManager/MyLeaseTest1");

                var blobMgr = new DefaultBlobManager(true, mycloudBlobDir);
                var device = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, $"{TestUtils.AzureMethodTestContainer}", "checkpoints", "fasterlogLease.log", deleteOnClose: true, underLease: true, blobManager: blobMgr);

                var checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                    new DefaultCheckpointNamingScheme($"{TestUtils.AzureMethodTestContainer}/checkpoints"));
                await FasterLogTest1(logChecksum, device, checkpointManager, iteratorType);
                device.Dispose();
                checkpointManager.PurgeAll();
                checkpointManager.Dispose();
                blobContainer.Delete();
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

            using (var iter = log.Scan(0, long.MaxValue))
            {
                var counter = new FasterLogTestBase.Counter(log);

                switch (iteratorType)
                {
                    case FasterLogTestBase.IteratorType.AsyncByteVector:
                        await foreach ((byte[] result, _, _, long nextAddress) in iter.GetAsyncEnumerable())
                        {
                            Assert.IsTrue(result.SequenceEqual(entry));
                            counter.IncrementAndMaybeTruncateUntil(nextAddress);

                            // MoveNextAsync() would hang at TailAddress, waiting for more entries (that we don't add).
                            // Note: If this happens and the test has to be canceled, there may be a leftover blob from the log.Commit(), because
                            // the log device isn't Dispose()d; the symptom is currently a numeric string format error in DefaultCheckpointNamingScheme.
                            if (nextAddress == log.TailAddress)
                                break;
                        }
                        break;
                    case FasterLogTestBase.IteratorType.AsyncMemoryOwner:
                        await foreach ((IMemoryOwner<byte> result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared))
                        {
                            Assert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                            result.Dispose();
                            counter.IncrementAndMaybeTruncateUntil(nextAddress);

                            // MoveNextAsync() would hang at TailAddress, waiting for more entries (that we don't add).
                            // Note: If this happens and the test has to be canceled, there may be a leftover blob from the log.Commit(), because
                            // the log device isn't Dispose()d; the symptom is currently a numeric string format error in DefaultCheckpointNamingScheme.
                            if (nextAddress == log.TailAddress)
                                break;
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

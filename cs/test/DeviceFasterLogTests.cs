// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Linq;
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

        [Test]
        public async ValueTask PageBlobFasterLogTest1([Values] LogChecksumType logChecksum, [Values]FasterLogTests.IteratorType iteratorType)
        {
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                var device = new AzureStorageDevice(EMULATED_STORAGE_STRING, $"{TEST_CONTAINER}", "PageBlobFasterLogTest1", "fasterlog.log", deleteOnClose: true);
                var checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(EMULATED_STORAGE_STRING),
                    new DefaultCheckpointNamingScheme($"{TEST_CONTAINER}/PageBlobFasterLogTest1"));
                await FasterLogTest1(logChecksum, device, checkpointManager, iteratorType);
                device.Dispose();
                checkpointManager.PurgeAll();
                checkpointManager.Dispose();
            }
        }

        private async ValueTask FasterLogTest1(LogChecksumType logChecksum, IDevice device, ILogCommitManager logCommitManager, FasterLogTests.IteratorType iteratorType)
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
                var counter = new FasterLogTests.Counter(log);

                switch (iteratorType)
                {
                    case FasterLogTests.IteratorType.AsyncByteVector:
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
                    case FasterLogTests.IteratorType.AsyncMemoryOwner:
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
                    case FasterLogTests.IteratorType.Sync:
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

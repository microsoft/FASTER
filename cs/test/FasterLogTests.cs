// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class FasterLogTests
    {
        const int entryLength = 100;
        const int numEntries = 1000000;
        private FasterLog log;
        private IDevice device;
        private string commitPath;
        private DeviceLogCommitCheckpointManager manager;

        [SetUp]
        public void Setup()
        {
            commitPath = TestContext.CurrentContext.TestDirectory + "/" + TestContext.CurrentContext.Test.Name +  "/";

            if (Directory.Exists(commitPath))
                TestUtils.DeleteDirectory(commitPath);

            device = Devices.CreateLogDevice(commitPath + "fasterlog.log", deleteOnClose: true);
            manager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(deleteOnClose: true), new DefaultCheckpointNamingScheme(commitPath));
        }

        [TearDown]
        public void TearDown()
        {
            if (log is { })
                log.Dispose();
            manager.Dispose();
            device.Dispose();

            if (Directory.Exists(commitPath))
                TestUtils.DeleteDirectory(commitPath);
        }

        internal class Counter
        {
            internal int count;
            private readonly FasterLog log;

            internal Counter(FasterLog fasterLog)
            {
                this.count = 0;
                this.log = fasterLog;
            }

            internal void IncrementAndMaybeTruncateUntil(long nextAddr)
            {
                this.count++;
                if (this.count % 100 == 0)
                    this.log.TruncateUntil(nextAddr);
            }

            public override string ToString() => $"{this.count}";
        }

        public enum IteratorType
        {
            AsyncByteVector,
            AsyncMemoryOwner,
            Sync
        }

        internal static bool IsAsync(IteratorType iterType) => iterType == IteratorType.AsyncByteVector || iterType == IteratorType.AsyncMemoryOwner;

        private async ValueTask AssertGetNext(IAsyncEnumerator<(byte[] entry, int entryLength, long currentAddress, long nextAddress)> asyncByteVectorIter,
                                              IAsyncEnumerator<(IMemoryOwner<byte> entry, int entryLength, long currentAddress, long nextAddress)> asyncMemoryOwnerIter,
                                              FasterLogScanIterator iter, byte[] expectedData = default, bool verifyAtEnd = false)
        {
            if (asyncByteVectorIter is {})
            {
                Assert.IsTrue(await asyncByteVectorIter.MoveNextAsync());
                if (expectedData is {})
                    Assert.IsTrue(asyncByteVectorIter.Current.entry.SequenceEqual(expectedData));

                // MoveNextAsync() would hang here waiting for more entries
                if (verifyAtEnd)
                    Assert.IsTrue(asyncByteVectorIter.Current.nextAddress == log.TailAddress);
                return;
            }

            if (asyncMemoryOwnerIter is {})
            {
                Assert.IsTrue(await asyncMemoryOwnerIter.MoveNextAsync());
                if (expectedData is {})
                    Assert.IsTrue(asyncMemoryOwnerIter.Current.entry.Memory.Span.ToArray().Take(expectedData.Length).SequenceEqual(expectedData));
                asyncMemoryOwnerIter.Current.entry.Dispose();

                // MoveNextAsync() would hang here waiting for more entries
                if (verifyAtEnd)
                    Assert.IsTrue(asyncMemoryOwnerIter.Current.nextAddress == log.TailAddress);
                return;
            }

            Assert.IsTrue(iter.GetNext(out byte[] result, out _, out _));
            if (expectedData is {})
                Assert.IsTrue(result.SequenceEqual(expectedData));
            if (verifyAtEnd)
                Assert.IsFalse(iter.GetNext(out _, out _, out _));
        }

        [Test]
        public async ValueTask FasterLogTest1([Values]LogChecksumType logChecksum, [Values]IteratorType iteratorType)
        {
            var logSettings = new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager };
            log = IsAsync(iteratorType) ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }
            log.Commit(true);

            // If endAddress > log.TailAddress then GetAsyncEnumerable() will wait until more entries are added.
            var endAddress = IsAsync(iteratorType) ? log.TailAddress : long.MaxValue;
            using var iter = log.Scan(0, endAddress);
            var counter = new Counter(log);
            switch (iteratorType)
            {
                case IteratorType.AsyncByteVector:
                    await foreach ((byte[] result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable())
                    {
                        Assert.IsTrue(result.SequenceEqual(entry));
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }
                    break;
                case IteratorType.AsyncMemoryOwner:
                    await foreach ((IMemoryOwner<byte> result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable(MemoryPool<byte>.Shared))
                    {
                        Assert.IsTrue(result.Memory.Span.ToArray().Take(entry.Length).SequenceEqual(entry));
                        result.Dispose();
                        counter.IncrementAndMaybeTruncateUntil(nextAddress);
                    }
                    break;
                case IteratorType.Sync:
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

        [Test]
        public async ValueTask FasterLogTest2([Values]LogChecksumType logChecksum, [Values]IteratorType iteratorType)
        {
            var logSettings = new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager };
            log = IsAsync(iteratorType) ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            const int dataLength = 10000;
            byte[] data1 = new byte[dataLength];
            for (int i = 0; i < dataLength; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;
                var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator() : default;
                int i = 0;
                while (i++ < 500)
                {
                    var waitingReader = iter.WaitAsync();
                    Assert.IsTrue(!waitingReader.IsCompleted);

                    while (!log.TryEnqueue(data1, out _)) ;

                    // We might have auto-committed at page boundary
                    // Ensure we don't find new entry in iterator
                    while (waitingReader.IsCompleted)
                    {
                        var _next = iter.GetNext(out _, out _, out _);
                        Assert.IsFalse(_next);
                        waitingReader = iter.WaitAsync();
                    }
                    Assert.IsFalse(waitingReader.IsCompleted);

                    await log.CommitAsync();
                    while (!waitingReader.IsCompleted) ;
                    Assert.IsTrue(waitingReader.IsCompleted);

                    await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1, verifyAtEnd :true);
                }
            }
        }

        [Test]
        public async ValueTask FasterLogTest3([Values]LogChecksumType logChecksum, [Values]IteratorType iteratorType)
        {
            var logSettings = new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager };
            log = IsAsync(iteratorType) ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering);
            var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;
            var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator() : default;

            var appendResult = log.TryEnqueue(data1, out _);
            Assert.IsTrue(appendResult);
            await log.CommitAsync();
            await iter.WaitAsync();

            await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1);

            // This will fail due to page overflow, leaving a "hole"
            appendResult = log.TryEnqueue(data1, out _);
            Assert.IsFalse(appendResult);
            await iter.WaitAsync();

            async Task retryAppend(bool waitTaskIsCompleted)
            {
                Assert.IsFalse(waitTaskIsCompleted);
                Assert.IsTrue(log.TryEnqueue(data1, out _));
                await log.CommitAsync();
            }

            switch (iteratorType)
            {
                case IteratorType.Sync:
                    // Should read the "hole" and return false
                    Assert.IsFalse(iter.GetNext(out _, out _, out _));

                    // Should wait for next item
                    var task = iter.WaitAsync();
                    await retryAppend(task.IsCompleted);
                    await task;

                    // Now the data is available.
                    Assert.IsTrue(iter.GetNext(out _, out _, out _));
                    break;
                case IteratorType.AsyncByteVector:
                    {
                        // Because we have a hole, awaiting MoveNextAsync would hang; instead, hold onto the task that results from WaitAsync() inside MoveNextAsync().
                        var moveNextTask = asyncByteVectorIter.MoveNextAsync();
                        await retryAppend(moveNextTask.IsCompleted);

                        // Now the data is available.
                        Assert.IsTrue(await moveNextTask);
                    }
                    break;
                case IteratorType.AsyncMemoryOwner:
                    {
                        // Because we have a hole, awaiting MoveNextAsync would hang; instead, hold onto the task that results from WaitAsync() inside MoveNextAsync().
                        var moveNextTask = asyncMemoryOwnerIter.MoveNextAsync();
                        await retryAppend(moveNextTask.IsCompleted);

                        // Now the data is available, and must be disposed.
                        Assert.IsTrue(await moveNextTask);
                        asyncMemoryOwnerIter.Current.entry.Dispose();
                    }
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }
        }

        [Test]
        public async ValueTask FasterLogTest4([Values]LogChecksumType logChecksum, [Values]IteratorType iteratorType)
        {
            var logSettings = new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager };
            log = IsAsync(iteratorType) ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            byte[] data1 = new byte[100];
            for (int i = 0; i < 100; i++) data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
            {
                log.Enqueue(data1);
            }

            Assert.IsTrue(log.CommittedUntilAddress == log.BeginAddress);
            await log.CommitAsync();

            Assert.IsTrue(log.CommittedUntilAddress == log.TailAddress);
            Assert.IsTrue(log.CommittedBeginAddress == log.BeginAddress);

            using var iter = log.Scan(0, long.MaxValue);
            var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;
            var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator() : default;

            await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1);

            log.TruncateUntil(iter.NextAddress);

            Assert.IsTrue(log.CommittedUntilAddress == log.TailAddress);
            Assert.IsTrue(log.CommittedBeginAddress < log.BeginAddress);
            Assert.IsTrue(iter.NextAddress == log.BeginAddress);

            await log.CommitAsync();

            Assert.IsTrue(log.CommittedUntilAddress == log.TailAddress);
            Assert.IsTrue(log.CommittedBeginAddress == log.BeginAddress);
        }

        [Test]
        public async ValueTask FasterLogTest5([Values]LogChecksumType logChecksum)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 16, LogChecksum = logChecksum, LogCommitManager = manager });

            int headerSize = logChecksum == LogChecksumType.None ? 4 : 12;
            bool _disposed = false;
            var commit = new Thread(() => { while (!_disposed) { log.Commit(true); Thread.Sleep(1); } });

            commit.Start();

            // 65536=page size|headerSize|64=log header
            await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize - 64]);

            // 65536=page size|headerSize
            await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize]);

            // 65536=page size|headerSize
            await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize]);

            // 65536=page size|headerSize
            await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize]);

            // 65536=page size|headerSize
            await log.EnqueueAndWaitForCommitAsync(new byte[65536 - headerSize]);

            _disposed = true;

            commit.Join();
        }

        [Test]
        public async ValueTask FasterLogTest6([Values] LogChecksumType logChecksum, [Values]IteratorType iteratorType)
        {
            var logSettings = new FasterLogSettings { LogDevice = device, MemorySizeBits = 20, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager };
            log = IsAsync(iteratorType) ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            byte[] data1 = new byte[1000];
            for (int i = 0; i < 100; i++) data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
            {
                log.Enqueue(data1);
            }
            log.RefreshUncommitted();
            Assert.IsTrue(log.SafeTailAddress == log.TailAddress);

            Assert.IsTrue(log.CommittedUntilAddress < log.SafeTailAddress);

            using var iter = log.Scan(0, long.MaxValue, scanUncommitted: true);
            var asyncByteVectorIter = iteratorType == IteratorType.AsyncByteVector ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;
            var asyncMemoryOwnerIter = iteratorType == IteratorType.AsyncMemoryOwner ? iter.GetAsyncEnumerable(MemoryPool<byte>.Shared).GetAsyncEnumerator() : default;

            switch (iteratorType)
            {
                case IteratorType.Sync:
                    while (iter.GetNext(out _, out _, out _))
                        log.TruncateUntil(iter.NextAddress);
                    Assert.IsTrue(iter.NextAddress == log.SafeTailAddress);
                    break;
                case IteratorType.AsyncByteVector:
                    {
                        while (await asyncByteVectorIter.MoveNextAsync() && asyncByteVectorIter.Current.nextAddress != log.SafeTailAddress)
                            log.TruncateUntil(asyncByteVectorIter.Current.nextAddress);
                    }
                    break;
                case IteratorType.AsyncMemoryOwner:
                    {
                        while (await asyncMemoryOwnerIter.MoveNextAsync())
                        {
                            log.TruncateUntil(asyncMemoryOwnerIter.Current.nextAddress);
                            asyncMemoryOwnerIter.Current.entry.Dispose();
                            if (asyncMemoryOwnerIter.Current.nextAddress == log.SafeTailAddress)
                                break;
                        }
                    }
                    break;
                default:
                    Assert.Fail("Unknown IteratorType");
                    break;
            }

            // Enqueue data but do not make it visible
            log.Enqueue(data1);

            // Do this only for sync; MoveNextAsync() would hang here waiting for more entries.
            if (!IsAsync(iteratorType))
                Assert.IsFalse(iter.GetNext(out _, out _, out _));

            // Make the data visible
            log.RefreshUncommitted();

            await AssertGetNext(asyncByteVectorIter, asyncMemoryOwnerIter, iter, data1, verifyAtEnd: true);
        }
    }
}

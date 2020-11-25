// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
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
            commitPath = TestContext.CurrentContext.TestDirectory + "\\" + TestContext.CurrentContext.Test.Name +  "\\";

            if (Directory.Exists(commitPath))
                DeleteDirectory(commitPath);

            device = Devices.CreateLogDevice(commitPath + "fasterlog.log", deleteOnClose: true);
            manager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(deleteOnClose: true), new DefaultCheckpointNamingScheme(commitPath));
        }

        [TearDown]
        public void TearDown()
        {
            manager.Dispose();
            device.Dispose();

            if (Directory.Exists(commitPath))
                DeleteDirectory(commitPath);
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

            internal void Increment(long nextAddr)
            {
                this.count++;
                if (this.count % 100 == 0)
                    this.log.TruncateUntil(nextAddr);
            }

            public override string ToString() => $"{this.count}";
        }


        [Test]
        public async ValueTask FasterLogTest1([Values]LogChecksumType logChecksum, [Values]bool isAsync)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager });

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }
            log.Commit(true);

            // If endAddress > log.TailAddress then GetAsyncEnumerable() will wait until more entries are added.
            var endAddress = isAsync ? log.TailAddress : long.MaxValue;
            using (var iter = log.Scan(0, endAddress))
            {
                var counter = new Counter(log);
                if (isAsync)
                {
                    await foreach ((byte[] result, int _, long _, long nextAddress) in iter.GetAsyncEnumerable())
                    {
                        Assert.IsTrue(result.SequenceEqual(entry));
                        counter.Increment(nextAddress);
                    }
                }
                else
                {
                    while (iter.GetNext(out byte[] result, out _, out _))
                    {
                        Assert.IsTrue(result.SequenceEqual(entry));
                        counter.Increment(iter.NextAddress);
                    }
                }
                Assert.IsTrue(counter.count == numEntries);
            }

            log.Dispose();
        }

        [Test]
        public async ValueTask FasterLogTest2([Values]LogChecksumType logChecksum, [Values]bool isAsync)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager });
            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                var asyncIter = isAsync ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;
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

                    if (isAsync)
                    {
                        var curr = await asyncIter.MoveNextAsync();
                        Assert.IsTrue(curr);
                        Assert.IsTrue(asyncIter.Current.entry.SequenceEqual(data1));
                        // MoveNextAsync() would hang here waiting for more entries
                        Assert.IsTrue(asyncIter.Current.nextAddress == log.TailAddress);
                    }
                    else
                    {
                        var curr = iter.GetNext(out byte[] result, out _, out _);
                        Assert.IsTrue(curr);
                        Assert.IsTrue(result.SequenceEqual(data1));
                        Assert.IsFalse(iter.GetNext(out _, out _, out _));
                    }
                }
            }
            log.Dispose();
        }

        [Test]
        public async ValueTask FasterLogTest3([Values]LogChecksumType logChecksum, [Values] bool isAsync)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager });
            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                var asyncIter = isAsync ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;

                var appendResult = log.TryEnqueue(data1, out _);
                Assert.IsTrue(appendResult);
                await log.CommitAsync();
                await iter.WaitAsync();
                var iterResult = isAsync ? await asyncIter.MoveNextAsync()
                                         : iter.GetNext(out _, out _, out _);
                Assert.IsTrue(iterResult);

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

                if (isAsync)
                {
                    // Because we have a hole, awaiting MoveNextAsync would hang; instead, hold onto the task that results from WaitAsync() inside MoveNextAsync().
                    var task = asyncIter.MoveNextAsync();
                    await retryAppend(task.IsCompleted);

                    // Now the data is available.
                    Assert.IsTrue(await task);
                }
                else
                {
                    // Should read the "hole" and return false
                    Assert.IsFalse(iter.GetNext(out _, out _, out _));

                    // Should wait for next item
                    var task = iter.WaitAsync();
                    await retryAppend(task.IsCompleted);
                    await task;

                    // Now the data is available.
                    Assert.IsTrue(iter.GetNext(out _, out _, out _));
                }
            }
            log.Dispose();
        }

        [Test]
        public async ValueTask FasterLogTest4([Values]LogChecksumType logChecksum, [Values] bool isAsync)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager });
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

            using (var iter = log.Scan(0, long.MaxValue))
            {
                var iterResult = isAsync ? await iter.GetAsyncEnumerable().GetAsyncEnumerator().MoveNextAsync()
                                         : iter.GetNext(out _, out _, out _);
                Assert.IsTrue(iterResult);
                log.TruncateUntil(iter.NextAddress);

                Assert.IsTrue(log.CommittedUntilAddress == log.TailAddress);
                Assert.IsTrue(log.CommittedBeginAddress < log.BeginAddress);
                Assert.IsTrue(iter.NextAddress == log.BeginAddress);

                await log.CommitAsync();

                Assert.IsTrue(log.CommittedUntilAddress == log.TailAddress);
                Assert.IsTrue(log.CommittedBeginAddress == log.BeginAddress);
            }
            log.Dispose();
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
            log.Dispose();
        }

        [Test]
        public async ValueTask FasterLogTest6([Values] LogChecksumType logChecksum, [Values] bool isAsync)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 20, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager });
            byte[] data1 = new byte[1000];
            for (int i = 0; i < 100; i++) data1[i] = (byte)i;

            for (int i = 0; i < 100; i++)
            {
                log.Enqueue(data1);
            }
            log.RefreshUncommitted();
            Assert.IsTrue(log.SafeTailAddress == log.TailAddress);

            Assert.IsTrue(log.CommittedUntilAddress < log.SafeTailAddress);

            using (var iter = log.Scan(0, long.MaxValue, scanUncommitted: true))
            {
                var asyncIter = isAsync ? iter.GetAsyncEnumerable().GetAsyncEnumerator() : default;

                if (isAsync)
                {
                    while (await asyncIter.MoveNextAsync() && asyncIter.Current.nextAddress != log.SafeTailAddress)
                        log.TruncateUntil(asyncIter.Current.nextAddress);
                }
                else
                {
                    while (iter.GetNext(out _, out _, out _))
                        log.TruncateUntil(iter.NextAddress);

                    Assert.IsTrue(iter.NextAddress == log.SafeTailAddress);
                }
                log.Enqueue(data1);
                if (!isAsync)   // MoveNextAsync() would hang here waiting for more entries
                    Assert.IsFalse(iter.GetNext(out _, out _, out _));
                log.RefreshUncommitted();
                Assert.IsTrue(isAsync ? await asyncIter.MoveNextAsync() : iter.GetNext(out _, out _, out _));
            }
            log.Dispose();
        }

        private static void DeleteDirectory(string path)
        {
            foreach (string directory in Directory.GetDirectories(path))
            {
                DeleteDirectory(directory);
            }

            try
            {
                Directory.Delete(path, true);
            }
            catch (IOException)
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch { }
            }
            catch (UnauthorizedAccessException)
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch { }
            }
        }
    }
}

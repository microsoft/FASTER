// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
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

        [Test]
        public void FasterLogTest1([Values]LogChecksumType logChecksum)
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

            using (var iter = log.Scan(0, long.MaxValue))
            {
                int count = 0;
                while (iter.GetNext(out byte[] result, out _, out _))
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

        [Test]
        public async Task FasterLogTest2([Values]LogChecksumType logChecksum)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum, LogCommitManager = manager });
            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
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

                    var curr = iter.GetNext(out byte[] result, out _, out _);
                    Assert.IsTrue(curr);
                    Assert.IsTrue(result.SequenceEqual(data1));

                    var next = iter.GetNext(out _, out _, out _);
                    Assert.IsFalse(next);
                }
            }
            log.Dispose();
        }

        [Test]
        public async Task FasterLogTest3([Values]LogChecksumType logChecksum)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum, LogCommitManager = manager });
            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                var appendResult = log.TryEnqueue(data1, out _);
                Assert.IsTrue(appendResult);
                await log.CommitAsync();
                await iter.WaitAsync();
                var iterResult = iter.GetNext(out byte[] entry, out _, out _);
                Assert.IsTrue(iterResult);

                appendResult = log.TryEnqueue(data1, out _);
                Assert.IsFalse(appendResult);
                await iter.WaitAsync();

                // Should read the "hole" and return false
                iterResult = iter.GetNext(out entry, out _, out _);
                Assert.IsFalse(iterResult);

                // Should wait for next item
                var task = iter.WaitAsync();
                Assert.IsFalse(task.IsCompleted);

                appendResult = log.TryEnqueue(data1, out _);
                Assert.IsTrue(appendResult);
                await log.CommitAsync();

                await task;
                iterResult = iter.GetNext(out entry, out _, out _);
                Assert.IsTrue(iterResult);
            }
            log.Dispose();
        }

        [Test]
        public async Task FasterLogTest4([Values]LogChecksumType logChecksum)
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
                // Should read the "hole" and return false
                var iterResult = iter.GetNext(out byte[] entry, out _, out _);
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
        public async Task FasterLogTest5([Values]LogChecksumType logChecksum)
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
        public void FasterLogTest6([Values] LogChecksumType logChecksum)
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
                while (iter.GetNext(out _, out _, out _))
                {
                    log.TruncateUntil(iter.NextAddress);
                }
                Assert.IsTrue(iter.NextAddress == log.SafeTailAddress);
                log.Enqueue(data1);
                Assert.IsFalse(iter.GetNext(out _, out _, out _));
                log.RefreshUncommitted();
                Assert.IsTrue(iter.GetNext(out _, out _, out _));
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

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

        [SetUp]
        public void Setup()
        {
            if (File.Exists(TestContext.CurrentContext.TestDirectory + "\\fasterlog.log.commit"))
                File.Delete(TestContext.CurrentContext.TestDirectory + "\\fasterlog.log.commit");
            device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\fasterlog.log", deleteOnClose: true);
        }

        [TearDown]
        public void TearDown()
        {
            device.Close();
            if (File.Exists(TestContext.CurrentContext.TestDirectory + "\\fasterlog.log.commit"))
                File.Delete(TestContext.CurrentContext.TestDirectory + "\\fasterlog.log.commit");
        }

        [Test]
        public void FasterLogTest1([Values]LogChecksumType logChecksum)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum });

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

        [Test]
        public async Task FasterLogTest2([Values]LogChecksumType logChecksum)
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, LogChecksum = logChecksum });
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
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum });
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
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 14, LogChecksum = logChecksum });
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
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 16, LogChecksum = logChecksum });

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
        public async Task FasterLogTest6([Values]LogChecksumType logChecksum)
        {
            byte[][] entries = new[] { 37, 183, 251 }.Select(length => Enumerable.Range(1, length).Select(i => (byte)i).ToArray()).ToArray();
            IDevice device = null;
            FasterLog log = null;

            CloseAndReopen(ref device, ref log, logChecksum, deleteBeforeOpen: true);
            ValueTask<long> writeTask1 = log.EnqueueAndWaitForCommitAsync(entries[0], default);
            log.Commit(false);
            await writeTask1;

            CloseAndReopen(ref device, ref log, logChecksum);
            using (var iterator = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                Assert.IsTrue(iterator.GetNext(out byte[] entry1, out _));
                Verify(entry1, entries[0]);
                Assert.IsFalse(iterator.WaitAsync().IsCompleted);
                ValueTask<long> writeTask2 = log.EnqueueAndWaitForCommitAsync(new ReadOnlySpanBatch(entries.Skip(1).ToArray()), default);
                log.TruncateUntil(iterator.NextAddress);
                log.Commit(true);
                await writeTask2;
            }

            CloseAndReopen(ref device, ref log, logChecksum);
            using (var iterator = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                Assert.IsTrue(iterator.GetNext(out byte[] entry2, out _));
                Verify(entry2, entries[1]);
                Assert.IsTrue(iterator.WaitAsync().IsCompletedSuccessfully);
                log.TruncateUntil(iterator.NextAddress);
                log.Commit(true);
            }

            CloseAndReopen(ref device, ref log, logChecksum, deleteBeforeOpen: true);
            using (var iterator = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                Assert.IsFalse(iterator.GetNext(out byte[] entry3, out _));
            }

            CloseAndReopen(ref device, ref log, logChecksum, createNew: false, deleteBeforeOpen: true);

            static void CloseAndReopen(ref IDevice d, ref FasterLog l, LogChecksumType logChecksum, bool createNew = true, bool deleteBeforeOpen = false)
            {
                l?.Dispose();
                d?.Close();

                if (deleteBeforeOpen)
                {
                    File.Delete(TestContext.CurrentContext.TestDirectory + $"\\fasterlog6_{logChecksum}.log");
                    File.Delete(TestContext.CurrentContext.TestDirectory + $"\\fasterlog6_{logChecksum}.log.commit");
                }

                if (createNew)
                {
                    d = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + $"\\fasterlog6_{logChecksum}.log", preallocateFile: true, deleteOnClose: false);
                    l = new FasterLog(new FasterLogSettings { LogDevice = d, LogChecksum = logChecksum });
                }
            }

            static void Verify(byte[] read, byte[] expected)
            {
                Assert.AreEqual(read.Length, expected.Length);
                for (int i = 0; i < read.Length; i++)
                {
                    Assert.AreEqual(read[i], expected[i]);
                }
            }
        }

        private class ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private byte[][] entries;

            public ReadOnlySpanBatch(byte[][] entries) => this.entries = entries;
            public ReadOnlySpan<byte> Get(int index) => entries[index];
            public int TotalEntries() => entries.Length;
        }
    }
}

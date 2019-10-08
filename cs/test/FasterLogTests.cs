// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
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
        public void FasterLogTest1()
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
            log.AcquireThread();

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Append(entry);
            }
            log.FlushAndCommit(true);

            using (var iter = log.Scan(0, long.MaxValue))
            {
                int count = 0;
                while (iter.GetNext(out byte[] result, out int length))
                {
                    count++;
                    Assert.IsTrue(result.SequenceEqual(entry));
                    if (count % 100 == 0)
                        log.TruncateUntil(iter.CurrentAddress);
                }
                Assert.IsTrue(count == numEntries);
            }

            log.ReleaseThread();
            log.Dispose();
        }

        [Test]
        public async Task FasterLogTest2()
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                int i = 0;
                while (i++ < 500)
                {
                    var waitingReader = iter.WaitAsync();
                    Assert.IsTrue(!waitingReader.IsCompleted);

                    while (!log.TryAppend(data1, out _)) ;
                    Assert.IsFalse(waitingReader.IsCompleted);

                    await log.FlushAndCommitAsync();
                    while (!waitingReader.IsCompleted) ;
                    Assert.IsTrue(waitingReader.IsCompleted);

                    var curr = iter.GetNext(out byte[] result, out _);
                    Assert.IsTrue(curr);
                    Assert.IsTrue(result.SequenceEqual(data1));

                    var next = iter.GetNext(out _, out _);
                    Assert.IsFalse(next);
                }
            }
            log.Dispose();
        }

        [Test]
        public async Task FasterLogTest3()
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 14 });
            byte[] data1 = new byte[10000];
            for (int i = 0; i < 10000; i++) data1[i] = (byte)i;

            using (var iter = log.Scan(0, long.MaxValue, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                var appendResult = log.TryAppend(data1, out _);
                Assert.IsTrue(appendResult);
                await log.FlushAndCommitAsync();
                await iter.WaitAsync();
                var iterResult = iter.GetNext(out byte[] entry, out _);
                Assert.IsTrue(iterResult);

                appendResult = log.TryAppend(data1, out _);
                Assert.IsFalse(appendResult);
                await iter.WaitAsync();

                // Should read the "hole" and return false
                iterResult = iter.GetNext(out entry, out _);
                Assert.IsFalse(iterResult);

                // Should wait for next item
                var task = iter.WaitAsync();
                Assert.IsFalse(task.IsCompleted);

                appendResult = log.TryAppend(data1, out _);
                Assert.IsTrue(appendResult);
                await log.FlushAndCommitAsync();

                await task;
                iterResult = iter.GetNext(out entry, out _);
                Assert.IsTrue(iterResult);
            }
            log.Dispose();
        }
    }
}

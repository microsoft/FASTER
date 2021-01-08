// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;
using System.Threading;


namespace FASTER.test
{
    [TestFixture]
    internal class FasterLogResumeTests
    {
        private IDevice device;
        private string commitPath;

        [SetUp]
        public void Setup()
        {
            commitPath = TestContext.CurrentContext.TestDirectory + "/" + TestContext.CurrentContext.Test.Name + "/";

            if (Directory.Exists(commitPath))
                Directory.Delete(commitPath, true);

            device = Devices.CreateLogDevice(commitPath + "fasterlog.log", deleteOnClose: true);
        }

        [TearDown]
        public void TearDown()
        {
            device.Dispose();

            if (Directory.Exists(commitPath))
                Directory.Delete(commitPath, true);
        }

        [Test]
        [Category("FasterLog")]
        public async Task FasterLogResumePersistedReaderSpec([Values] LogChecksumType logChecksum)
        {
            CancellationToken cancellationToken;

            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };
            string readerName = "abc";

            using (var l = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 16, LogChecksum = logChecksum, LogCommitFile = commitPath }))
            {
                await l.EnqueueAsync(input1, cancellationToken);
                await l.EnqueueAsync(input2);
                await l.EnqueueAsync(input3);
                await l.CommitAsync();

                using var originalIterator = l.Scan(0, long.MaxValue, readerName);
                Assert.IsTrue(originalIterator.GetNext(out _, out _, out _, out long recoveryAddress));
                originalIterator.CompleteUntil(recoveryAddress);
                Assert.IsTrue(originalIterator.GetNext(out _, out _, out _, out _));  // move the reader ahead
                await l.CommitAsync();
            }

            using (var l = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 16, LogChecksum = logChecksum, LogCommitFile = commitPath }))
            {
                using var recoveredIterator = l.Scan(0, long.MaxValue, readerName);
                Assert.IsTrue(recoveredIterator.GetNext(out byte[] outBuf, out _, out _, out _));
                Assert.True(input2.SequenceEqual(outBuf));  // we should have read in input2, not input1 or input3
            }
        }

        [Test]
        [Category("FasterLog")]
        public async Task FasterLogResumePersistedReader2([Values] LogChecksumType logChecksum, [Values] bool overwriteLogCommits, [Values] bool removeOutdated)
        {
            var input1 = new byte[] { 0, 1, 2, 3 };
            var input2 = new byte[] { 4, 5, 6, 7, 8, 9, 10 };
            var input3 = new byte[] { 11, 12 };
            string readerName = "abc";

            using (var logCommitManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(), new DefaultCheckpointNamingScheme(commitPath), overwriteLogCommits, removeOutdated))
            {

                long originalCompleted;

                using (var l = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 16, LogChecksum = logChecksum, LogCommitManager = logCommitManager }))
                {
                    await l.EnqueueAsync(input1);
                    await l.CommitAsync();
                    await l.EnqueueAsync(input2);
                    await l.CommitAsync();
                    await l.EnqueueAsync(input3);
                    await l.CommitAsync();

                    using var originalIterator = l.Scan(0, long.MaxValue, readerName);
                    Assert.IsTrue(originalIterator.GetNext(out _, out _, out _, out long recoveryAddress));
                    originalIterator.CompleteUntil(recoveryAddress);
                    Assert.IsTrue(originalIterator.GetNext(out _, out _, out _, out _));  // move the reader ahead
                    await l.CommitAsync();
                    originalCompleted = originalIterator.CompletedUntilAddress;
                }

                using (var l = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 16, MemorySizeBits = 16, LogChecksum = logChecksum, LogCommitManager = logCommitManager }))
                {
                    using var recoveredIterator = l.Scan(0, long.MaxValue, readerName);
                    Assert.IsTrue(recoveredIterator.GetNext(out byte[] outBuf, out _, out _, out _));

                    // we should have read in input2, not input1 or input3
                    Assert.True(input2.SequenceEqual(outBuf), $"Original: {input2[0]}, Recovered: {outBuf[0]}, Original: {originalCompleted}, Recovered: {recoveredIterator.CompletedUntilAddress}");

                    // TestContext.Progress.WriteLine($"Original: {originalCompleted}, Recovered: {recoveredIterator.CompletedUntilAddress}"); 
                }
            }
        }
    }
}

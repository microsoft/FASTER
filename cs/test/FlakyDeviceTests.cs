using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    internal class TestFallbackScheme : IFallbackScheme
    {
        private IDevice mainDevice, fallbackDevice;

        public TestFallbackScheme(IDevice mainDevice, IDevice fallbackDevice)
        {
            this.mainDevice = mainDevice;
            this.fallbackDevice = fallbackDevice;
        }
        
        public void Dispose()
        {
            mainDevice.Dispose();
            fallbackDevice.Dispose();
        }

        public string BaseName() => mainDevice.FileName;

        public uint SectorSize() => mainDevice.SectorSize;

        public long SegmentSize() => mainDevice.SegmentSize;

        public long Capacity() => mainDevice.Capacity;

        public IDevice GetMainDevice() => mainDevice;

        public IDevice GetFallbackDevice() => fallbackDevice;

        // test devices not expected to persist across runs
        public List<(long, long)> GetFallbackRanges() => new();

        public void AddFallbackRange(long rangeStart, long rangeEnd)
        {
        }
    }
    
    [TestFixture]
    internal class FlakyDeviceTests : FasterLogTestBase
    {
        [SetUp]
        public void Setup() => base.BaseSetup();

        [TearDown]
        public void TearDown() => base.BaseTearDown();

        [Test]
        [Category("FasterLog")]
        public async ValueTask FlakyLogTestCleanFailure([Values] bool isAsync)
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.1,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(path + "fasterlog.log", deleteOnClose: true),
                errorOptions);
            var logSettings = new FasterLogSettings
                {LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager};
            log = new FasterLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte) i;

            try
            {
                // Ensure we execute long enough to trigger errors
                for (int j = 0; j < 100; j++)
                {
                    for (int i = 0; i < numEntries; i++)
                    {
                        log.Enqueue(entry);
                    }

                    if (isAsync)
                        await log.CommitAsync();
                    else
                        log.Commit();
                }
            }
            catch (CommitFailureException e)
            {
                var errorRangeStart = e.LinkedCommitInfo.CommitInfo.FromAddress;
                Assert.LessOrEqual(log.CommittedUntilAddress, errorRangeStart);
                Assert.LessOrEqual(log.FlushedUntilAddress, errorRangeStart);
                return;
            }

            // Should not ignore failures
            Assert.Fail();
        }

        [Test]
        [Category("FasterLog")]
        public async ValueTask FlakyLogTestConcurrentWriteFailure()
        {
            var errorOptions = new ErrorSimulationOptions
            {
                readTransientErrorRate = 0,
                readPermanentErrorRate = 0,
                writeTransientErrorRate = 0,
                writePermanentErrorRate = 0.05,
            };
            device = new SimulatedFlakyDevice(Devices.CreateLogDevice(path + "fasterlog.log", deleteOnClose: true),
                errorOptions);
            var logSettings = new FasterLogSettings
                {LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager};
            log = new FasterLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte) i;

            var failureList = new List<CommitFailureException>();
            ThreadStart runTask = () =>
            {
                var random = new Random();
                try
                {
                    // Ensure we execute long enough to trigger errors
                    for (int j = 0; j < 100; j++)
                    {
                        for (int i = 0; i < numEntries; i++)
                        {
                            log.Enqueue(entry);
                            // create randomly interleaved concurrent writes
                            if (random.NextDouble() < 0.1)
                                log.Commit();
                        }
                    }
                }
                catch (CommitFailureException e)
                {
                    lock (failureList)
                        failureList.Add(e);
                }
            };

            var threads = new List<Thread>();
            for (var i = 0; i < Environment.ProcessorCount + 1; i++)
            {
                var t = new Thread(runTask);
                t.Start();
                threads.Add(t);
            }

            foreach (var thread in threads)
                thread.Join();

            // Every thread observed the failure
            Assert.IsTrue(failureList.Count == threads.Count);
            // They all observed the same failure
            foreach (var failure in failureList)
            {
                Assert.AreEqual(failure, failureList[0]);
            }
        }
    }
}
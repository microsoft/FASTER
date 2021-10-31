using System;
using System.IO;
using FASTER.core;
using FASTER.test.recovery;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class FasterLogFastCommitTests : FasterLogTestBase
    {
        [SetUp]
        public void Setup() => base.BaseSetup(false, false);

        [TearDown]
        public void TearDown() => base.BaseTearDown();

        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void FasterLogSimpleFastCommitTest([Values] TestUtils.DeviceType deviceType)
        {
            var cookie = new byte[100];
            new Random().NextBytes(cookie);
            
            device = TestUtils.CreateTestDevice(deviceType, "fasterlog.log", deleteOnClose: true);
            var logSettings = new FasterLogSettings { LogDevice = device, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager, FastCommitMode = true};
            log = new FasterLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }

            var cookie1 = new byte[100];
            new Random().NextBytes(cookie1);
            var commitSuccessful = log.CommitStrongly(out var commit1Addr, out _, true, cookie1, 1);
            Assert.IsTrue(commitSuccessful);
            
            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }
            
            var cookie2 = new byte[100];
            new Random().NextBytes(cookie2);
            commitSuccessful = log.CommitStrongly(out var commit2Addr, out _, true, cookie2, 2);
            Assert.IsTrue(commitSuccessful);
            
            for (int i = 0; i < numEntries; i++)
            {
                log.Enqueue(entry);
            }
            
            var cookie6 = new byte[100];
            new Random().NextBytes(cookie6);
            commitSuccessful = log.CommitStrongly(out var commit6Addr, out _, true, cookie6, 6); 
            Assert.IsTrue(commitSuccessful);
            
            // be a deviant and remove commit metadata files
            manager.PurgeAll();

            // Recovery should still work
            var recoveredLog = new FasterLog(logSettings, 1);
            Assert.AreEqual(cookie1, recoveredLog.RecoveredCookie);
            Assert.AreEqual(commit1Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();
            
            recoveredLog = new FasterLog(logSettings, 2);
            Assert.AreEqual(cookie2, recoveredLog.RecoveredCookie);
            Assert.AreEqual(commit2Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();

            // Default argument should recover to most recent
            recoveredLog = new FasterLog(logSettings);
            Assert.AreEqual(cookie6, recoveredLog.RecoveredCookie);
            Assert.AreEqual(commit6Addr, recoveredLog.TailAddress);
            recoveredLog.Dispose();
        }

        
    }
}
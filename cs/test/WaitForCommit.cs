// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class WaitForCommitTests
    {
        public FasterLog log;
        public IDevice device;
        private string path;
        static readonly byte[] entry = new byte[10];

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait:true);

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "WaitForCommit", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;

            // Clean up log files
            TestUtils.DeleteDirectory(path);
        }

        [TestCase("Sync")]  // use string here instead of Bool so shows up in Test Explorer with more descriptive name
        [TestCase("Async")]
        [Test]
        [Category("FasterLog")]
        public void WaitForCommitBasicTest(string SyncTest)
        {

            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;

            // make it small since launching each on separate threads 
            int entryLength = 10;
            int expectedEntries = 3;  // Not entry length because this is number of enqueues called

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            Task currentTask;

            // Enqueue and Commit in a separate thread (wait there until commit is done though).
            if (SyncTest == "Sync")
            {
                currentTask = Task.Run(() => LogWriter(log, entry), token);
            }
            else
            {
                currentTask = Task.Run(() => LogWriterAsync(log, entry), token);
            }

            // Give all a second or so to queue up and to help with timing issues - shouldn't need but timing issues
            Thread.Sleep(2000);

            // Commit to the log
            log.Commit(true);
            currentTask.Wait(4000, token);

            // double check to make sure finished - seen cases where timing kept running even after commit done
            if (currentTask.Status != TaskStatus.RanToCompletion)
                cts.Cancel();

            // Read the log to make sure all entries are put in
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {

                        Assert.IsTrue(result[currentEntry] == (byte)currentEntry, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + " not match expected:" + currentEntry);

                        currentEntry++;
                    }
                }
            }

            // Make sure expected entries is same as current - also makes sure that data verification was not skipped
            Assert.AreEqual(expectedEntries, currentEntry);

            // NOTE: seeing issues where task is not running to completion on Release builds
            // This is a final check to make sure task finished. If didn't then assert
            // One note - if made it this far, know that data was Enqueue and read properly, so just
            // case of task not stopping
            if (currentTask.Status != TaskStatus.RanToCompletion)
            {
                Assert.Fail("Final Status check Failure -- Task should be 'RanToCompletion' but current Status is:" + currentTask.Status);
            }
        }

        static void LogWriter(FasterLog log, byte[] entry)
        {
            // Enter in some entries then wait on this separate thread
            log.Enqueue(entry);
            log.Enqueue(entry);
            log.Enqueue(entry);
            log.WaitForCommit(log.TailAddress);
        }

        static async Task LogWriterAsync(FasterLog log, byte[] entry)
        {
            // Enter in some entries then wait on this separate thread
            await log.EnqueueAsync(entry);
            await log.EnqueueAsync(entry);
            await log.EnqueueAsync(entry);
            await log.WaitForCommitAsync(log.TailAddress);
        }

    }
}



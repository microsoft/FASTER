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
    internal class WaitForCommitTests
    {
        public FasterLog log;
        public IDevice device;
        private string path = Path.GetTempPath() + "WaitForCommitTests/";
        static readonly byte[] entry = new byte[10];

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            try { new DirectoryInfo(path).Delete(true); }
            catch { }

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "WaitForCommit", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            device.Dispose();

            // Clean up log files
            try { new DirectoryInfo(path).Delete(true); }
            catch { }
        }

// NOTE: Having issues where Tasks aren't stopping on Release which kills the CIs - only run in debug until figure it out why
#if DEBUG
        [Test]
        [Category("FasterLog")]
        public void WaitForCommitBasicTest()
        {

            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;

            // make it small since launching each on separate threads 
            int entryLength = 10;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Enqueue and Commit in a separate thread (wait there until commit is done though).
            Task currentTask = Task.Run(() => LogWriter(log, entry), token);

            // Give all a second or so to queue up
            Thread.Sleep(2000);

            // Commit to the log
            log.Commit(true);
            currentTask.Wait(4000, token);

            // double check to make sure finished - seen cases where timing kept running even after commit done
            if (currentTask.Status != TaskStatus.RanToCompletion)
                cts.Cancel();

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log to make sure all entries are put in
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        Assert.IsTrue(result[currentEntry] == (byte)currentEntry, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + " not match expected:" + currentEntry);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");

            // NOTE: seeing issues where task is not running to completion on Release builds
            // This is a final check to make sure task finished. If didn't then assert
            // One note - if made it this far, know that data was Enqueue and read properly, so just
            // case of task not stopping
            if (currentTask.Status != TaskStatus.RanToCompletion)
            {
                Assert.Fail("Final Status check Failure -- Task should be 'RanToCompletion' but current Status is:" + currentTask.Status);
            }
        }
#endif

        static void LogWriter(FasterLog log, byte[] entry)
        {
            // Enter in some entries then wait on this separate thread
            log.Enqueue(entry);
            log.Enqueue(entry);
            log.Enqueue(entry);
            log.WaitForCommit(log.TailAddress);
        }

    }
}



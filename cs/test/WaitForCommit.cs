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
            device = Devices.CreateLogDevice(path + "EnqueueAndWaitForCommit", deleteOnClose: true);
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


        [Test]
        [Category("FasterLog")]
        public void WaitForCommitBasicTest()
        {
            // make it small since launching each on separate threads 
            int entryLength = 10;
            int numEntries = 5;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Fill the log
            for (int i = 0; i < numEntries; i++)
            {
                // Fill entry for the log
                if (i < entryLength)
                    entry[i] = (byte)i;
                
                //Launch on separate thread so it can sit until commit
                new Thread(delegate () 
                {
                    LogWriter(log, entry); 
                }).Start();
                
            }

            // Give all a second or so to queue up
            Thread.Sleep(2000);

            // Commit to the log
            log.Commit(true);

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
        }

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



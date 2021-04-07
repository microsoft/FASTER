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
using System.Text;

namespace FASTER.test
{

    [TestFixture]
    internal class FasterLogScanTests
    {

        private FasterLog log;
        private IDevice device;
        private FasterLog logUncommitted;
        private IDevice deviceUnCommitted;

        private string path = Path.GetTempPath() + "ScanTests/";
        static readonly byte[] entry = new byte[100];
        static int entryLength = 100;
        static int numEntries = 1000;
        static int entryFlag = 9999;

        // Create and populate the log file so can do various scans
        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            try {  new DirectoryInfo(path).Delete(true);  }
            catch {}

            // Set up the Devices \ logs
            device = Devices.CreateLogDevice(path + "LogScan", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
            deviceUnCommitted = Devices.CreateLogDevice(path + "LogScanUncommitted", deleteOnClose: true);
            logUncommitted = new FasterLog(new FasterLogSettings { LogDevice = deviceUnCommitted });

            //****** Populate log for Basic data for tests 
            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Enqueue but set each Entry in a way that can differentiate between entries
            for (int i = 0; i < numEntries; i++)
            {
                // Flag one part of entry data that corresponds to index
                if (i < entryLength)
                    entry[i] = (byte)entryFlag;

                // puts back the previous entry value
                if ((i > 0) && (i < entryLength))
                    entry[i - 1] = (byte)(i - 1);

                // Add to FasterLog
                log.Enqueue(entry);
            }

            // Commit to the log
            log.Commit(true);


            //****** Populate uncommitted log / device for ScanUncommittedTest
            // Set Default entry data
            for (int j = 0; j < entryLength; j++)
            {
                entry[j] = (byte)j;
            }

            // Enqueue but set each Entry in a way that can differentiate between entries
            for (int j = 0; j < numEntries; j++)
            {
                // Flag one part of entry data that corresponds to index
                if (j < entryLength)
                    entry[j] = (byte)entryFlag;

                // puts back the previous entry value
                if ((j > 0) && (j < entryLength))
                    entry[j - 1] = (byte)(j - 1);

                // Add to FasterLog
                logUncommitted.Enqueue(entry);
            }

            // refresh uncommitted so can see it when scan - do NOT commit though 
            logUncommitted.RefreshUncommitted(true);

        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            device.Dispose();
            deviceUnCommitted.Dispose();
            logUncommitted.Dispose();

            // Clean up log files
            try { new DirectoryInfo(path).Delete(true); }
            catch { }
        }

        [Test]
        [Category("FasterLog")]
        public void ScanBasicDefaultTest()
        {

            // Basic default scan from start to end 
            // Indirectly used in other tests, but good to have the basic test here for completeness

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))  
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result["+ currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }

        [Test]
        [Category("FasterLog")]
        public void ScanNoDefaultTest()
        {

            // Test where all params are set just to make sure handles it ok

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000,name: null,recover: true,scanBufferingMode: ScanBufferingMode.DoublePageBuffering,scanUncommitted: false))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }


        [Test]
        [Category("FasterLog")]
        public void ScanByNameTest()
        {

            //You can persist iterators(or more precisely, their CompletedUntilAddress) as part of a commit by simply naming them during their creation. 

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, name: "TestScan", recover: true))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
            Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }


        [Test]
        [Category("FasterLog")]
        public void ScanWithoutRecoverTest()
        {
            // You may also force an iterator to start at the specified begin address, i.e., without recovering: recover parameter = false

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log 
            int currentEntry = 9;   // since starting at specified address of 1000, need to set current entry as 9 so verification starts at proper spot
            using (var iter = log.Scan(1000, 100_000_000, recover: false))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }

        [Test]
        [Category("FasterLog")]
        public void ScanBufferingModeDoublePageTest()
        {
            // Same as default, but do it just to make sure have test in case default changes

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, scanBufferingMode: ScanBufferingMode.DoublePageBuffering))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }

        [Test]
        [Category("FasterLog")]
        public void ScanBufferingModeSinglePageTest()
        {
            
            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000, scanBufferingMode: ScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }


        [Test]
        [Category("FasterLog")]
        public void ScanUncommittedTest()
        {

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Setting scanUnCommitted to true is actual test here.
            // Read the log - Look for the flag so know each entry is unique and still reads uncommitted
            int currentEntry = 0;
            using (var iter = logUncommitted.Scan(0, 100_000_000, scanUncommitted: true))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        // set check flag to show got in here
                        datacheckrun = true;

                        // Span Batch only added first entry several times so have separate verification
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }


    }
}



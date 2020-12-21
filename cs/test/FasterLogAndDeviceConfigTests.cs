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

    //* NOTE: 
    //* A lot of various usage of Log config and Device config are in FasterLog.cs so the test here
    //* is for areas / parameters not covered by the tests in other areas of the test system
    //* For completeness, setting other parameters too where possible
    //* However, the verification is pretty light. Just makes sure log file created and things be added and read from it 



    /*  *** Seeing if this is the issue as I have a test that is messing up CI
       
       
    [TestFixture]
    internal class LogAndDeviceConfigTests
    {
        private FasterLog log;
        private IDevice device;
        private string path = Path.GetTempPath() + "DeviceConfigTests/";
        static readonly byte[] entry = new byte[100];


        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            try {  new DirectoryInfo(path).Delete(true);  }
            catch {} 

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "DeviceConfig", deleteOnClose: true, recoverDevice: true, preallocateFile: true, capacity: 1 << 30);
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 80, MemorySizeBits = 20, GetMemory = null, SegmentSizeBits = 80, MutableFraction = 0.2, LogCommitManager = null});
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
        public void DeviceAndLogConfig()
        {

            int entryLength = 100;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
                log.Enqueue(entry);
            }

            // Commit to the log
            log.Commit(true);

            // Verify  
            Assert.IsTrue(File.Exists(path+"/log-commits/commit.0.0"));
            Assert.IsTrue(File.Exists(path + "/DeviceConfig.0"));

            // Read the log just to verify can actually read it
            int currentEntry = 0;
            using (var iter = log.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                       Assert.IsTrue(result[currentEntry] == currentEntry, "Fail - Result[" + currentEntry.ToString() + "]: is not same as "+currentEntry.ToString() );

                       currentEntry++;
                }
            }
        }

    }
    */
}



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
    internal class EnqueueTests
    {
        private FasterLog log;
        private IDevice device;
        private string path = Path.GetTempPath() + "EnqueTests/";

        [SetUp]
        public void Setup()
        {

            // Clean up log files from previous test runs in case they weren't cleaned up
            try {  new DirectoryInfo(path).Delete(true);  }
            catch {} 

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "Enqueue", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });

        }

        [TearDown]
        public void TearDown()
        {
         
            log.Dispose();

            // Clean up log files
            try { new DirectoryInfo(path).Delete(true); }
            catch { }
        }


        [Test]
        public void EnqueueBasicTest()
        {

            //*** This test alone works - add parameter sweep to do different kind of enqueue?
            Assert.Fail("TO DO: Add parameter sweep??");

            int entryLength = 20;
            int numEntries = 1000000; // 1000;
            int entryFlag = 9999;
            byte[] entry = new byte[entryLength];

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
            log.Commit(false);

            int currentEntry = 0;

            // Read the log - Look for the flag so know each entry is unique
            using (var iter = log.Scan(0, 100_000_000))  
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    if (currentEntry < entryLength)
                    {
                        Assert.IsTrue(result[currentEntry] == (byte)entryFlag);  // simple check to make sure log enqueue worked
                        currentEntry++;
                    }
                }
            }

        }
    }
}



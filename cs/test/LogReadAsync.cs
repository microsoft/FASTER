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
    internal class LogReadAsyncTests
    {
        private FasterLog log;
        private IDevice device;
        private string path = Path.GetTempPath() + "LogReadAsync/";

        public enum ParameterDefaultsIteratorType
        {
            DefaultParams,
            LengthParam,
            TokenParam
        }

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            try { new DirectoryInfo(path).Delete(true); }
            catch { }

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "LogReadAsync", deleteOnClose: true);
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
        public void LogReadAsyncBasicTest([Values] ParameterDefaultsIteratorType iteratorType)
        {
            int entryLength = 100;
            int numEntries = 1000000;
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
                
                log.Enqueue(entry);
            }

            // Commit to the log
            log.Commit();

            // Read one entry based on different parameters for AsyncReadOnly and verify 
            switch (iteratorType)
            {
                case ParameterDefaultsIteratorType.DefaultParams:
                    // Read one entry and verify
                    var record = log.ReadAsync(log.BeginAddress);
                    var foundFlagged = record.Result.Item1[0];   // 15
                    var foundEntry = record.Result.Item1[1];  // 1
                    var foundTotal = record.Result.Item2;

                    Assert.IsTrue(foundFlagged == (byte)entryFlag, "Fail reading data - Found Flagged Entry:" + foundFlagged.ToString() + "  Expected Flagged entry:" + entryFlag);
                    Assert.IsTrue(foundEntry == 1, "Fail reading data - Found Normal Entry:" + foundEntry.ToString() + "  Expected Value: 1");
                    Assert.IsTrue(foundTotal == 100, "Fail reading data - Found Total:" + foundTotal.ToString() + "  Expected Total: 100");

                    break;
                case ParameterDefaultsIteratorType.LengthParam:
                    // Read one entry and verify
                    record = log.ReadAsync(log.BeginAddress, 208);
                    foundFlagged = record.Result.Item1[0];   // 15
                    foundEntry = record.Result.Item1[1];  // 1
                    foundTotal = record.Result.Item2;

                    Assert.IsTrue(foundFlagged == (byte)entryFlag, "Fail reading data - Found Flagged Entry:" + foundFlagged.ToString() + "  Expected Flagged entry:" + entryFlag);
                    Assert.IsTrue(foundEntry == 1, "Fail reading data - Found Normal Entry:" + foundEntry.ToString() + "  Expected Value: 1");
                    Assert.IsTrue(foundTotal == 100, "Fail reading data - Found Total:" + foundTotal.ToString() + "  Expected Total: 100");

                    break;
                case ParameterDefaultsIteratorType.TokenParam:
                    var cts = new CancellationToken();

                    // Read one entry and verify
                    record = log.ReadAsync(log.BeginAddress, 104, cts);
                    foundFlagged = record.Result.Item1[0];   // 15
                    foundEntry = record.Result.Item1[1];  // 1
                    foundTotal = record.Result.Item2;

                    Assert.IsTrue(foundFlagged == (byte)entryFlag, "Fail reading data - Found Flagged Entry:" + foundFlagged.ToString() + "  Expected Flagged entry:" + entryFlag);
                    Assert.IsTrue(foundEntry == 1, "Fail reading data - Found Normal Entry:" + foundEntry.ToString() + "  Expected Value: 1");
                    Assert.IsTrue(foundTotal == 100, "Fail reading data - Found Total:" + foundTotal.ToString() + "  Expected Total: 100");

                    break;
                default:
                    Assert.Fail("Unknown case ParameterDefaultsIteratorType.DefaultParams:");
                    break;
            }
        }

    }
}



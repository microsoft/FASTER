// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class LogReadAsyncTests
    {
        private FasterLog log;
        private IDevice device;
        private string path;

        public enum ParameterDefaultsIteratorType
        {
            DefaultParams,
            LengthParam,
            TokenParam
        }

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait:true);

        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;

            // Clean up log files
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void LogReadAsyncBasicTest([Values] ParameterDefaultsIteratorType iteratorType, [Values] TestUtils.DeviceType deviceType)
        {
            int entryLength = 20;
            int numEntries = 500;
            int entryFlag = 9999;
            string filename = path + "LogReadAsync" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new FasterLog(new FasterLogSettings { LogDevice = device,SegmentSizeBits = 22, LogCommitDir = path });

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
            log.Commit(true);


            // Read one entry based on different parameters for AsyncReadOnly and verify 
            switch (iteratorType)
            {
                case ParameterDefaultsIteratorType.DefaultParams:
                    // Read one entry and verify
                    var record = log.ReadAsync(log.BeginAddress);
                    var foundFlagged = record.Result.Item1[0];   // 15
                    var foundEntry = record.Result.Item1[1];  // 1
                    var foundTotal = record.Result.Item2;

                    Assert.AreEqual((byte)entryFlag, foundFlagged, $"Fail reading Flagged Entry");
                    Assert.AreEqual(1, foundEntry, $"Fail reading Normal Entry");
                    Assert.AreEqual(entryLength, foundTotal, $"Fail reading Total");

                    break;
                case ParameterDefaultsIteratorType.LengthParam:
                    // Read one entry and verify
                    record = log.ReadAsync(log.BeginAddress, 208);
                    foundFlagged = record.Result.Item1[0];   // 15
                    foundEntry = record.Result.Item1[1];  // 1
                    foundTotal = record.Result.Item2;

                    Assert.AreEqual((byte)entryFlag, foundFlagged, $"Fail reading Flagged Entry");
                    Assert.AreEqual(1, foundEntry, $"Fail reading Normal Entry");
                    Assert.AreEqual(entryLength, foundTotal, $"Fail readingTotal");

                    break;
                case ParameterDefaultsIteratorType.TokenParam:
                    var cts = new CancellationToken();

                    // Read one entry and verify
                    record = log.ReadAsync(log.BeginAddress, 104, cts);
                    foundFlagged = record.Result.Item1[0];   // 15
                    foundEntry = record.Result.Item1[1];  // 1
                    foundTotal = record.Result.Item2;

                    Assert.AreEqual((byte)entryFlag, foundFlagged, $"Fail readingFlagged Entry");
                    Assert.AreEqual(1, foundEntry, $"Fail reading Normal Entry");
                    Assert.AreEqual(entryLength, foundTotal, $"Fail reading Total");

                    break;
                default:
                    Assert.Fail("Unknown case ParameterDefaultsIteratorType.DefaultParams:");
                    break;
            }


        }

    }
}



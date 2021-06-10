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

    //** Fundamental basic test for TryEnqueue that covers all the parameters in TryEnqueue
    //** Other tests in FasterLog.cs provide more coverage for TryEnqueue

    [TestFixture]
    internal class TryEnqueueTests
    {
        private FasterLog log;
        private IDevice device;
        static readonly byte[] entry = new byte[100];
        private string commitPath;

        public enum TryEnqueueIteratorType
        {
            Byte,
            SpanBatch,
            SpanByte
        }

        private struct ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private readonly int batchSize;
            public ReadOnlySpanBatch(int batchSize) => this.batchSize = batchSize;
            public ReadOnlySpan<byte> Get(int index) => entry; 
            public int TotalEntries() => batchSize;
        }

        [SetUp]
        public void Setup()
        {
            commitPath = TestContext.CurrentContext.TestDirectory + "/" + TestContext.CurrentContext.Test.Name + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            try {  new DirectoryInfo(commitPath).Delete(true);  }
            catch {} 
        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            device.Dispose();

            // Clean up log files
            try { new DirectoryInfo(commitPath).Delete(true); }
            catch { }
        }


       [Test]
       [Category("FasterLog")]
       [Category("Smoke")]
        public void TryEnqueueBasicTest([Values] TryEnqueueIteratorType iteratorType, [Values] TestUtils.DeviceType deviceType)
        {
            int entryLength = 50;
            int numEntries = 10000; 
            int entryFlag = 9999;

            // Create devices \ log for test
            string filename = commitPath + "TryEnqueue" + deviceType.ToString() + ".log";
            device = TestUtils.CreateTestDevice(deviceType, filename);
            log = new FasterLog(new FasterLogSettings { LogDevice = device, SegmentSizeBits = 22 });

#if WINDOWS
            // Issue with Non Async Commit and Emulated Azure so don't run it - at least put after device creation to see if crashes doing that simple thing
            if (deviceType == TestUtils.DeviceType.EmulatedAzure)
                return;
#endif

            // Reduce SpanBatch to make sure entry fits on page
            if (iteratorType == TryEnqueueIteratorType.SpanBatch)
            {
                entryLength = 10;
                numEntries = 50;
            }

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);

            // TryEnqueue but set each Entry in a way that can differentiate between entries
            for (int i = 0; i < numEntries; i++)
            {
                bool appendResult = false;
                long logicalAddress = 0;
                long ExpectedOutAddress = 0;

                // Flag one part of entry data that corresponds to index
                if (i < entryLength)
                    entry[i] = (byte)entryFlag;

                // puts back the previous entry value
                if ((i > 0) && (i < entryLength))
                    entry[i - 1] = (byte)(i - 1);

                // Add to FasterLog
                switch (iteratorType)
                {
                    case TryEnqueueIteratorType.Byte:
                        // Default is add bytes so no need to do anything with it
                        appendResult = log.TryEnqueue(entry, out logicalAddress);
                        break;
                    case TryEnqueueIteratorType.SpanByte:
                        // Could slice the span but for basic test just pass span of full entry - easier verification
                        Span<byte> spanEntry = entry;
                        appendResult = log.TryEnqueue(spanEntry, out logicalAddress);
                        break;
                    case TryEnqueueIteratorType.SpanBatch:
                        appendResult = log.TryEnqueue(spanBatch, out logicalAddress);
                        break;
                    default:
                        Assert.Fail("Unknown TryEnqueueIteratorType");
                        break;
                }

                // Verify each Enqueue worked
                Assert.IsTrue(appendResult == true, "Fail - TryEnqueue failed with a 'false' result for entry:" + i.ToString());

                // logical address has new entry every x bytes which is one entry less than the TailAddress
                if (iteratorType == TryEnqueueIteratorType.SpanBatch)
                    ExpectedOutAddress = log.TailAddress - 5200;
                else
                    ExpectedOutAddress = log.TailAddress - 104;

                Assert.IsTrue(logicalAddress == ExpectedOutAddress, "Fail - returned LogicalAddr: " + logicalAddress.ToString() + " is not equal to Expected LogicalAddr: " + ExpectedOutAddress.ToString());
            }

            // Commit to the log
            log.Commit(true);

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
                        if (iteratorType == TryEnqueueIteratorType.SpanBatch)
                            Assert.IsTrue(result[0] == (byte)entryFlag, "Fail - Result[0]:"+result[0].ToString()+"  entryFlag:"+entryFlag);  
                        else
                            Assert.IsTrue(result[currentEntry] == (byte)entryFlag, "Fail - Result["+ currentEntry.ToString() + "]:" + result[0].ToString() + "  entryFlag:" + entryFlag);

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



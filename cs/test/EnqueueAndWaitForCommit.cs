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
    internal class EnqWaitCommitTest
    {
        public FasterLog log;
        public IDevice device;
        static readonly byte[] entry = new byte[10];
        static readonly ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(100); 
        private string commitPath;

        public enum EnqueueIteratorType
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
            try { new DirectoryInfo(commitPath).Delete(true); }
            catch { }

            // Create devices \ log for test
            device = Devices.CreateLogDevice(commitPath + "EnqueueAndWaitForCommit.log", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
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

 // The CIs in Azure Dev Ops time out when this runs under Release. Until figure out what is going on, put this as Debug only
//#if DEBUG
        [Test]
        [Category("FasterLog")]
        public void EnqWaitCommitBasicTest([Values] EnqueueIteratorType iteratorType)
        {

            // make it very small to keep run time down
            int entryLength = 5;
            int numEntries = 2;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);

            // Fill Log
            for (int i = 0; i < numEntries; i++)
            {
                // Fill log entry
                if (i < entryLength)
                    entry[i] = (byte)i;

                // Add to FasterLog
                switch (iteratorType)
                {
                    case EnqueueIteratorType.Byte:
                        // Launch on separate thread so it can sit until commit
                        Task.Run(() => LogWriter(log, entry, EnqueueIteratorType.Byte));
                        break;
                    case EnqueueIteratorType.SpanByte:
                        // Could slice the span but for basic test just pass span of full entry - easier verification
                        Span<byte> spanEntry = entry;
                        Task.Run(() => LogWriter(log, entry, EnqueueIteratorType.SpanByte));
                        break;
                    case EnqueueIteratorType.SpanBatch:
                        Task.Run(() => LogWriter(log, entry, EnqueueIteratorType.SpanBatch));
                        break;

                    default:
                        Assert.Fail("Unknown EnqueueIteratorType");
                        break;
                }
            }

            // Give all a second or so to queue up
            Thread.Sleep(1000);

            // Commit to the log
            log.Commit(true);

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            // Read the log - Look for the flag so know each entry is unique
            int currentEntry = 0;
            using (var iter = log.Scan(0, 1000))
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
//#endif

        public static void LogWriter(FasterLog log, byte[] entry, EnqueueIteratorType iteratorType)
        {

            // Add to FasterLog on separate threads as it will sit and await until Commit happens
            switch (iteratorType)
            {
                case EnqueueIteratorType.Byte:
                    var rc = log.EnqueueAndWaitForCommit(entry);
                    break;
                case EnqueueIteratorType.SpanByte:
                    Span<byte> spanEntry = entry;
                    rc = log.EnqueueAndWaitForCommit(spanEntry);
                    break;
                case EnqueueIteratorType.SpanBatch:
                    rc = log.EnqueueAndWaitForCommit(spanBatch);
                    break;
                default:
                    Assert.Fail("Unknown EnqueueIteratorType");
                    break;
            }
        }

    }
}



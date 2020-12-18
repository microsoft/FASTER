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
    internal class EnqueueAndWaitForCommitTests
    {
        public FasterLog log;
        public IDevice device;
        private string path = Path.GetTempPath() + "EnqueueAndWaitForCommitTests/";
        static readonly byte[] entry = new byte[10];
        static readonly ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(10000);

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
        public void EnqueueAndWaitForCommitBasicTest([Values] EnqueueIteratorType iteratorType)
        {
            // make it small since launching each on separate threads 
            int entryLength = 10;
            int numEntries = 5;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);

            // Enqueue but set each Entry in a way that can differentiate between entries
            for (int i = 0; i < numEntries; i++)
            {
                // Fill log
                if (i < entryLength)
                    entry[i] = (byte)i;

                // Add to FasterLog
                switch (iteratorType)
                {
                    case EnqueueIteratorType.Byte:
                        // Launch on separate thread so it can sit until commit
                        new Thread(delegate () 
                        {
                            LogWriter(log, entry, EnqueueIteratorType.Byte); 
                        }).Start();
                        
                        break;
                    case EnqueueIteratorType.SpanByte:
                        // Could slice the span but for basic test just pass span of full entry - easier verification
                        Span<byte> spanEntry = entry;

                        new Thread(delegate (){
                            LogWriter(log, entry, EnqueueIteratorType.SpanByte);
                        }).Start();
                        break;
                    case EnqueueIteratorType.SpanBatch:
                        new Thread(delegate (){
                            LogWriter(log, entry, EnqueueIteratorType.SpanBatch);
                        }).Start();
                        break;
                    default:
                        Assert.Fail("Unknown EnqueueIteratorType");
                        break;
                }
            }

            // Give all a second or so to queue up
            Thread.Sleep(2000);

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

                        Assert.IsTrue(result[currentEntry] == (byte)currentEntry, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + " not match expected:" + currentEntry);

                        currentEntry++;
                    }
                }
            }

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }

        static void LogWriter(FasterLog log, byte[] entry, EnqueueIteratorType iteratorType)
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



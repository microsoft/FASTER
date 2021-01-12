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
        static readonly byte[] entry = new byte[5];
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
// NOTE: Having issues where Tasks aren't stopping on Release which kills the CIs - only run in debug until figure it out why
#if DEBUG
        [Test]
        [Category("FasterLog")]
        public void EnqueueWaitCommitBasicTest([Values] EnqueueIteratorType iteratorType)
        {

            // keep it simple by just adding one entry
            int entryLength = 5;
            int numEntries = 1;

            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken token = cts.Token;
            ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);
            Task currentTask = null;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Add to FasterLog
            switch (iteratorType)
            {
                case EnqueueIteratorType.Byte:
                    // Launch on separate thread so it can sit until commit
                    currentTask = Task.Run(() => LogWriter(log, entry, EnqueueIteratorType.Byte), token);
                    break;
                case EnqueueIteratorType.SpanByte:
                    // Could slice the span but for basic test just pass span of full entry - easier verification
                    Span<byte> spanEntry = entry;
                    currentTask = Task.Run(() => LogWriter(log, entry, EnqueueIteratorType.SpanByte), token);
                    break;
                case EnqueueIteratorType.SpanBatch:
                    currentTask = Task.Run(() => LogWriter(log, entry, EnqueueIteratorType.SpanBatch), token);
                    break;

                default:
                    Assert.Fail("Unknown EnqueueIteratorType");
                    break;
            }

            // Give all a second or so to make sure LogWriter got called first - if things working right, wouldn't need this
            Thread.Sleep(2000);

            // Commit to the log and wait for tasks to finish
            log.Commit(true);
            currentTask.Wait(4000,token);

            // double check to make sure finished
            if (currentTask.Status != TaskStatus.RanToCompletion)
            {
                cts.Cancel();
            }

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


            // NOTE: seeing issues where task is not running to completion on Release builds
            // This is a final check to make sure task finished. If didn't then assert
            // One note - if made it this far, know that data was Enqueue and read properly, so just
            // case of task not stopping
            if (currentTask.Status != TaskStatus.RanToCompletion)
            {
                Assert.Fail("Final Status check Failure -- Task should be 'RanToCompletion' but current Status is:"+ currentTask.Status);
            }
        }
#endif

        public static void LogWriter(FasterLog log, byte[] entry, EnqueueIteratorType iteratorType)
        {

            try
            {
                long returnLogicalAddress = 0;

                // Add to FasterLog on separate threads as it will sit and await until Commit happens
                switch (iteratorType)
                {
                    case EnqueueIteratorType.Byte:
                        returnLogicalAddress = log.EnqueueAndWaitForCommit(entry);
                        break;
                    case EnqueueIteratorType.SpanByte:
                        Span<byte> spanEntry = entry;
                        returnLogicalAddress = log.EnqueueAndWaitForCommit(spanEntry);
                        break;
                    case EnqueueIteratorType.SpanBatch:
                        returnLogicalAddress = log.EnqueueAndWaitForCommit(spanBatch);
                        break;
                    default:
                        Assert.Fail("Unknown EnqueueIteratorType");
                        break;
                }

                if (returnLogicalAddress == 0)
                    Assert.Fail("LogWriter: Returned Logical Address = 0");
            }
            catch (Exception ex)
            {
                Assert.Fail("EnqueueAndWaitForCommit had exception:" + ex.Message);
            }

        }
    }
}



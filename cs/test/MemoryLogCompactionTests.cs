// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;
using NUnit.Framework;
using System.Buffers;

namespace FASTER.test
{
    [TestFixture]
    internal class MemoryLogCompactionTests
    {
        private FasterKV<ReadOnlyMemory<int>, Memory<int>> fht;
        private IDevice log;
        private string path;

        [SetUp]
        public void Setup()
        {

            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path);
        }

#if false // TODOtest
        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        public void MemoryLogCompactionTest1([Values] TestUtils.DeviceType deviceType, [Values] CompactionType compactionType)
        {

            string filename = path + "MemoryLogCompactionTests1" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<ReadOnlyMemory<int>, Memory<int>>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 12, PageSizeBits = 10, SegmentSizeBits = 22 });

            using var session = fht.For(new MemoryCompaction()).NewSession<MemoryCompaction>();

            var key = new Memory<int>(new int[20]);
            var value = new Memory<int>(new int[20]);

            const int totalRecords = 200; 
            var start = fht.Log.TailAddress;

            for (int i = 0; i < totalRecords; i++)
            {
                key.Span.Fill(i);
                value.Span.Fill(i);
                session.Upsert(key, value);
                if (i < 5)  
                    session.Delete(key); // in-place delete
            }

            for (int i = 5; i < 10; i++)  
            {
                key.Span.Fill(i);
                value.Span.Fill(i);
                session.Delete(key); // tombstone inserted
            }

            // Compact log
            var compactUntil = fht.Log.BeginAddress + (fht.Log.TailAddress - fht.Log.BeginAddress) / 5;
            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.Truncate();

            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read total keys - all but first 5 (deleted) should be present
            for (int i = 0; i < totalRecords; i++)
            {
                key.Span.Fill(i);

                var (status, output) = session.Read(key, userContext: i < 10 ? 1 : 0); 
                if (status != Status.PENDING)
                {
                    if (i < 10)
                        Assert.AreEqual(Status.NOTFOUND, status);
                    else
                    {
                        Assert.AreEqual(Status.OK, status);
                        Assert.IsTrue(output.Item1.Memory.Span.Slice(0, output.Item2).SequenceEqual(key.Span));
                        output.Item1.Dispose();
                    }
                }
            }
            session.CompletePending(true);

            // Test iteration of distinct live keys
            using (var iter = session.Iterate())
            {
                int count = 0;
                while (iter.GetNext(out RecordInfo recordInfo))
                {
                    var k = iter.GetKey();
                    Assert.GreaterOrEqual(k.Span[0], 10); 
                    count++;
                }
                Assert.AreEqual(190, count); 
            }

            // Test iteration of all log records
            using (var iter = fht.Log.Scan(fht.Log.BeginAddress, fht.Log.TailAddress))
            {
                int count = 0;
                while (iter.GetNext(out RecordInfo recordInfo))
                {
                    var k = iter.GetKey();
                    Assert.GreaterOrEqual(k.Span[0], 5);  
                    count++;
                }
                // Includes 190 live records + 5 deleted records
                Assert.AreEqual(195, count);  
            }
        }
#endif
    }

    public class MemoryCompaction : MemoryFunctions<ReadOnlyMemory<int>, int, int>
    {
        public override void RMWCompletionCallback(ref ReadOnlyMemory<int> key, ref Memory<int> input, ref (IMemoryOwner<int>, int) output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
        }

        public override void ReadCompletionCallback(ref ReadOnlyMemory<int> key, ref Memory<int> input, ref (IMemoryOwner<int>, int) output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            try
            {
                if (ctx == 0)
                {
                    Assert.AreEqual(Status.OK, status);
                    Assert.IsTrue(output.Item1.Memory.Span.Slice(0, output.Item2).SequenceEqual(key.Span));
                }
                else
                {
                    Assert.AreEqual(Status.NOTFOUND, status);
                }
            }
            finally
            {
                if (status == Status.OK) output.Item1.Dispose();
            }
        }
    }
}

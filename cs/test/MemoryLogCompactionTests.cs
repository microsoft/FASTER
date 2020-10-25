// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Buffers;

namespace FASTER.test
{

    [TestFixture]
    internal class MemoryLogCompactionTests
    {
        private FasterKV<ReadOnlyMemory<int>, Memory<int>> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\MemoryLogCompactionTests1.log", deleteOnClose: true);
            fht = new FasterKV<ReadOnlyMemory<int>, Memory<int>>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Dispose();
        }

        [Test]
        public void MemoryLogCompactionTest1()
        {
            using var session = fht.For(new MemoryCompaction()).NewSession<MemoryCompaction>();

            var key = new Memory<int>(new int[20]);
            var value = new Memory<int>(new int[20]);

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                key.Span.Fill(i);
                value.Span.Fill(i);
                session.Upsert(key, value);
            }
            session.Compact(compactUntil, true);

            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                key.Span.Fill(i);
                value.Span.Fill(i);

                var (status, output) = session.Read(key);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.Item1.Memory.Span.Slice(0, output.Item2).SequenceEqual(key.Span));
                }
            }
        }
    }

    public class MemoryCompaction : MemoryFunctions<ReadOnlyMemory<int>, int, int>
    {
        public override void RMWCompletionCallback(ref ReadOnlyMemory<int> key, ref Memory<int> input, int ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public override void ReadCompletionCallback(ref ReadOnlyMemory<int> key, ref Memory<int> input, ref (IMemoryOwner<int>, int) output, int ctx, Status status)
        {
            try
            {
                if (ctx == 0)
                {
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.Item1.Memory.Span.Slice(0, output.Item2).SequenceEqual(key.Span));
                }
                else
                {
                    Assert.IsTrue(status == Status.NOTFOUND);
                }
            }
            finally
            {
                if (status == Status.OK) output.Item1.Dispose();
            }
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System.IO;
using static FASTER.test.TestUtils;

namespace FASTER.test.SingleWriter
{
    internal class SingleWriterTestFunctions : SimpleFunctions<int, int>
    {
        internal int actualExecuted = 0;

        public override void SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address, WriteReason reason)
        {
            Assert.AreEqual((WriteReason)input, reason);
            actualExecuted |= 1 << input;
        }

        public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address, WriteReason reason)
        {
            Assert.AreEqual((WriteReason)input, reason);
            actualExecuted |= 1 << input;
        }
    }

    class SingleWriterTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;

        SingleWriterTestFunctions functions;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, SingleWriterTestFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            functions = new SingleWriterTestFunctions();

            ReadCacheSettings readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings, CopyReadsToTail = CopyReadsToTail.FromStorage });
            session = fht.For(functions).NewSession<SingleWriterTestFunctions>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            int input = (int)WriteReason.Upsert;
            int output = 0;
            for (int key = 0; key < numRecords; key++)
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, input, key * valueMult, ref output));
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void SingleWriterReasonsTest()
        {
            int expectedExecuted = 0;

            expectedExecuted |= 1 << (int)WriteReason.Upsert;
            Populate();
            Assert.AreEqual(expectedExecuted, functions.actualExecuted);

            fht.Log.FlushAndEvict(wait: true);

            int key = 42;
            int input = (int)WriteReason.CopyToReadCache;
            expectedExecuted |= 1 << input;
            var status = session.Read(key, input, out int output);
            Assert.AreEqual(Status.PENDING, status);
            session.CompletePending(wait: true);
            Assert.AreEqual(expectedExecuted, functions.actualExecuted);

            key = 64;
            input = (int)WriteReason.CopyToTail;
            expectedExecuted |= 1 << input;
            RecordMetadata recordMetadata = default;
            status = session.Read(ref key, ref input, ref output, ref recordMetadata, ReadFlags.CopyToTail);
            Assert.AreEqual(Status.PENDING, status);
            session.CompletePending(wait: true);
            Assert.AreEqual(expectedExecuted, functions.actualExecuted);

            input = (int)WriteReason.Compaction;
            expectedExecuted |= 1 << input;
            fht.Log.Compact<int, int, Empty, SingleWriterTestFunctions>(functions, ref input, ref output, fht.Log.SafeReadOnlyAddress, CompactionType.Scan);
            Assert.AreEqual(expectedExecuted, functions.actualExecuted);
        }
    }
}

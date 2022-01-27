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
        internal WriteReason actualReason;

        public override void SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address, WriteReason reason)
        {
            Assert.AreEqual((WriteReason)input, reason);
            actualReason = reason;
        }

        public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address, WriteReason reason)
        {
            Assert.AreEqual((WriteReason)input, reason);
            actualReason = reason;
        }
    }

    class SingleWriterTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;
        const WriteReason NoReason = (WriteReason)(-1);

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
            ReadCacheSettings readCacheSettings = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    break;
                }
            }

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
        [Category(FasterKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void SingleWriterReasonsTest([Values] ReadCopyDestination readCopyDestination)
        {
            functions.actualReason = NoReason;
            Populate();
            Assert.AreEqual(WriteReason.Upsert, functions.actualReason);

            fht.Log.FlushAndEvict(wait: true);

            functions.actualReason = NoReason;
            int key = 42;
            WriteReason expectedReason = readCopyDestination == ReadCopyDestination.ReadCache ? WriteReason.CopyToReadCache : WriteReason.CopyToTail;
            int input = (int)expectedReason;
            var status = session.Read(key, input, out int output);
            Assert.AreEqual(Status.PENDING, status);
            session.CompletePending(wait: true);
            Assert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            key = 64;
            expectedReason = WriteReason.CopyToTail;
            input = (int)expectedReason;
            RecordMetadata recordMetadata = default;
            status = session.Read(ref key, ref input, ref output, ref recordMetadata, ReadFlags.CopyToTail);
            Assert.AreEqual(Status.PENDING, status);
            session.CompletePending(wait: true);
            Assert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            expectedReason = WriteReason.Compaction;
            input = (int)expectedReason;
            fht.Log.Compact<int, int, Empty, SingleWriterTestFunctions>(functions, ref input, ref output, fht.Log.SafeReadOnlyAddress, CompactionType.Scan);
            Assert.AreEqual(expectedReason, functions.actualReason);
        }
    }
}

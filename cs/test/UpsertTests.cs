// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System.IO;
using System.Threading.Tasks;

namespace FASTER.test.UpsertTests
{
    [TestFixture]    
    class UpsertTests
    {
        const int AddValue = 10_000;
        const int MultValue = 100;
        const int NumRecs = 10;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, UpsertInputFunctions> session;
        private IDevice log;

        internal class UpsertInputFunctions : FunctionsBase<int, int, int, int, Empty>
        {
            public override bool ConcurrentReader(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
                => SingleReader(ref key, ref input, ref value, ref output, ref recordInfo, address);

            /// <inheritdoc/>
            public override bool SingleReader(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
            {
                Assert.AreEqual(key * input, value);
                output = value + AddValue;
                return true;
            }

            /// <inheritdoc/>
            public override bool ConcurrentWriter(ref int key, ref int input, ref int src, ref int dst, ref RecordInfo recordInfo, long address)
            {
                SingleWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);
                return true;
            }
            /// <inheritdoc/>
            public override void SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref RecordInfo recordInfo, long address) => dst = src * input;
            /// <inheritdoc/>
            public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref RecordInfo recordInfo, long address) => Assert.AreEqual(key * input, dst);
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = TestUtils.CreateTestDevice(TestUtils.DeviceType.LocalMemory, Path.Combine(TestUtils.MethodTestDir, "Device.log"));
            fht = new FasterKV<int, int>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new UpsertInputFunctions()).NewSession<UpsertInputFunctions>();
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        // Simple Upsert test with Input
        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public async Task UpsertWithInputsTest([Values]bool isAsync)
        {
            int input = MultValue;
            int output = -1;

            async Task doWrites()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    if (isAsync)
                    {
                        var r = await session.UpsertAsync(ref key, ref input, ref key);
                        while (r.Status == Status.PENDING)
                            r = await r.CompleteAsync();
                    }
                    else
                        session.Upsert(ref key, ref input, ref key);
                }
            }

            void doReads()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    session.Read(ref key, ref input, ref output);
                    Assert.AreEqual(key * input + AddValue, output);
                }
            }

            // SingleWriter (records do not yet exist)
            await doWrites();
            doReads();

            // ConcurrentWriter (update existing records)
            await doWrites();
            doReads();
        }
    }
}

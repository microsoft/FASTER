// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System.IO;
using System.Threading.Tasks;

namespace FASTER.test.InputOutputParameterTests
{
    [TestFixture]    
    class InputOutputParameterTests
    {
        const int AddValue = 10_000;
        const int MultValue = 100;
        const int NumRecs = 10;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, UpsertInputFunctions> session;
        private IDevice log;

        internal class UpsertInputFunctions : FunctionsBase<int, int, int, int, Empty>
        {
            internal long lastWriteAddress;

            public override bool ConcurrentReader(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
            {
                lastWriteAddress = address;
                return SingleReader(ref key, ref input, ref value, ref output, ref recordInfo, address);
            }

            /// <inheritdoc/>
            public override bool SingleReader(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
            {
                Assert.AreEqual(key * input, value);
                lastWriteAddress = address;
                output = value + AddValue;
                return true;
            }

            /// <inheritdoc/>
            public override bool ConcurrentWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address)
            {
                SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);
                return true;
            }
            /// <inheritdoc/>
            public override void SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address)
            {
                lastWriteAddress = address;
                dst = output = src * input;
            }

            /// <inheritdoc/>
            public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, long address)
            {
                Assert.AreEqual(lastWriteAddress, address);
                Assert.AreEqual(key * input, dst);
                Assert.AreEqual(dst, output);
            }

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
            {
                InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
                return true;
            }
            public override void InitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
            {
                lastWriteAddress = address;
                value = output = key * input;
            }
            /// <inheritdoc/>
            public override void PostInitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, long address)
            {
                Assert.AreEqual(lastWriteAddress, address);
                Assert.AreEqual(key * input, value);
                Assert.AreEqual(value, output);
            }
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
        public async Task InputOutputParametersTest([Values]bool useRMW, [Values]bool isAsync)
        {
            int input = MultValue;
            Status status;
            int output = -1;
            bool loading = true;

            async Task doWrites()
            {
                for (int key = 0; key < NumRecs; ++key)
                {
                    var tailAddress = this.fht.Log.TailAddress;
                    RecordMetadata recordMetadata;
                    if (isAsync)
                    {
                        if (useRMW)
                        {
                            var r = await session.RMWAsync(ref key, ref input);
                            if ((key & 0x1) == 0)
                            {
                                while (r.Status == Status.PENDING)
                                    r = await r.CompleteAsync();
                                status = r.Status;
                                output = r.Output;
                                recordMetadata = r.RecordMetadata;
                            }
                            else
                            {
                                (status, output) = r.Complete(out recordMetadata);
                            }
                        }
                        else
                        {
                            var r = await session.UpsertAsync(ref key, ref input, ref key);
                            if ((key & 0x1) == 0)
                            {
                                while (r.Status == Status.PENDING)
                                    r = await r.CompleteAsync();
                                status = r.Status;
                                output = r.Output;
                                recordMetadata = r.RecordMetadata;
                            }
                            else
                            {
                                (status, output) = r.Complete(out recordMetadata);
                            }
                        }
                    }
                    else
                    {
                        status = useRMW
                            ? session.RMW(ref key, ref input, ref output, out recordMetadata)
                            : session.Upsert(ref key, ref input, ref key, ref output, out recordMetadata);
                    }
                    Assert.AreEqual(loading && useRMW ? Status.NOTFOUND : Status.OK, status);
                    Assert.AreEqual(key * input, output);
                    if (loading)
                        Assert.AreEqual(tailAddress, session.functions.lastWriteAddress);
                    Assert.AreEqual(session.functions.lastWriteAddress, recordMetadata.Address);
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

            loading = false;
            input *= input;

            // ConcurrentWriter (update existing records)
            await doWrites();
            doReads();
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.InteropServices;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class SpanByteTests
    {
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void SpanByteTest1()
        {
            Span<byte> output = stackalloc byte[20];
            SpanByte input = default;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);

            try
            {
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog1.log", deleteOnClose: true);
                using var fht = new FasterKV<SpanByte, SpanByte>
                    (128, new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 });
                using var s = fht.NewSession(new SpanByteFunctions<Empty>());

                var key1 = MemoryMarshal.Cast<char, byte>("key1".AsSpan());
                var value1 = MemoryMarshal.Cast<char, byte>("value1".AsSpan());
                var output1 = SpanByteAndMemory.FromFixedSpan(output);

                s.Upsert(key1, value1);

                s.Read(key1, ref input, ref output1);

                Assert.IsTrue(output1.IsSpanByte);
                Assert.IsTrue(output1.SpanByte.AsReadOnlySpan().SequenceEqual(value1));

                var key2 = MemoryMarshal.Cast<char, byte>("key2".AsSpan());
                var value2 = MemoryMarshal.Cast<char, byte>("value2value2value2".AsSpan());
                var output2 = SpanByteAndMemory.FromFixedSpan(output);

                s.Upsert(key2, value2);
                s.Read(key2, ref input, ref output2);

                Assert.IsTrue(!output2.IsSpanByte);
                Assert.IsTrue(output2.Memory.Memory.Span.Slice(0, output2.Length).SequenceEqual(value2));
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void MultiReadSpanByteKeyTest()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/MultiReadSpanByteKeyTest.log", deleteOnClose: true);
                using var fht = new FasterKV<SpanByte, long>(
                    size: 1L << 10,
                    new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
                using var session = fht.For(new MultiReadSpanByteKeyTestFunctions()).NewSession<MultiReadSpanByteKeyTestFunctions>();

                for (int i = 0; i < 200; i++)
                {
                    var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                    fixed (byte* _ = key)
                        session.Upsert(SpanByte.FromFixedSpan(key), i);
                }

                // Evict all records to disk
                fht.Log.FlushAndEvict(true);

                for (long key = 0; key < 50; key++)
                {
                    // read each key multiple times
                    for (int i = 0; i < 10; i++)
                        Assert.AreEqual(key, ReadKey($"{key}"));
                }

                long ReadKey(string keyString)
                {
                    Status status;

                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    fixed (byte* _ = key)
                        status = session.Read(key: SpanByte.FromFixedSpan(key), out var unused);

                    // All keys need to be fetched from disk
                    Assert.IsTrue(status.IsPending);

                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);

                    var count = 0;
                    var value = 0L;
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            count++;
                            Assert.IsTrue(completedOutputs.Current.Status.IsFound);
                            value = completedOutputs.Current.Output;
                        }
                    }
                    Assert.AreEqual(1, count);
                    return value;
                }
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void SpanByteUnitTest1()
        {
            Span<byte> payload = stackalloc byte[20];
            Span<byte> serialized = stackalloc byte[24];

            SpanByte sb = SpanByte.FromFixedSpan(payload);
            Assert.IsFalse(sb.Serialized);
            Assert.AreEqual(20, sb.Length);
            Assert.AreEqual(24, sb.TotalSize);
            Assert.AreEqual(20, sb.AsSpan().Length);
            Assert.AreEqual(20, sb.AsReadOnlySpan().Length);

            fixed (byte* ptr = serialized)
                sb.CopyTo(ptr);
            ref SpanByte ssb = ref SpanByte.ReinterpretWithoutLength(serialized);
            Assert.IsTrue(ssb.Serialized);
            Assert.AreEqual(0, ssb.MetadataSize);
            Assert.AreEqual(20, ssb.Length);
            Assert.AreEqual(24, ssb.TotalSize);
            Assert.AreEqual(20, ssb.AsSpan().Length);
            Assert.AreEqual(20, ssb.AsReadOnlySpan().Length);

            ssb.MarkExtraMetadata();
            Assert.IsTrue(ssb.Serialized);
            Assert.AreEqual(8, ssb.MetadataSize);
            Assert.AreEqual(20, ssb.Length);
            Assert.AreEqual(24, ssb.TotalSize);
            Assert.AreEqual(20 - 8, ssb.AsSpan().Length);
            Assert.AreEqual(20 - 8, ssb.AsReadOnlySpan().Length);
            ssb.ExtraMetadata = 31337;
            Assert.AreEqual(31337, ssb.ExtraMetadata);

            sb.MarkExtraMetadata();
            Assert.AreEqual(20, sb.Length);
            Assert.AreEqual(24, sb.TotalSize);
            Assert.AreEqual(20 - 8, sb.AsSpan().Length);
            Assert.AreEqual(20 - 8, sb.AsReadOnlySpan().Length);
            sb.ExtraMetadata = 31337;
            Assert.AreEqual(31337, sb.ExtraMetadata);

            fixed (byte* ptr = serialized)
                sb.CopyTo(ptr);
            Assert.IsTrue(ssb.Serialized);
            Assert.AreEqual(8, ssb.MetadataSize);
            Assert.AreEqual(20, ssb.Length);
            Assert.AreEqual(24, ssb.TotalSize);
            Assert.AreEqual(20 - 8, ssb.AsSpan().Length);
            Assert.AreEqual(20 - 8, ssb.AsReadOnlySpan().Length);
            Assert.AreEqual(31337, ssb.ExtraMetadata);
        }

        class MultiReadSpanByteKeyTestFunctions : FunctionsBase<SpanByte, long, long, long, Empty>
        {
            public override bool SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref RecordInfo recordInfo, long address)
            {
                dst = value;
                return true;
            }

            public override bool ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref RecordInfo recordInfo, long address)
            {
                dst = value;
                return true;
            }
        }
    }
}

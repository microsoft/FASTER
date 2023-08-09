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

        class SpanByteKey_LongValue_TestFunctions : SpanByteFunctions_SimpleValue<long, Empty>
        {
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void MultiRead_SpanByteKey_LongValue_Test()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/test.log", deleteOnClose: true);
                using var fht = new FasterKV<SpanByte, long>(
                    size: 1L << 10,
                    new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
                using var session = fht.For(new SpanByteKey_LongValue_TestFunctions()).NewSession<SpanByteKey_LongValue_TestFunctions>();

                for (int i = 0; i < 200; i++)
                {
                    var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                    fixed (byte* _ = key)
                        session.Upsert(SpanByte.FromFixedSpan(key), i);
                }

                // Read, evict all records to disk, read again
                MultiRead(evicted: false);
                fht.Log.FlushAndEvict(true);
                MultiRead(evicted: true);

                void MultiRead(bool evicted)
                { 
                    for (long key = 0; key < 50; key++)
                    {
                        // read each key multiple times
                        for (int i = 0; i < 10; i++)
                            Assert.AreEqual(key, ReadKey($"{key}", evicted));
                    }
                }

                long ReadKey(string keyString, bool evicted)
                {
                    Status status;
                    long output;

                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    fixed (byte* _ = key)
                        status = session.Read(key: SpanByte.FromFixedSpan(key), out output);
                    Assert.AreEqual(evicted, status.IsPending, "evicted/pending mismatch");

                    if (!evicted)
                        Assert.IsTrue(status.Found, $"expected to find key; status = {status}");
                    else    // needs to be fetched from disk
                    { 
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        using (completedOutputs)
                        {
                            for (var count = 0; completedOutputs.Next(); ++count)
                            {
                                Assert.AreEqual(0, count, "should only have one record returned");
                                Assert.IsTrue(completedOutputs.Current.Status.Found);
                                output = completedOutputs.Current.Output;
                            }
                        }
                    }
                    return output;
                }
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        class LongKey_SpanByteValue_TestFunctions : SpanByteFunctions<long, Empty>
        {
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void MultiRead_LongKey_SpanByteValue_Test()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/test.log", deleteOnClose: true);
                using var fht = new FasterKV<long, SpanByte>(
                    size: 1L << 10,
                    new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
                using var session = fht.For(new LongKey_SpanByteValue_TestFunctions()).NewSession<LongKey_SpanByteValue_TestFunctions>();

                for (long key = 0; key < 200; key++)
                {
                    var value = MemoryMarshal.Cast<char, byte>($"{key}".AsSpan());
                    fixed (byte* _ = value)
                        session.Upsert(key, SpanByte.FromFixedSpan(value));
                }

                // Read, evict all records to disk, read again
                MultiRead(evicted: false);
                fht.Log.FlushAndEvict(true);
                MultiRead(evicted: true);

                void MultiRead(bool evicted)
                {
                    for (long key = 0; key < 50; key++)
                    {
                        // read each key multiple times
                        for (int i = 0; i < 10; i++)
                            Assert.AreEqual(key, ReadKey(key, evicted));
                    }
                }

                long ReadKey(long key, bool evicted)
                {
                    Status status = session.Read(key, out SpanByteAndMemory output);
                    Assert.AreEqual(evicted, status.IsPending, "evicted/pending mismatch");

                    if (!evicted)
                        Assert.IsTrue(status.Found, $"expected to find key; status = {status}");
                    else    // needs to be fetched from disk
                    {
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        using (completedOutputs)
                        {
                            for (var count = 0; completedOutputs.Next(); ++count)
                            {
                                Assert.AreEqual(0, count, "should only have one record returned");
                                Assert.IsTrue(completedOutputs.Current.Status.Found);
                                output = completedOutputs.Current.Output;
                            }
                        }
                    }
                    var valueChars = MemoryMarshal.Cast<byte, char>(output.Memory.Memory.Span);
                    return long.Parse(valueChars);
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
    }
}

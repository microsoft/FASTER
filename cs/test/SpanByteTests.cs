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
        public unsafe void SpanByteTest1()
        {
            Span<byte> output = stackalloc byte[20];
            SpanByte input = default;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);

            {   // Directory lifetime scope
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        public unsafe void MultiReadSpanByteKeyTest()
        {
            {   // Directory lifetime scope
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/MultiReadSpanByteKeyTest.log", deleteOnClose: true);
                using var fht = new FasterKV<SpanByte, long>(
                    size: 1L << 20,
                    new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
                using var session = fht.For(new MultiReadSpanByteKeyTestFunctions()).NewSession<MultiReadSpanByteKeyTestFunctions>();

                for (int i = 0; i < 3000; i++)
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
                    Assert.AreEqual(Status.PENDING, status);

                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);

                    var count = 0;
                    var value = 0L;
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            count++;
                            Assert.AreEqual(Status.OK, completedOutputs.Current.Status);
                            value = completedOutputs.Current.Output;
                        }
                    }
                    Assert.AreEqual(1, count);
                    return value;
                }
            }
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        class MultiReadSpanByteKeyTestFunctions : FunctionsBase<SpanByte, long, long, long, Empty>
        {
            public override void SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst) => dst = value;
            public override void ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst) => dst = value;
        }
    }
}

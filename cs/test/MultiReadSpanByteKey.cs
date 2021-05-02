// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class MultiReadSpanByteKey
    {
        class Functions : FunctionsBase<SpanByte, long, long, long, Empty>
        {
            public override void SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst) => dst = value;
            public override void ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst) => dst = value;
        }

        [Test]
        public unsafe void MultiReadSpanByteKeyTest()
        {
            var log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/BasicFasterTests.log", deleteOnClose: true);
            var fht = new FasterKV<SpanByte, long>(
                size: 1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    MemorySizeBits = 15,
                    PageSizeBits = 12,
                });
            var session = fht.For(new Functions()).NewSession<Functions>();

            // write enough records to be flushed to disk
            for (int i = 0; i < 5000; i++)
            {
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                fixed (byte* f = key)
                    session.Upsert(SpanByte.FromFixedSpan(key), i);
            }
            session.CompletePending(wait: true);

            // read "5" twice successfully
            Assert.AreEqual(5, ReadKey5());
            Assert.AreEqual(5, ReadKey5());

            // BUT not found on the third time
            Assert.AreEqual(5, ReadKey5());

            long ReadKey5()
            {
                var keyString = $"5";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());

                fixed (byte* f = key)
                {
                    var status = session.Read(key: SpanByte.FromFixedSpan(key), out var unused);
                    // key low enough to need to be fetched from disk
                    Assert.AreEqual(Status.PENDING, status);
                }

                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);

                var count = 0;
                var value = 0L;
                while (completedOutputs.Next())
                {
                    count++;
                    ref var completedKey = ref completedOutputs.Current.Key;
                    var completedStringKey = new string(MemoryMarshal.Cast<byte, char>(completedKey.AsReadOnlySpan()).ToArray());
                    Assert.AreEqual(Status.OK, completedOutputs.Current.Status);
                    Assert.AreEqual(5, completedOutputs.Current.Output);
                    value = completedOutputs.Current.Output;
                }
                completedOutputs.Dispose();

                Assert.AreEqual(1, count);
                return value;
            }
        }
    }
}

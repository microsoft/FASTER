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
using System.Runtime.InteropServices;

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

            FasterKV<SpanByte, SpanByte> fht;
            IDevice log;
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/hlog1.log", deleteOnClose: true);
            fht = new FasterKV<SpanByte, SpanByte>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 });

            var s = fht.NewSession(new SpanByteFunctions<Empty>());

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


            s.Dispose();
            fht.Dispose();
            fht = null;
            log.Dispose();
        }        
    }
}

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
        public unsafe void SpanByteTest1()
        {
            Span<byte> key = stackalloc byte[20];
            Span<byte> value = stackalloc byte[20];
            Span<byte> output = stackalloc byte[20];
            SpanByte input = default;

            FasterKV<SpanByte, SpanByte> fht;
            IDevice log;
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<SpanByte, SpanByte>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 });

            var s = fht.NewSession(new SpanByteFunctions<Empty>());

            SpanByte.Copy(MemoryMarshal.Cast<char, byte>("key1".AsSpan()), key);
            SpanByte.Copy(MemoryMarshal.Cast<char, byte>("value1".AsSpan()), value);
            var output_ = SpanByteAndMemory.FromFixedSpan(output);

            s.Upsert(key, value);

            s.Read(key, ref input, ref output_);

            Assert.IsTrue(output_.IsSpanByte);
            Assert.IsTrue(output.SequenceEqual(value));

            s.Dispose();
            fht.Dispose();
            fht = null;
            log.Dispose();
        }        
    }
}

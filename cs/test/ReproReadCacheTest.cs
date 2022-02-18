// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.ReadCacheTests
{
    [TestFixture]
    internal class RandomReadCacheTest
    {
        public class Context
        {
            public Status Status { get; set; }
        }

        class Functions : FunctionsBase<SpanByte, long, long, long, Context>
        {
            public override bool ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref RecordInfo recordInfo, long address)
            {
                dst = value;
                return true;
            }

            public override bool SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref RecordInfo recordInfo, long address)
            {
                dst = value;
                return true;
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref long input, ref long output, Context context, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(input, output);
                context.Status = status;
            }
        }

        [Test]
        [Category("FasterKV")]

        public unsafe void RandomReadCacheTest1()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BasicFasterTests.log", deleteOnClose: true);
            var fht = new FasterKV<SpanByte, long>(
                size: 1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    MemorySizeBits = 15,
                    PageSizeBits = 12,

                    ReadCacheSettings = new ReadCacheSettings
                    {
                        MemorySizeBits = 15,
                        PageSizeBits = 12,
                        SecondChanceFraction = 0.1,
                    }
                });

            var session = fht.For(new Functions()).NewSession<Functions>();

            void Read(int i)
            {
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                fixed (byte* _ = key)
                {
                    var context = new Context();
                    var sb = SpanByte.FromFixedSpan(key);
                    long input = i * 2;
                    long output = 0;
                    var status = session.Read(ref sb, ref input, ref output, context);

                    if (status.Found)
                    {
                        Assert.AreEqual(input, output);
                        return;
                    }

                    Assert.IsTrue(status.Pending, $"was not OK or PENDING: {keyString}");

                    session.CompletePending(wait: true);
                }
            }

            var num = 30000;

            // write the values
            for (int i = 0; i < num; i++)
            {
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                fixed (byte* _ = key)
                {
                    var sb = SpanByte.FromFixedSpan(key);
                    var status = session.Upsert(sb, i * 2);
                    Assert.IsTrue(!status.Found && status.CreatedRecord, status.ToString());
                }
            }

            // read through the keys in order (works)
            for (int i = 0; i < num; i++)
            {
                Read(i);
            }

            // pick random keys to read
            var r = new Random(2115);
            for (int i = 0; i < num; i++)
            {
                Read(r.Next(num));
            }

            fht.Dispose();
            log.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System.Runtime.InteropServices;
using System;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class SpanByteLogScanTests
    {
        private FasterKV<SpanByte, SpanByte> fht;
        private IDevice log;
        const int numRecords = 200;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);
            fht = new FasterKV<SpanByte, SpanByte>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 20, PageSizeBits = 15 }, lockingMode: LockingMode.None);
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]

        public unsafe void BlittableScanJumpToBeginAddressTest()
        {
            using var session = fht.For(new SpanByteFunctions<Empty>()).NewSession<SpanByteFunctions<Empty>>();

            const int numRecords = 200;
            const int numTailRecords = 10;
            long shiftBeginAddressTo = 0;
            int shiftToKey = 0;
            for (int i = 0; i < numRecords; i++)
            {
                if (i == numRecords - numTailRecords)
                {
                    shiftBeginAddressTo = fht.Log.TailAddress;
                    shiftToKey = i;
                }

                var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                session.Upsert(key, value);
            }

            using var iter = fht.Log.Scan(fht.Log.HeadAddress, fht.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                Assert.IsTrue(iter.GetNext(out var recordInfo));
                Assert.AreEqual(i, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetKey().AsSpan())));
                Assert.AreEqual(i, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetValue().AsSpan())));
            }

            fht.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                Assert.IsTrue(iter.GetNext(out var recordInfo));
                if (i == 0)
                    Assert.AreEqual(fht.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                Assert.AreEqual(expectedKey, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetKey().AsSpan())));
                Assert.AreEqual(expectedKey, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetValue().AsSpan())));
            }
        }
    }
}

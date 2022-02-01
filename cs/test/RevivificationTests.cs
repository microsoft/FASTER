// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.IO;
using static FASTER.test.TestUtils;

namespace FASTER.test.Revivification
{
    [TestFixture]
    class RevivificationFixedLenTests
    {
        internal class RevivificationFixedLenFunctions : SimpleFunctions<int, int>
        {
        }

        const int numRecords = 1000;
        internal const int valueMult = 1_000_000;

        RevivificationFixedLenFunctions functions;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, RevivificationFixedLenFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 },
                                            supportsLocking: true);
            functions = new RevivificationFixedLenFunctions();
            session = fht.For(functions).NewSession<RevivificationFixedLenFunctions>();
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

            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void FixedLenSimpleTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = fht.Log.TailAddress;
            session.Delete(42);
            // TODO: figure out how to know if it was the first record in the chain or mid-chain
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(54, 5400);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(54, 5400);
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }
    }

    [TestFixture]
    class RevivificationVarLenTests
    {
        const int LengthPadding = 25;
        const int InitialLength = 50;
        const int GrowLength = InitialLength + LengthPadding;
        const int ShrinkLength = InitialLength - LengthPadding;

        internal class RevivificationSpanByteFunctions : SpanByteFunctions<Empty>
        {
            internal FasterKV<SpanByte, SpanByte> fht;
            internal RevivificationVLS vls;

            internal int expectedConcurrentDestLength = InitialLength;
            internal int expectedSingleDestLength = InitialLength;
            internal int expectedInputLength = InitialLength;
            internal int expectedFullLength = -1;

            internal RevivificationSpanByteFunctions(FasterKV<SpanByte, SpanByte> fht, RevivificationVLS vls)
            {
                this.fht = fht;
                this.vls = vls;
            }

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedSingleDestLength, dst.Length);
                if (expectedFullLength > 0)
                    Assert.AreEqual(expectedFullLength, fullLength);
                Assert.AreEqual(0, usedLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                input.CopyTo(ref dst);
                usedLength = dst.Length;
                return true;
            }

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedSingleDestLength, value.Length);
                if (expectedFullLength > 0)
                    Assert.AreEqual(expectedFullLength, fullLength);
                Assert.AreEqual(0, usedLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                input.CopyTo(ref value);
                usedLength = value.Length;
                return true;
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedSingleDestLength, newValue.Length);
                if (expectedFullLength > 0)
                    Assert.AreEqual(expectedFullLength, fullLength);
                Assert.AreEqual(0, usedLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                input.CopyTo(ref newValue);
                usedLength = newValue.Length;
                return true;
            }

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedConcurrentDestLength, dst.Length);
                if (expectedFullLength > 0)
                    Assert.AreEqual(expectedFullLength, fullLength);
                Assert.AreEqual(FasterKV<SpanByte, SpanByte>.RoundupLength(expectedConcurrentDestLength), usedLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                if (input.Length > dst.Length)
                    return false;
                input.CopyTo(ref dst);
                usedLength = dst.Length;
                return true;
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                if (expectedFullLength > 0)
                    Assert.AreEqual(expectedFullLength, fullLength);
                Assert.AreEqual(FasterKV<SpanByte, SpanByte>.RoundupLength(expectedConcurrentDestLength), usedLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);
                usedLength = value.Length;
                return true;
            }
        }

        /// <summary>
        /// Callback for length computation based on value and input.
        /// </summary>
        public class RevivificationVLS : IVariableLengthStruct<SpanByte, SpanByte>
        {
            /// <summary>
            /// Initial length of value, when populated using given input number.
            /// We include sizeof(int) for length header.
            /// </summary>
            public int GetInitialLength(ref SpanByte input) => sizeof(int) + input.Length;

            /// <summary>
            /// Length of resulting object when doing RMW with given value and input.
            /// For ASCII sum, output is one digit longer than the max of input and old value.
            /// </summary>
            public int GetLength(ref SpanByte t, ref SpanByte input) => sizeof(int) + input.Length;
        }

        public enum ChainPosition { First, Middle }

        const int numRecords = 200;

        RevivificationSpanByteFunctions functions;

        private FasterKV<SpanByte, SpanByte> fht;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            fht = new FasterKV<SpanByte, SpanByte>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 },
                                            supportsLocking: true, maxFreeRecordsInBin: 1024);

            var valueVLS = new RevivificationVLS();
            functions = new RevivificationSpanByteFunctions(fht, valueVLS);
            session = fht.For(functions).NewSession<RevivificationSpanByteFunctions>(
                    sessionVariableLengthStructSettings: new SessionVariableLengthStructSettings<SpanByte, SpanByte> { valueLength = valueVLS });
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

            DeleteDirectory(MethodTestDir);
        }

        void Populate() => Populate(0, numRecords);

        void Populate(int from, int to)
        {
            Span<byte> keyVec = stackalloc byte[10];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            for (int ii = from; ii < to; ++ii)
            {
                keyVec.Fill((byte)ii);

                // Use input for value because CreateNewRow* doesn't use Input
                Assert.AreNotEqual(Status.PENDING, session.Upsert(ref key, ref input, ref input, ref output));
            }
        }

        public enum Growth { None, Grow, Shrink };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenNoRevivLengthTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] Growth growth)
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill(42);
            var key = SpanByte.FromFixedSpan(keyVec);

            // Do NOT delete; this is a no-reviv test of lengths

            functions.expectedInputLength = growth switch
            {
                Growth.None => InitialLength,
                Growth.Grow => GrowLength,
                Growth.Shrink => ShrinkLength,
                _ => -1
            };

            functions.expectedSingleDestLength = functions.expectedInputLength;
            functions.expectedConcurrentDestLength = InitialLength; // This is from the initial Populate()

            Span<byte> inputVec = stackalloc byte[functions.expectedInputLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            if (growth == Growth.Shrink)
            {
                // Now let's see if we have the correct expected extra length in the destination.
                functions.expectedInputLength = InitialLength;
                inputVec = stackalloc byte[functions.expectedInputLength];
                input = SpanByte.FromFixedSpan(inputVec);
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenSimpleTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = fht.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill(42);
            var key = SpanByte.FromFixedSpan(keyVec);
            session.Delete(ref key);

            // TODO: figure out how to know if it was the first record in the chain or mid-chain
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedFullLength = FasterKV<SpanByte, SpanByte>.RoundupLength(InitialLength);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenReadOnlyMinAddressTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = fht.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill(42);
            var key = SpanByte.FromFixedSpan(keyVec);
            session.Delete(ref key);

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
            fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);

            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedFullLength = FasterKV<SpanByte, SpanByte>.RoundupLength(InitialLength);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.Greater(fht.Log.TailAddress, tailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenCopyUpdaterTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            Populate(0, numRecords / 2);
            fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
            Populate(numRecords / 2 + 1, numRecords);

            var tailAddress = fht.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill((byte)(numRecords / 2 + 2));
            var key = SpanByte.FromFixedSpan(keyVec);
            session.Delete(ref key);

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedFullLength = FasterKV<SpanByte, SpanByte>.RoundupLength(InitialLength);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            keyVec.Fill((byte)(numRecords / 2 - 2));

            session.ctx.phase = phase;
            session.RMW(ref key, ref input);
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }
    }
}

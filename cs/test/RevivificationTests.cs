// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using static FASTER.core.Utility;
using static FASTER.test.TestUtils;

namespace FASTER.test.Revivification
{
    public enum DeleteDest { FreeList, InChain };

    public enum CollisionRange { Ten = 10, None = int.MaxValue }

    public enum BumpEpochMode { Bump, NoBump };

    static class RevivificationTestUtils
    {
        internal static RMWInfo CopyToRMWInfo(ref UpsertInfo upsertInfo)
            => new()
            {
                Version = upsertInfo.Version,
                SessionID = upsertInfo.SessionID,
                Address = upsertInfo.Address,
                KeyHash = upsertInfo.KeyHash,
                RecordInfo = default,
                UsedValueLength = upsertInfo.UsedValueLength,
                FullValueLength = upsertInfo.FullValueLength,
                Action = RMWAction.Default,
            };

        internal static FreeRecordPool<TKey, TValue> SwapFreeRecordPool<TKey, TValue>(FasterKV<TKey, TValue> fkv, FreeRecordPool<TKey, TValue> pool)
        {
            var temp = fkv.FreeRecordPool;
            fkv.FreeRecordPool = pool;
            return temp;
        }

        internal static FreeRecordPool<TKey, TValue> CreateSingleBinFreeRecordPool<TKey, TValue>(FasterKV<TKey, TValue> fkv, RevivificationBin binDef, int fixedRecordLength = 0)
            => new (fkv, new RevivificationSettings() { FreeListBins = new[] { binDef } }, fixedRecordLength);

        internal static void WaitForEpochBump<TKey, TValue>(FasterKV<TKey, TValue> fkv)
            => WaitForEpochBump(fkv, fkv.epoch.CurrentEpoch + 1);

        internal static void WaitForEpochBump<TKey, TValue>(FasterKV<TKey, TValue> fkv, long targetEpoch, int timeoutMs = FreeRecordPool<int, int>.DefaultBumpIntervalMs * 2)
        {
            // Wait until the timer calls BumpCurrentEpoch.
            var sw = new Stopwatch();
            sw.Start();
            while (fkv.epoch.CurrentEpoch < targetEpoch)
            {
                Assert.Less(sw.ElapsedMilliseconds, timeoutMs, "Timeout while waiting for BumpCurrentEpoch");
                Thread.Yield();
            }
        }

        internal static void BumpCurrentEpoch<TKey, TValue>(FasterKV<TKey, TValue> fkv)
        {
            // For many tests, they either enqueue a single free record enqueue so would wait FreeRecordPool.DefaultBumpIntervalMs,
            // or call this within a loop. Either way this slows the tests too much, other than a few that specifically test this,
            // so just bump directly.
            //RevivificationTestUtils.WaitForEpochBump(fkv);
            fkv.epoch.BumpCurrentEpoch();
        }
    }

    internal readonly struct RevivificationSpanByteComparer : IFasterEqualityComparer<SpanByte>
    {
        private readonly SpanByteComparer defaultComparer;
        private readonly int collisionRange;

        internal RevivificationSpanByteComparer(CollisionRange range)
        {
            this.defaultComparer = new SpanByteComparer();
            this.collisionRange = (int)range;
        }

        public bool Equals(ref SpanByte k1, ref SpanByte k2) => defaultComparer.Equals(ref k1, ref k2);

        // The hash code ends with 0 so mod Ten isn't so helpful, so shift
        public long GetHashCode64(ref SpanByte k) => (defaultComparer.GetHashCode64(ref k) >> 4) % this.collisionRange;
    }

    /// <summary>
    /// Callback for length computation based on value and input.
    /// </summary>
    public class RevivificationVLS : IVariableLengthStruct<SpanByte, SpanByte>
    {
        public int GetInitialLength(ref SpanByte input) => input.TotalSize;

        public int GetLength(ref SpanByte value, ref SpanByte input) => input.TotalSize;
    }

    [TestFixture]
    class RevivificationFixedLenTests
    {
        internal class RevivificationFixedLenFunctions : SimpleFunctions<int, int>
        {
        }

        const int numRecords = 1000;
        internal const int valueMult = 1_000_000;

        RevivificationFixedLenFunctions functions;

        private FasterKV<int, int> fkv;
        private ClientSession<int, int, int, int, Empty, RevivificationFixedLenFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            var lockingMode = LockingMode.Standard;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is LockingMode lm)
                {
                    lockingMode = lm;
                    continue;
                }
            }

            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 },
                                            lockingMode: lockingMode, revivificationSettings: RevivificationSettings.DefaultFixedLength);
            functions = new RevivificationFixedLenFunctions();
            session = fkv.For(functions).NewSession<RevivificationFixedLenFunctions>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fkv?.Dispose();
            fkv = null;
            log?.Dispose();
            log = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
            {
                var status = session.Upsert(key, key * valueMult);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleFixedLenTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] DeleteDest deleteDest, 
                                       [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] BumpEpochMode bumpMode)
        {
            Populate();

            var deleteKey = 42;
            var tailAddress = fkv.Log.TailAddress;
            session.Delete(deleteKey);
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
            Assert.IsTrue(fkv.FreeRecordPoolHasRecords, "Expected a free record after delete");

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;
            var updateValue = updateKey + valueMult;

            if (bumpMode == BumpEpochMode.Bump)
                fkv.BumpCurrentEpoch();

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(updateKey, updateValue);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(updateKey, updateValue);

            if (bumpMode == BumpEpochMode.Bump)
            {
                Assert.IsFalse(fkv.FreeRecordPoolHasRecords, "Bump: expected no free records after update");
                Assert.AreEqual(tailAddress, fkv.Log.TailAddress, "Bump: expected tail address to be unchanged");
            }
            else
            {
                Assert.IsTrue(fkv.FreeRecordPoolHasRecords, $"NoBump: expected free records after update");
                Assert.AreNotEqual(tailAddress, fkv.Log.TailAddress, "NoBump: expected tail address to grow");
            }
        }
    }

    [TestFixture]
    class RevivificationVarLenTests
    {
        const int KeyLength = 10;
        const int InitialLength = 50;
        const int GrowLength = InitialLength + 75;      // Must be large enough to go to next bin
        const int ShrinkLength = InitialLength - 25;    // Must be small enough to go to previous bin

        const int OversizeLength = RevivificationBin.MaxInlineRecordSize + 42;

        internal class RevivificationSpanByteFunctions : SpanByteFunctions<Empty>
        {
            private readonly FasterKV<SpanByte, SpanByte> fkv;

            // Must be set after session is created
            internal ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions> session;

            internal int expectedConcurrentDestLength = InitialLength;
            internal int expectedSingleDestLength = InitialLength;
            internal int expectedConcurrentFullValueLength = -1;
            internal int expectedSingleFullValueLength = -1;
            internal int expectedInputLength = InitialLength;

            // This is a queue rather than a single value because there may be calls to, for example, ConcurrentWriter with one length
            // followed by SingleWriter with another.
            internal Queue<int> expectedUsedValueLengths = new();

            internal bool readCcCalled, rmwCcCalled;

            internal RevivificationSpanByteFunctions(FasterKV<SpanByte, SpanByte> fkv)
            {
                this.fkv = fkv;
            }

            private void AssertInfoValid(ref UpsertInfo updateInfo)
            {
                Assert.AreEqual(this.session.ctx.version, updateInfo.Version);
            }
            private void AssertInfoValid(ref RMWInfo rmwInfo)
            {
                Assert.AreEqual(this.session.ctx.version, rmwInfo.Version);
            }
            private void AssertInfoValid(ref DeleteInfo deleteInfo)
            {
                Assert.AreEqual(this.session.ctx.version, deleteInfo.Version);
            }

            private void VerifyKeyAndValue(ref SpanByte functionsKey, ref SpanByte functionsValue)
            {
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                Assert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    Span<byte> valueSpan = functionsValue.AsSpan().Slice(valueOffset, compareLength);
                    Span<byte> keySpan = functionsKey.AsSpan()[..compareLength];
                    Assert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {SpanByte.FromFixedSpan(valueSpan)}) does not match functionsKey ({SpanByte.FromFixedSpan(keySpan)})");
                    valueLengthRemaining -= compareLength;
                }
            }

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                var rmwInfo = RevivificationTestUtils.CopyToRMWInfo(ref upsertInfo);
                var result = InitialUpdater(ref key, ref input, ref dst, ref output, ref rmwInfo);
                upsertInfo.UsedValueLength = rmwInfo.UsedValueLength;
                return result;
            }

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                VerifyKeyAndValue(ref key, ref dst);
                var rmwInfo = RevivificationTestUtils.CopyToRMWInfo(ref upsertInfo);
                var result = InPlaceUpdater(ref key, ref input, ref dst, ref output, ref rmwInfo);
                upsertInfo.UsedValueLength = rmwInfo.UsedValueLength;
                return result;
            }

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();

                if (value.Length == 0)
                {
                    Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);   // for the length header
                    Assert.AreEqual(VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment, rmwInfo.FullValueLength); // This should be the "added record for Delete" case, so a "default" value
                }
                else
                {
                    Assert.AreEqual(expectedSingleDestLength, value.Length);
                    Assert.AreEqual(expectedSingleFullValueLength, rmwInfo.FullValueLength);
                    Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);
                    Assert.GreaterOrEqual(rmwInfo.Address, fkv.hlog.ReadOnlyAddress);
                }
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);
                rmwInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();

                if (newValue.Length == 0)
                {
                    Assert.AreEqual(sizeof(int), rmwInfo.UsedValueLength);   // for the length header
                    Assert.AreEqual(VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment, rmwInfo.FullValueLength); // This should be the "added record for Delete" case, so a "default" value
                }
                else
                {
                    Assert.AreEqual(expectedSingleDestLength, newValue.Length);
                    Assert.AreEqual(expectedSingleFullValueLength, rmwInfo.FullValueLength);
                    Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);
                    Assert.GreaterOrEqual(rmwInfo.Address, fkv.hlog.ReadOnlyAddress);
                }
                if (input.Length > newValue.Length)
                    return false;
                input.CopyTo(ref newValue);
                rmwInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, rmwInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);

                Assert.GreaterOrEqual(rmwInfo.Address, fkv.hlog.ReadOnlyAddress);
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);      // Does not change dst.Length, which is fine for everything except shrinking (we've allocated sufficient space in other cases)
                if (input.Length < value.Length)
                    value.Length = input.Length;
                rmwInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                Assert.AreEqual(expectedSingleDestLength, value.Length);
                Assert.AreEqual(expectedSingleFullValueLength, deleteInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, deleteInfo.UsedValueLength);

                Assert.GreaterOrEqual(deleteInfo.Address, fkv.hlog.ReadOnlyAddress);
                value = default;
                return true;
            }

            public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, deleteInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, deleteInfo.UsedValueLength);

                Assert.GreaterOrEqual(deleteInfo.Address, fkv.hlog.ReadOnlyAddress);
                value = default;
                return true;
            }

            public override void PostCopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                base.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }

            public override void PostInitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                base.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            public override void PostSingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason writeReason)
            {
                AssertInfoValid(ref upsertInfo);
                base.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, writeReason);
            }

            public override void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                base.PostSingleDeleter(ref key, ref deleteInfo);
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                this.readCcCalled = true;
                base.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);
            }

            public override void RMWCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                this.rmwCcCalled = true;
                base.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);
            }
        }

        /// <summary>
        /// Callback for length computation based on value and input.
        /// </summary>
        public class RevivificationVarLenStruct : IVariableLengthStruct<SpanByte, SpanByte>
        {
            public int GetInitialLength(ref SpanByte input) => input.TotalSize;

            // This is why we don't use SpanByteVarLenStructForSpanByteInput; we always want the input length, for these tests.
            public int GetLength(ref SpanByte value, ref SpanByte input) => input.TotalSize;
        }

        static int RoundUpSpanByteFullValueLength(SpanByte input) => RoundupTotalSizeFullValue(input.TotalSize);

        static int RoundUpSpanByteFullValueLength(int dataLength) => RoundupTotalSizeFullValue(sizeof(int) + dataLength);

        internal static int RoundupTotalSizeFullValue(int length) => (length + VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment - 1) & (~(VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment - 1));

        static int RoundUpSpanByteUsedLength(int dataLength) => RoundUp(SpanByteTotalSize(dataLength), sizeof(int));

        static int SpanByteTotalSize(int dataLength) => sizeof(int) + dataLength;

        const int numRecords = 200;

        RevivificationSpanByteFunctions functions;
        RevivificationSpanByteComparer comparer;
        RevivificationVarLenStruct valueVLS;

        private FasterKV<SpanByte, SpanByte> fkv;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 17, MemorySizeBits = 22 };
            var lockingMode = LockingMode.Standard;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
                if (arg is LockingMode lm)
                {
                    lockingMode = lm;
                    continue;
                }
                if (arg is PendingOp)
                {
                    logSettings.ReadCopyOptions = new(ReadCopyFrom.Device, ReadCopyTo.MainLog);
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            fkv = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings, comparer: comparer, lockingMode: lockingMode, revivificationSettings: RevivificationSettings.PowerOf2Bins);

            valueVLS = new RevivificationVarLenStruct();
            functions = new RevivificationSpanByteFunctions(fkv);
            session = fkv.For(functions).NewSession<RevivificationSpanByteFunctions>(
                    sessionVariableLengthStructSettings: new SessionVariableLengthStructSettings<SpanByte, SpanByte> { valueLength = valueVLS });
            functions.session = session;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fkv?.Dispose();
            fkv = null;
            log?.Dispose();
            log = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate() => Populate(0, numRecords);

        void Populate(int from, int to)
        {
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);

            SpanByteAndMemory output = new();

            for (int ii = from; ii < to; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);
                functions.expectedUsedValueLengths.Enqueue(input.TotalSize);
                var status = session.Upsert(ref key, ref input, ref input, ref output);
                Assert.IsTrue(status.Record.Created, status.ToString());
                Assert.IsEmpty(functions.expectedUsedValueLengths);
            }
        }

        public enum Growth { None, Grow, Shrink };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenNoRevivLengthTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] Growth growth)
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
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
            functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(functions.expectedInputLength);
            functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            Span<byte> inputVec = stackalloc byte[functions.expectedInputLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            // For Grow, we won't be able to satisfy the request with a revivification, and the new value length will be GrowLength
            functions.expectedUsedValueLengths.Enqueue(sizeof(int) + InitialLength);
            if (growth == Growth.Grow)
                functions.expectedUsedValueLengths.Enqueue(sizeof(int) + GrowLength);

            SpanByteAndMemory output = new();

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.IsEmpty(functions.expectedUsedValueLengths);

            if (growth == Growth.Shrink)
            {
                // What's there now will be what is passed to ConcurrentWriter/IPU (if Shrink, we kept the same value we allocated initially)
                functions.expectedConcurrentFullValueLength = growth == Growth.Shrink ? RoundUpSpanByteFullValueLength(InitialLength) : functions.expectedSingleFullValueLength;

                // Now let's see if we have the correct expected extra length in the destination.
                inputVec = stackalloc byte[InitialLength / 2];  // Grow this from ShrinkLength to InitialLength
                input = SpanByte.FromFixedSpan(inputVec);
                inputVec.Fill(fillByte);

                functions.expectedInputLength = InitialLength / 2;
                functions.expectedConcurrentDestLength = InitialLength / 2;
                functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(functions.expectedInputLength);
                functions.expectedUsedValueLengths.Enqueue(input.TotalSize);

                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                Assert.IsEmpty(functions.expectedUsedValueLengths);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenSimpleTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = fkv.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromFixedSpan(keyVec);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());

            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            RevivificationTestUtils.BumpCurrentEpoch(fkv);

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenReadOnlyMinAddressTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = fkv.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromFixedSpan(keyVec);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());

            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
            fkv.Log.ShiftReadOnlyAddress(fkv.Log.TailAddress, wait: true);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.Greater(fkv.Log.TailAddress, tailAddress);
        }

        public enum UpdateKey { Unfound, DeletedAboveRO, DeletedBelowRO, CopiedBelowRO };

        const byte unfound = numRecords + 2;
        const byte delBelowRO = numRecords / 2 - 4;
        const byte copiedBelowRO = numRecords / 2 - 5;

        private long PrepareDeletes(bool stayInChain, byte delAboveRO, FlushMode flushMode, CollisionRange collisionRange)
        {
            Populate(0, numRecords / 2);

            FreeRecordPool<SpanByte, SpanByte> pool = default;
            if (stayInChain)
                pool = RevivificationTestUtils.SwapFreeRecordPool(fkv, pool);

            // Delete key below (what will be) the readonly line. This is for a target for the test; the record should not be revivified.
            Span<byte> keyVecDelBelowRO = stackalloc byte[KeyLength];
            keyVecDelBelowRO.Fill(delBelowRO);
            var delKeyBelowRO = SpanByte.FromFixedSpan(keyVecDelBelowRO);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref delKeyBelowRO);
            Assert.IsTrue(status.Found, status.ToString());

            if (flushMode == FlushMode.ReadOnly)
                fkv.Log.ShiftReadOnlyAddress(fkv.Log.TailAddress, wait: true);
            else if (flushMode == FlushMode.OnDisk)
                fkv.Log.FlushAndEvict(wait: true);

            Populate(numRecords / 2 + 1, numRecords);

            var tailAddress = fkv.Log.TailAddress;

            // Delete key above the readonly line. This is the record that will be revivified.
            // If not stayInChain, this also puts two elements in the free list; one should be skipped over on dequeue as it is below readonly.
            Span<byte> keyVecDelAboveRO = stackalloc byte[KeyLength];
            keyVecDelAboveRO.Fill(delAboveRO);
            var delKeyAboveRO = SpanByte.FromFixedSpan(keyVecDelAboveRO);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            status = session.Delete(ref delKeyAboveRO);
            Assert.IsTrue(status.Found, status.ToString());

            RevivificationTestUtils.BumpCurrentEpoch(fkv);

            if (stayInChain)
            {
                Assert.AreEqual(0, pool.NumberOfRecords);
                pool = RevivificationTestUtils.SwapFreeRecordPool(fkv, pool);
            }
            else if (collisionRange == CollisionRange.None)     // CollisionRange.Ten has a valid .PreviousAddress so won't be moved to FreeList
                Assert.IsTrue(fkv.FreeRecordPool.HasRecords, "Delete did not move to FreeRecordList as expected");

            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);

            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            return tailAddress;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenUpdateRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] DeleteDest deleteDest, [Values] UpdateKey updateKey,
                                          [Values] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            bool stayInChain = deleteDest == DeleteDest.InChain;

            byte delAboveRO = (byte)(numRecords - (stayInChain
                ? (int)CollisionRange.Ten + 3       // Will remain in chain
                : 2));                              // Will be sent to free list

            long tailAddress = PrepareDeletes(stayInChain, delAboveRO, FlushMode.ReadOnly, collisionRange);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            Span<byte> keyVecToTest = stackalloc byte[KeyLength];
            var keyToTest = SpanByte.FromFixedSpan(keyVecToTest);

            bool expectReviv;
            if (updateKey == UpdateKey.Unfound || updateKey == UpdateKey.CopiedBelowRO)
            {
                // Unfound key should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain.
                // CopiedBelowRO should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain
                //      (but exercises a different code path than Unfound).
                // CollisionRange.Ten has a valid PreviousAddress so it is not elided from the cache.
                byte fillByte = updateKey == UpdateKey.Unfound ? unfound : copiedBelowRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedBelowRO)
            {
                // DeletedBelowRO will not match the key for the in-chain above-RO slot, and we cannot reviv below RO or retrieve below-RO from the
                // freelist, so we will always allocate a new record unless we're using the freelist.
                byte fillByte = delBelowRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedAboveRO)
            {
                // DeletedAboveRO means we will reuse an in-chain record, or will get it from the freelist if deleteDest is FreeList.
                byte fillByte = delAboveRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = true;
            }
            else
            {
                Assert.Fail($"Unexpected updateKey {updateKey}");
                expectReviv = false;    // make the compiler happy
            }

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;

            // A revivified record will have the full initial value length. A new record here will be created with the half-size input
            // (which we do in these tests because we retrieve from the "next higher bin"
            if (!expectReviv)
                functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            RevivificationTestUtils.BumpCurrentEpoch(fkv);

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref keyToTest, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref keyToTest, ref input);

            if (expectReviv)
                Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
            else
                Assert.Greater(fkv.Log.TailAddress, tailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleMidChainRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                               [Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;
            FreeRecordPool<SpanByte, SpanByte> pool = default;
            if (stayInChain)
                pool = RevivificationTestUtils.SwapFreeRecordPool(fkv, pool);

            // This freed record stays in the hash chain.
            byte chainKey = numRecords / 2 - 1;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());

            var tailAddress = fkv.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(chainKey);

            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteEntireChainAndRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                                     [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            // These freed records stay in the hash chain; we even skip the first one to ensure nothing goes into the free list.
            byte chainKey = 5;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);
            var hash = comparer.GetHashCode64(ref key);

            List<byte> deletedSlots = new();
            for (int ii = chainKey + 1; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                if (comparer.GetHashCode64(ref key) != hash)
                    continue;

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
                deletedSlots.Add((byte)ii);
            }

            // For this test we're still limiting to byte repetition
            Assert.Greater(255 - numRecords, deletedSlots.Count);
            Assert.AreEqual(0, fkv.FreeRecordPool.NumberOfRecords);
            Assert.Greater(deletedSlots.Count, 5);    // should be about Ten
            var tailAddress = fkv.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(chainKey);

            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            for (int ii = 0; ii < deletedSlots.Count; ++ii)
            {
                keyVec.Fill(deletedSlots[ii]);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
            }
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.None)] CollisionRange collisionRange,
                                                    [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            long tailAddress = fkv.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // Delete
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
            Assert.AreEqual(numRecords, fkv.FreeRecordPool.NumberOfRecords, $"Expected numRecords ({numRecords}) free records");

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;

            // These come from the existing initial allocation so keep the full length
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            RevivificationTestUtils.WaitForEpochBump(fkv);

            // Revivify
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                Assert.AreEqual(tailAddress, fkv.Log.TailAddress, $"unexpected new record for key {ii}");
            }
            Assert.AreEqual(0, fkv.FreeRecordPool.NumberOfRecords, "Expected no free records");

            // Confirm
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                var status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void BinSelectionTest()
        {
            int expectedBin = 0, recordSize = FreeRecordBin.MinRecordSize;
            for (; recordSize <= RevivificationBin.MaxInlineRecordSize; recordSize *= 2)
            {
                Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize - 1, out int actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);

                Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize + 1, out actualBin));
                Assert.AreEqual(expectedBin + 1, actualBin);
                ++expectedBin;
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialBinWrappingTest()
        {
            Populate();

            const int recordSize = 42;

            Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(2, binIndex);
            var bin = fkv.FreeRecordPool.bins[binIndex];

            var initialPartition = bin.GetInitialPartitionIndex();

            const int minAddress = 1_000;
            int logicalAddress = 1_000_000;

            var partitionStart = bin.GetPartitionStart(initialPartition);
            ref int read = ref FreeRecordBin.GetReadPos(partitionStart);
            ref int write = ref FreeRecordBin.GetWritePos(partitionStart);
            Assert.AreEqual(1, read);
            Assert.AreEqual(1, write);

            // Fill the partition.
            var count = 0;
            for (var ii = 1; ii < bin.partitionSize - 1; ++ii)
            {
                Assert.IsTrue(fkv.FreeRecordPool.Enqueue(logicalAddress + ii, recordSize), "ArtificialBinWrappingTest: Failed to enqueue free record");
                Assert.AreEqual(1, read);
                Assert.AreEqual(1 + ii, write);
                ++count;
            }
            Assert.AreEqual(bin.partitionSize - 1, write);

            // partitionSize - 2 because:
            //   The first element of the partition is head/tail
            //   The tail cannot be incremented to be equal to head (or the list would be considered empty), so we lose one element of capacity
            Assert.AreEqual(bin.partitionSize - 2, count);

            RevivificationTestUtils.WaitForEpochBump(fkv);

            // Dequeue one to open up a space in the partition.
            Assert.IsTrue(bin.Dequeue(recordSize, minAddress, fkv, out _));
            Assert.AreEqual(2, read);
            Assert.AreEqual(bin.partitionSize - 1, write);
            --count;

            // Now wrap with an enqueue
            Assert.IsTrue(bin.Enqueue(logicalAddress + bin.partitionSize, recordSize, fkv));
            Assert.AreEqual(2, read);
            Assert.AreEqual(1, write);
            ++count;

            // Bump epoch directly as we inserted directly to the bin (which bypassed the timer-controlled BumpCurrentEpoch calls in FreeRecordPool).
            RevivificationTestUtils.BumpCurrentEpoch(fkv);

            // Dequeue to the end of the bin.
            for (var ii = 2; ii < bin.partitionSize - 1; ++ii)
            {
                Assert.IsTrue(bin.Dequeue(recordSize, minAddress, fkv, out _));
                Assert.AreEqual(1 + ii, read);
                Assert.AreEqual(1, write);
                --count;
            }
            Assert.AreEqual(bin.partitionSize - 1, read);
            Assert.AreEqual(1, write);
            Assert.AreEqual(1, count);

            // And now wrap the dequeue to empty the bin.
            Assert.IsTrue(bin.Dequeue(recordSize, minAddress, fkv, out _));
            Assert.AreEqual(1, read);
            Assert.AreEqual(1, write);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialPartitionWrappingTest()
        {
            Populate();

            const int recordSize = 42;

            Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(2, binIndex);
            var bin = fkv.FreeRecordPool.bins[binIndex];

            var initialPartition = 1;

            const int minAddress = 1_000;
            int logicalAddress = 1_000_000;
            var count = 0;

            // Initial state: all partitions empty
            for (var iPart = initialPartition; iPart < bin.partitionCount; ++iPart)
            {
                var p = bin.GetPartitionStart(iPart);
                ref int h = ref FreeRecordBin.GetReadPos(p);
                ref int t = ref FreeRecordBin.GetWritePos(p);
                Assert.AreEqual(1, h, $"initialPartition {initialPartition}, current partition {iPart}");
                Assert.AreEqual(1, t, $"initialPartition {initialPartition}, current partition {iPart}");
            }

            // Fill up all partitions to the end of the bin.
            for (var iPart = initialPartition; iPart < bin.partitionCount; ++iPart)
            {
                // Fill the partition; partitionSize - 2 because:
                //   The first element of the partition is head/tail
                //   The tail cannot be incremented to be equal to head (or the list would be considered empty), so we lose one element of capacity
                for (var ii = 0; ii < bin.partitionSize - 2; ++ii)
                {
                    Assert.IsTrue(bin.Enqueue(logicalAddress + ii, recordSize, fkv, initialPartition), $"Failed to enqueue ii {ii}, iPart {iPart}, initialPart {initialPartition}");
                    ++count;
                }

                // Make sure we didn't overflow to the next bin (ensures counts are as expected)
                var nextPart = iPart < bin.partitionCount - 1 ? iPart + 1 : 0;
                var p = bin.GetPartitionStart(nextPart);
                ref int h = ref FreeRecordBin.GetReadPos(p);
                ref int t = ref FreeRecordBin.GetWritePos(p);
                Assert.AreEqual(1, h, $"initialPartition {initialPartition}, current partition {iPart}, nextPart = {nextPart}, count = {count}");
                Assert.AreEqual(1, t, $"initialPartition {initialPartition}, current partition {iPart}, nextPart = {nextPart}, count = {count}");
            }

            // Prepare for wrap: Get partition 0's info
            var partitionStart = bin.GetPartitionStart(0);
            ref int head = ref FreeRecordBin.GetReadPos(partitionStart);
            ref int tail = ref FreeRecordBin.GetWritePos(partitionStart);
            Assert.AreEqual(1, head, $"initialPartition {initialPartition}");
            Assert.AreEqual(1, tail, $"initialPartition {initialPartition}");

            // Add to the bin, which will cause this to wrap.
            Assert.IsTrue(bin.Enqueue(logicalAddress + 1, recordSize, fkv));
            Assert.IsTrue(bin.Enqueue(logicalAddress + 2, recordSize, fkv));
            Assert.AreEqual(1, head, $"initialPartition {initialPartition}");
            Assert.AreEqual(3, tail, $"initialPartition {initialPartition}");
            count += 2;

            RevivificationTestUtils.BumpCurrentEpoch(fkv);

            // Now dequeue everything
            for (; count > 0; --count)
                Assert.IsTrue(bin.Dequeue(recordSize, minAddress, fkv, out _));

            Assert.AreEqual(3, head, $"initialPartition {initialPartition}");
            Assert.AreEqual(3, tail, $"initialPartition {initialPartition}");
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void LiveBinWrappingTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            // Note: this test assumes no collisions (every delete goes to the FreeList)

            long tailAddress = fkv.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            var recordSize = RecordInfo.GetLength() + RoundUp(keyVec.Length, 8) + RoundUp(InitialLength, 8);
            Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(3, binIndex);
            var bin = fkv.FreeRecordPool.bins[binIndex];

            var initialPartition = bin.GetInitialPartitionIndex();
            var partitionStart = bin.GetPartitionStart(initialPartition);
            ref int read = ref FreeRecordBin.GetReadPos(partitionStart);
            ref int write = ref FreeRecordBin.GetWritePos(partitionStart);

            Span<byte> revivInputVec = stackalloc byte[InitialLength];
            var revivInput = SpanByte.FromFixedSpan(revivInputVec);

            SpanByteAndMemory output = new();

            // Pick some number that won't align with the bin size, so we wrap
            var numKeys = bin.partitionSize - 3;

            for (var iter = 0; iter < 100; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < numKeys; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);
                    revivInputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(iter == 0 ? InitialLength : InitialLength));
                    var status = session.Delete(ref key);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(ii + 1, fkv.FreeRecordPool.NumberOfRecords);
                    Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
                }

                RevivificationTestUtils.BumpCurrentEpoch(fkv);

                // Revivify
                functions.expectedInputLength = InitialLength;
                functions.expectedSingleDestLength = InitialLength;
                functions.expectedConcurrentDestLength = InitialLength;
                for (var ii = 0; ii < numKeys; ++ii)
                {
                    keyVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                    if (updateOp == UpdateOp.Upsert)
                        session.Upsert(ref key, ref revivInput, ref input, ref output);
                    else if (updateOp == UpdateOp.RMW)
                        session.RMW(ref key, ref revivInput);

                    Assert.AreEqual(tailAddress, fkv.Log.TailAddress, $"unexpected new record for key {ii} iter {iter}");
                    Assert.AreEqual(numKeys - ii - 1, fkv.FreeRecordPool.NumberOfRecords);
                }

                Assert.AreEqual(0, fkv.FreeRecordPool.NumberOfRecords);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleOversizeRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] DeleteDest deleteDest,
                                               [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;

            // Both in and out of chain revivification of oversize should have the same lengths.
            FreeRecordPool<SpanByte, SpanByte> pool = default;
            if (stayInChain)
                pool = RevivificationTestUtils.SwapFreeRecordPool(fkv, pool);

            byte chainKey = numRecords + 1;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[OversizeLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            keyVec.Fill(chainKey);
            inputVec.Fill(chainKey);

            // Oversize records in this test do not go to "next higher" bin (there is no next-higher bin in the default PowersOf2 bins we use)
            functions.expectedInputLength = OversizeLength;
            functions.expectedSingleDestLength = OversizeLength;
            functions.expectedConcurrentDestLength = OversizeLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(OversizeLength);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(OversizeLength));

            // Initial insert of the oversize record
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            // Delete it
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(OversizeLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());
            if (!stayInChain)
            { 
                Assert.IsTrue(fkv.FreeRecordPoolHasRecords, "Expected a record in the FreeRecordPool for freelist mode");
                RevivificationTestUtils.WaitForEpochBump(fkv);
            }

            var tailAddress = fkv.Log.TailAddress;

            // Revivify in the chain. Because this is oversize, the expectedFullValueLength remains the same
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(OversizeLength));
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
        }

        public enum PendingOp { Read, RMW };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimplePendingOpsRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.None)] CollisionRange collisionRange,
                                                 [Values] PendingOp pendingOp)
        {
            byte delAboveRO = numRecords - 2;   // Will be sent to free list
            byte targetRO = numRecords / 2 - 15;

            long tailAddress = PrepareDeletes(stayInChain: false, delAboveRO, FlushMode.OnDisk, collisionRange);

            // We always want freelist for this test.
            Assert.IsTrue(fkv.FreeRecordPool.HasRecords);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            // Use a different key below RO than we deleted; this will go pending to retrieve it
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            session.ctx.phase = phase;
            if (pendingOp == PendingOp.Read)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                keyVec.Fill(targetRO);
                inputVec.Fill(targetRO);

                functions.expectedInputLength = InitialLength;
                functions.expectedSingleDestLength = InitialLength;
                functions.expectedConcurrentDestLength = InitialLength;

                var spanSlice = inputVec[..InitialLength];
                var inputSlice = SpanByte.FromFixedSpan(spanSlice);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Read(ref key, ref inputSlice, ref output);
                Assert.IsTrue(status.IsPending, status.ToString());
                session.CompletePending(wait: true);
                Assert.IsTrue(functions.readCcCalled);
            }
            else if (pendingOp == PendingOp.RMW)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                keyVec.Fill(targetRO);
                inputVec.Fill(targetRO);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

                session.RMW(ref key, ref input);
                session.CompletePending(wait: true);
                Assert.IsTrue(functions.rmwCcCalled);
            }
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
        }
    }

    [TestFixture]
    class RevivificationObjectTests
    {
        const int numRecords = 1000;
        internal const int valueMult = 1_000_000;

        private MyFunctions functions;
        private FasterKV<MyKey, MyValue> fkv;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions> session;
        private IDevice log;
        private IDevice objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: true);

            var lockingMode = LockingMode.Standard;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is LockingMode lm)
                {
                    lockingMode = lm;
                    continue;
                }
            }

            fkv = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 22, PageSizeBits = 12 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() },
                lockingMode: lockingMode, revivificationSettings: RevivificationSettings.DefaultFixedLength);

            functions = new MyFunctions();
            session = fkv.For(functions).NewSession<MyFunctions>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fkv?.Dispose();
            fkv = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
            {
                var keyObj = new MyKey { key = key };
                var valueObj = new MyValue { value = key + valueMult };
                var status = session.Upsert(keyObj, valueObj);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleObjectTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] DeleteDest deleteDest, 
                                     [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] BumpEpochMode bumpMode)
        {
            Populate();

            var deleteKey = 42;
            var tailAddress = fkv.Log.TailAddress;
            session.Delete(new MyKey { key = deleteKey });
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
            Assert.IsTrue(fkv.FreeRecordPoolHasRecords, "Expected a free record after delete");

            if (bumpMode == BumpEpochMode.Bump)
                fkv.BumpCurrentEpoch();

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;

            var key = new MyKey { key = updateKey };
            var value = new MyValue { value = key.key + valueMult };
            var input = new MyInput { value = value.value };

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(key, value);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(key, input);

            if (bumpMode == BumpEpochMode.Bump)
            {
                Assert.IsFalse(fkv.FreeRecordPoolHasRecords, "Bump: expected no free records after update");
                Assert.AreEqual(tailAddress, fkv.Log.TailAddress, "Bump: expected tail address to be unchanged");
            }
            else
            {
                Assert.IsTrue(fkv.FreeRecordPoolHasRecords, $"NoBump: expected free records after update");
                Assert.AreNotEqual(tailAddress, fkv.Log.TailAddress, "NoBump: expected tail address to grow");
            }
        }
    }

    [TestFixture]
    class RevivificationVarLenStressTests
    {
        const int KeyLength = 10;
        const int InitialLength = 50;

        internal class RevivificationStressFunctions : SpanByteFunctions<Empty>
        {
            readonly IFasterEqualityComparer<SpanByte> keyComparer;     // non-null if we are doing key comparisons (and thus expectedKey is non-default)
            internal SpanByte expectedKey = default;                    // Set for each operation by the calling thread

            internal RevivificationStressFunctions(IFasterEqualityComparer<SpanByte> keyComparer) => this.keyComparer = keyComparer;

            private void VerifyKey(ref SpanByte functionsKey)
            {
                if (keyComparer is not null)
                    Assert.IsTrue(this.keyComparer.Equals(ref this.expectedKey, ref functionsKey));
            }

            private void VerifyKeyAndValue(ref SpanByte functionsKey, ref SpanByte functionsValue)
            {
                if (keyComparer is not null)
                { 
                    Assert.IsTrue(this.keyComparer.Equals(ref this.expectedKey, ref functionsKey), "functionsKey does not equal expectedKey");
                    int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                    Assert.Less(functionsKey.Length, valueLengthRemaining);
                    while (valueLengthRemaining > 0)
                    {
                        var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                        Span<byte> valueSpan = functionsValue.AsSpan().Slice(valueOffset, compareLength);
                        Span<byte> keySpan = functionsKey.AsSpan()[..compareLength];
                        Assert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {SpanByte.FromFixedSpan(valueSpan)}) does not match functionsKey ({SpanByte.FromFixedSpan(keySpan)})");
                        valueOffset += compareLength;
                        valueLengthRemaining -= compareLength;
                    }
                }
            }

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                VerifyKey(ref key);
                var rmwInfo = RevivificationTestUtils.CopyToRMWInfo(ref upsertInfo);

                // Pass src, not input (which may be empty)
                var result = InitialUpdater(ref key, ref src, ref dst, ref output, ref rmwInfo);
                upsertInfo.UsedValueLength = rmwInfo.UsedValueLength;
                return result;
            }

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                VerifyKeyAndValue(ref key, ref dst);
                var rmwInfo = RevivificationTestUtils.CopyToRMWInfo(ref upsertInfo);

                // Pass src, not input (which may be empty)
                var result = InPlaceUpdater(ref key, ref src, ref dst, ref output, ref rmwInfo);
                upsertInfo.UsedValueLength = rmwInfo.UsedValueLength;
                return result;
            }

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKey(ref key);
                if (input.Length > newValue.Length)
                    return false;
                input.CopyTo(ref newValue);
                newValue.Length = input.Length;
                rmwInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKeyAndValue(ref key, ref oldValue);
                if (input.Length > newValue.Length)
                    return false;
                input.CopyTo(ref newValue);
                newValue.Length = input.Length;
                rmwInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKey(ref key);
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);        // Does not change dst.Length, which is fine for everything except shrinking (we've allocated sufficient space in other cases)
                value.Length = input.Length;    // We must ensure that the value length on the log is the same as the UsedValueLength we return
                rmwInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
                => ConcurrentDeleter(ref key, ref value, ref deleteInfo);

            public unsafe override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
            {
                value = default;
                deleteInfo.UsedValueLength = value.TotalSize;
                return true;
            }
        }

        const int numRecords = 200;
        const int DefaultMaxRecsPerBin = 1024;

        RevivificationStressFunctions functions;
        RevivificationSpanByteComparer comparer;

        private FasterKV<SpanByte, SpanByte> fkv;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 17, MemorySizeBits = 22 };
            var lockingMode = LockingMode.Standard;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
                if (arg is LockingMode lm)
                {
                    lockingMode = lm;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            fkv = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings, comparer: comparer, lockingMode: lockingMode, revivificationSettings: RevivificationSettings.PowerOf2Bins);

            var valueVLS = new RevivificationVLS();
            functions = new RevivificationStressFunctions(keyComparer: null);
            session = fkv.For(functions).NewSession<RevivificationStressFunctions>(
                    sessionVariableLengthStructSettings: new SessionVariableLengthStructSettings<SpanByte, SpanByte> { valueLength = valueVLS });
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fkv?.Dispose();
            fkv = null;
            log?.Dispose();
            log = null;

            DeleteDirectory(MethodTestDir);
        }

        unsafe void Populate()
        {
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            for (int ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                var status = session.Upsert(ref key, ref input, ref input, ref output);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        private unsafe List<int> EnumerateSetRecords(FreeRecordBin bin)
        {
            List<int> result = new();
            for (var iPart = 0; iPart < bin.partitionCount; ++iPart)
            {
                FreeRecord* partitionStart = bin.GetPartitionStart(iPart);

                // Skip the first element (containing read/write pointers)
                for (var iRec = 1; iRec < bin.partitionSize; ++iRec)
                {
                    if (partitionStart[iRec].IsSet)
                        result.Add(iPart * bin.partitionSize + iRec);
                }
            }
            return result;
        }

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void ArtificialFreeBinThreadStressTest()
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");
            const int numIterations = 100;
            const int numItems = 10000;
            var flags = new long[numItems];
            const int size = 42;    // size doesn't matter in this test
            const int numEnqueueThreads = 1;
            const int numDequeueThreads = 1;
            const int numRetries = 10;

            // TODO < numItems; set flag according to Enqueue return
            // For this test we are bypassing the FreeRecordPool in fkv.
            var binDef = new RevivificationBin()
            {
                RecordSize = size,
                NumberOfPartitions = 4,
                NumberOfRecordsPerPartition = numItems / 2
            };
            using var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(fkv, binDef);

            bool done = false;

            unsafe void runEnqueueThread(int tid)
            {
                try
                {
                    for (var iteration = 0; iteration < numIterations; ++iteration)
                    {
                        // Start at 1 so 0 is clearly invalid.
                        for (var ii = 1; ii < numItems; ++ii)
                        {
                            flags[ii] = 1;
                            Assert.IsTrue(freeRecordPool.Enqueue(ii, size));
                        }

                        RevivificationTestUtils.WaitForEpochBump(fkv);

                        // Continue until all are dequeued or we hit the retry limit.
                        List<int> strayFlags = new();
                        for (var retries = 0; retries < numRetries; ++retries)
                        {
                            // Yield to let the dequeue threads catch up.
                            Thread.Sleep(50);
                            strayFlags.Clear();
                            for (var ii = 1; ii < numItems; ++ii)
                            {
                                if (flags[ii] != 0)
                                    strayFlags.Add(ii);
                            }
                            if (strayFlags.Count == 0)
                                return;
                        }

                        Assert.AreEqual(0, strayFlags.Count);
                        var strayRecords = EnumerateSetRecords(freeRecordPool.bins[0]);
                        Assert.AreEqual(0, strayRecords.Count);
                    }
                }
                finally
                {
                    done = true;
                }
            }

            unsafe void runDequeueThread(int tid)
            {
                while (!done)
                {
                    if (freeRecordPool.bins[0].Dequeue(size, 0, fkv, out long address))
                    {
                        var prevFlag = Interlocked.CompareExchange(ref flags[address], 0, 1);
                        Assert.AreEqual(1, prevFlag);
                    }
                }
            }

            // Task rather than Thread for propagation of exception.
            List<Task> tasks = new();
            for (int t = 0; t < numEnqueueThreads; t++)
            { 
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runEnqueueThread(tid)));
            }
            for (int t = 0; t < numDequeueThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDequeueThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }

        public enum ThreadingPattern { SameKeys, RandomKeys };

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void LiveFreeListThreadStressTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] CollisionRange collisionRange,
                                             [Values] ThreadingPattern threadingPattern, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            int numIterations = 100;
            const int numDeleteThreads = 5, numUpdateThreads = 5;

            unsafe void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = fkv.For(new RevivificationStressFunctions(keyComparer: null)).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numDeleteThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(numRecords) : ii;
                        keyVec.Fill((byte)kk);
                        localSession.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                using var localSession = fkv.For(new RevivificationStressFunctions(keyComparer: fkv.comparer)).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numUpdateThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(numRecords) : ii;
                        keyVec.Fill((byte)kk);
                        inputVec.Fill((byte)kk);

                        localSession.functions.expectedKey = key;
                        if (updateOp == UpdateOp.Upsert)
                            localSession.Upsert(key, input);
                        else
                            localSession.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numDeleteThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numUpdateThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void LiveInChainThreadStressTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                                [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            // Turn off freelist.
            RevivificationTestUtils.SwapFreeRecordPool(fkv, null);

            const int numIterations = 500;
            const int numDeleteThreads = 5, numUpdateThreads = 5;

            unsafe void runDeleteThread(int tid)
            {
                using var localSession = fkv.For(new RevivificationStressFunctions(keyComparer: null)).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numDeleteThreads)
                    {
                        keyVec.Fill((byte)ii);
                        localSession.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                using var localSession = fkv.For(new RevivificationStressFunctions(keyComparer: null)).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numUpdateThreads)
                    {
                        keyVec.Fill((byte)ii);
                        inputVec.Fill((byte)ii);

                        if (updateOp == UpdateOp.Upsert)
                            localSession.Upsert(key, input);
                        else
                            localSession.RMW(key, input);
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numDeleteThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numUpdateThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}
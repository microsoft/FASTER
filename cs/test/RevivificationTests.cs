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
    public enum DeleteDest { FreeList, InChain }

    public enum CollisionRange { Ten = 10, None = int.MaxValue }

    public enum RevivificationEnabled { Reviv, NoReviv }

    static class RevivificationTestUtils
    {
        internal static RMWInfo CopyToRMWInfo(ref UpsertInfo upsertInfo)
            => new()
            {
                Version = upsertInfo.Version,
                SessionID = upsertInfo.SessionID,
                Address = upsertInfo.Address,
                KeyHash = upsertInfo.KeyHash,
                recordInfoPtr = upsertInfo.recordInfoPtr,
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

        internal const int DefaultSafeWaitTimeout = BumpEpochWorker.DefaultBumpIntervalMs * 2;

        internal static FreeRecordPool<TKey, TValue> CreateSingleBinFreeRecordPool<TKey, TValue>(FasterKV<TKey, TValue> fkv, RevivificationBin binDef, int fixedRecordLength = 0)
            => new (fkv, new RevivificationSettings() { FreeListBins = new[] { binDef } }, fixedRecordLength);

        internal static void WaitForSafeRecords<TKey, TValue>(FasterKV<TKey, TValue> fkv, bool want, FreeRecordPool<TKey, TValue> pool = null)
        {
            pool ??= fkv.FreeRecordPool;

            // Wait until the worker calls BumpCurrentEpoch and finds safe records. If this is called in a loop it may return true before all epochs
            // in that loop become safe; if this is not specifically what the test wants, then use WaitForSafeEpoch() on the last epoch of the loop.
            // WaitForSafeEpoch is generally useful mostly for single-delete tests, as a verification that the HasSafeRecords setting is working properly.
            var sw = new Stopwatch();
            sw.Start();
            while (pool.HasSafeRecords != want)
            {
                Assert.Less(sw.ElapsedMilliseconds, DefaultSafeWaitTimeout, $"Timeout while waiting for HasSafeRecords to be {want}");
                Thread.Yield();
            }
        }

        internal static void WaitForSafeEpoch<TKey, TValue>(FasterKV<TKey, TValue> fkv, long targetEpoch, FreeRecordPool<TKey, TValue> pool = null)
        {
            // Note: For tests that Delete() or TryAdd() in a loop, use WaitForSafeLatestAddedEpoch due to potential race conditions making latestAddedEpoch obsolete.
            pool ??= fkv.FreeRecordPool;

            // Wait until the worker calls BumpCurrentEpoch and the specified epoch has become safe. Because there is a bit of a lag between 
            // BumpCurrentEpoch/ComputeNewSafeToReclaimEpoch and the scan encountering a record and setting HasSafeRecords, make sure we satisfy both.
            var sw = new Stopwatch();
            sw.Start();
            while (fkv.epoch.SafeToReclaimEpoch < targetEpoch || !pool.HasSafeRecords)
            {
                Assert.Less(sw.ElapsedMilliseconds, DefaultSafeWaitTimeout,
                            $"Timeout while waiting for SafeEpoch and HasSafeRecords: target epoch {targetEpoch}, currentEpoch {fkv.epoch.CurrentEpoch}, safeEpoch {fkv.epoch.SafeToReclaimEpoch}, pool.HasSafeRecords {pool.HasSafeRecords}");
                Thread.Yield();
            }
        }

        internal static unsafe int GetFreeRecordCount<TKey, TValue>(FreeRecordPool<TKey, TValue> pool)
        {
            // This returns the count of all records, not just the free ones.
            int count = 0;
            foreach (var bin in pool.bins)
            {
                for (var ii = 0; ii < bin.recordCount; ++ii)
                {
                    if ((bin.records + ii)->IsSet)
                        ++count;
                }
            }
            return count;
        }

        internal static unsafe long GetLatestAddedEpoch<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int recordSize)
        {
            Assert.IsTrue(pool.GetBinIndex(recordSize, out var binIndex));
            var bin = pool.bins[binIndex];
            long epoch = 0;
            for (var ii = 0; ii < bin.recordCount; ++ii)
            {
                if ((bin.records + ii)->addedEpoch > epoch)
                    epoch = (bin.records + ii)->addedEpoch;
            }
            Assert.Greater(epoch, 0);
            return epoch;
        }

        internal static void WaitForSafeLatestAddedEpoch<TKey, TValue>(FasterKV<TKey, TValue> fkv, int recordSize, FreeRecordPool<TKey, TValue> pool = null)
        {
            // This is necessary because the epoch may have been bumped after we stored off latestAddedEpoch.
            pool ??= fkv.FreeRecordPool;
            long epoch = GetLatestAddedEpoch(pool, recordSize);
            WaitForSafeEpoch(fkv, epoch, pool);
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
        public void SimpleFixedLenTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var deleteKey = 42;
            var tailAddress = fkv.Log.TailAddress;
            session.Delete(deleteKey);
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;
            var updateValue = updateKey + valueMult;

            RevivificationTestUtils.WaitForSafeRecords(fkv, want: true);

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(updateKey, updateValue);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(updateKey, updateValue);

            RevivificationTestUtils.WaitForSafeRecords(fkv, want: false);
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress, "Expected tail address not to grow (record was revivified)");
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

            private static void VerifyKeyAndValue(ref SpanByte functionsKey, ref SpanByte functionsValue)
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
                return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
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
                return base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, rmwInfo.FullValueLength);

                VerifyKeyAndValue(ref key, ref value);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);

                Assert.GreaterOrEqual(rmwInfo.Address, fkv.hlog.ReadOnlyAddress);

               return base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                Assert.AreEqual(expectedSingleDestLength, value.Length);
                Assert.AreEqual(expectedSingleFullValueLength, deleteInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, deleteInfo.UsedValueLength);

                Assert.GreaterOrEqual(deleteInfo.Address, fkv.hlog.ReadOnlyAddress);

                return base.SingleDeleter(ref key, ref value, ref deleteInfo);
            }

            public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, deleteInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, deleteInfo.UsedValueLength);

                Assert.GreaterOrEqual(deleteInfo.Address, fkv.hlog.ReadOnlyAddress);

                return base.ConcurrentDeleter(ref key, ref value, ref deleteInfo);
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
            var revivificationSettings = RevivificationSettings.PowerOf2Bins;
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
                if (arg is RevivificationEnabled revivEnabled)
                {
                    if (revivEnabled == RevivificationEnabled.NoReviv)
                        revivificationSettings = default;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            fkv = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings, comparer: comparer, lockingMode: lockingMode, revivificationSettings: revivificationSettings);

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
        public void VarLenNoRevivLengthTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] Growth growth)
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
        public void VarLenSimpleTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
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

            RevivificationTestUtils.WaitForSafeRecords(fkv, want: true);

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void VarLenReadOnlyMinAddressTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
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
            // If not stayInChain, this also puts two elements in the free list; one should be skipped over on Take() as it is below readonly.
            Span<byte> keyVecDelAboveRO = stackalloc byte[KeyLength];
            keyVecDelAboveRO.Fill(delAboveRO);
            var delKeyAboveRO = SpanByte.FromFixedSpan(keyVecDelAboveRO);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var lastAddedEpoch = fkv.epoch.CurrentEpoch;
            status = session.Delete(ref delKeyAboveRO);
            Assert.IsTrue(status.Found, status.ToString());

            if (stayInChain)
            {
                Assert.IsFalse(pool.HasSafeRecords, "Expected empty pool");
                pool = RevivificationTestUtils.SwapFreeRecordPool(fkv, pool);
            }
            else if (collisionRange == CollisionRange.None)     // CollisionRange.Ten has a valid .PreviousAddress so won't be moved to FreeList
            {
                RevivificationTestUtils.WaitForSafeEpoch(fkv, lastAddedEpoch);
            }

            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);

            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            return tailAddress;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(300)]
        public void VarLenUpdateRevivifyTest([Values] DeleteDest deleteDest, [Values] UpdateKey updateKey,
                                          [Values] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

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

            if (!expectReviv)
                functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

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
        public void SimpleMidChainRevivifyTest([Values(CollisionRange.Ten)] CollisionRange collisionRange,
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
        public void DeleteEntireChainAndRevivifyTest([Values(CollisionRange.Ten)] CollisionRange collisionRange,
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
            Assert.IsFalse(fkv.FreeRecordPool.HasSafeRecords, "Expected empty pool");
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
        public void DeleteAllRecordsAndRevivifyTest([Values(CollisionRange.None)] CollisionRange collisionRange,
                                                    [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            long tailAddress = fkv.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // "sizeof(int) +" because SpanByte has an int length prefix
            var recordSize = RecordInfo.GetLength() + RoundUp(sizeof(int) + keyVec.Length, 8) + RoundUp(sizeof(int) + InitialLength, 8);

            // Delete
            long latestAddedEpoch = fkv.epoch.CurrentEpoch;
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                latestAddedEpoch = fkv.epoch.CurrentEpoch;
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);
            Assert.AreEqual(numRecords, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), $"Expected numRecords ({numRecords}) free records");

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;

            // These come from the existing initial allocation so keep the full length
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            RevivificationTestUtils.WaitForSafeLatestAddedEpoch(fkv, recordSize);

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

            Assert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), "expected no free records remaining");
            RevivificationTestUtils.WaitForSafeRecords(fkv, want: false);

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
        public void DeleteAllRecordsAndTakeSnapshotTest()
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // Delete
            long latestAddedEpoch = fkv.epoch.CurrentEpoch;
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                latestAddedEpoch = fkv.epoch.CurrentEpoch;
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(numRecords, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), $"Expected numRecords ({numRecords}) free records");

            this.fkv.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndIterateTest()
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // Delete
            long latestAddedEpoch = fkv.epoch.CurrentEpoch;
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                latestAddedEpoch = fkv.epoch.CurrentEpoch;
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(numRecords, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), $"Expected numRecords ({numRecords}) free records");

            using var iterator = this.session.Iterate();
            while (iterator.GetNext(out _))
                ;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void BinSelectionTest()
        {
            int expectedBin = 0, recordSize = fkv.FreeRecordPool.bins[expectedBin].maxRecordSize;
            while (true)
            {
                Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize - 1, out int actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);

                if (++expectedBin == fkv.FreeRecordPool.bins.Length)
                {
                    Assert.IsFalse(fkv.FreeRecordPool.GetBinIndex(recordSize + 1, out actualBin));
                    Assert.AreEqual(-1, actualBin);
                    break;
                }
                Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize + 1, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                recordSize = fkv.FreeRecordPool.bins[expectedBin].maxRecordSize;
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(30)]
        public unsafe void ArtificialBinWrappingTest()
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            Populate();

            const int recordSize = 42;

            Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(2, binIndex);
            var bin = fkv.FreeRecordPool.bins[binIndex];

            const int minAddress = 1_000;
            int logicalAddress = 1_000_000;

            // Fill the bin, including wrapping around at the end.
            for (var ii = 0; ii < bin.recordCount; ++ii)
                Assert.IsTrue(fkv.FreeRecordPool.TryAdd(logicalAddress + ii, recordSize), "ArtificialBinWrappingTest: Failed to Add free record, pt 1");

            // Try to add to a full bin; this should fail.
            Assert.IsFalse(fkv.FreeRecordPool.TryAdd(logicalAddress + bin.recordCount, recordSize), "ArtificialBinWrappingTest: Expected to fail Adding free record");

            RevivificationTestUtils.WaitForSafeRecords(fkv, want: true);

            for (var ii = 0; ii < bin.recordCount; ++ii)
                Assert.IsTrue((bin.records + ii)->IsSet, "expected bin to be set at ii == {ii}");

            // Take() one to open up a space in the bin, then add one
            Assert.IsTrue(bin.TryTake(recordSize, minAddress, fkv, out _));
            var lastAddedEpoch = fkv.epoch.CurrentEpoch;
            Assert.IsTrue(fkv.FreeRecordPool.TryAdd(logicalAddress + bin.recordCount + 1, recordSize), "ArtificialBinWrappingTest: Failed to Add free record, pt 2");

            RevivificationTestUtils.WaitForSafeEpoch(fkv, lastAddedEpoch);

            // Take() all records in the bin.
            for (var ii = 0; ii < bin.recordCount; ++ii)
                Assert.IsTrue(bin.TryTake(recordSize, minAddress, fkv, out _), $"ArtificialBinWrappingTest: failed to Take at ii == {ii}");
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void LiveBinWrappingTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] WaitMode waitMode, [Values] DeleteDest deleteDest)
        {
            Populate();

            // Note: this test assumes no collisions (every delete goes to the FreeList)

            long tailAddress = fkv.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            // "sizeof(int) +" because SpanByte has an int length prefix.
            var recordSize = RecordInfo.GetLength() + RoundUp(sizeof(int) + keyVec.Length, 8) + RoundUp(sizeof(int) + InitialLength, 8);
            Assert.IsTrue(fkv.FreeRecordPool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(3, binIndex);
            var bin = fkv.FreeRecordPool.bins[binIndex];

            // Pick some number that 
            Assert.AreNotEqual(0, bin.GetSegmentStart(recordSize), "SegmentStart should not be 0, to test wrapping");

            int maxIter = waitMode == WaitMode.Wait ? 5 : 100;
            for (var iter = 0; iter < maxIter; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < numRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(iter == 0 ? InitialLength : InitialLength));
                    var status = session.Delete(ref key);
                    Assert.IsTrue(status.Found, $"{status} for key {ii}");
                    //Assert.AreEqual(ii + 1, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), $"mismatched free record count for key {ii}, pt 1");
                }

                if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
                { 
                    Assert.AreEqual(numRecords, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), "mismatched free record count");
                    RevivificationTestUtils.WaitForSafeLatestAddedEpoch(fkv, recordSize);
                }

                // Revivify
                functions.expectedInputLength = InitialLength;
                functions.expectedSingleDestLength = InitialLength;
                functions.expectedConcurrentDestLength = InitialLength;
                for (var ii = 0; ii < numRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

                    SpanByteAndMemory output = new();
                    if (updateOp == UpdateOp.Upsert)
                        session.Upsert(ref key, ref input, ref input, ref output);
                    else if (updateOp == UpdateOp.RMW)
                        session.RMW(ref key, ref input);
                    output.Memory?.Dispose();

                    if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait && tailAddress != fkv.Log.TailAddress)
                    {
                        var freeRecs = RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool);
                        Debugger.Launch();
                        Assert.AreEqual(tailAddress, fkv.Log.TailAddress, $"failed to revivify record for key {ii} iter {iter}, freeRecs {freeRecs}");
                    }
                }

                if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
                { 
                    Assert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(fkv.FreeRecordPool), "expected no free records remaining");
                    RevivificationTestUtils.WaitForSafeRecords(fkv, want: false);
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void LiveBinWrappingNoRevivTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values(RevivificationEnabled.NoReviv)] RevivificationEnabled revivEnabled)
        {
            // For a comparison to the reviv version above.
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            for (var iter = 0; iter < 100; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < numRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(iter == 0 ? InitialLength : InitialLength));
                    var status = session.Delete(ref key);
                    Assert.IsTrue(status.Found, $"{status} for key {ii}, iter {iter}");
                }

                for (var ii = 0; ii < numRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

                    SpanByteAndMemory output = new();
                    if (updateOp == UpdateOp.Upsert)
                        session.Upsert(ref key, ref input, ref input, ref output);
                    else if (updateOp == UpdateOp.RMW)
                        session.RMW(ref key, ref input);
                    output.Memory?.Dispose();
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleOversizeRevivifyTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
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
                RevivificationTestUtils.WaitForSafeRecords(fkv, want: true);

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
        public void SimplePendingOpsRevivifyTest([Values(CollisionRange.None)] CollisionRange collisionRange, [Values] PendingOp pendingOp)
        {
            byte delAboveRO = numRecords - 2;   // Will be sent to free list
            byte targetRO = numRecords / 2 - 15;

            long tailAddress = PrepareDeletes(stayInChain: false, delAboveRO, FlushMode.OnDisk, collisionRange);

            // We always want freelist for this test.
            Assert.IsTrue(fkv.FreeRecordPool.HasSafeRecords);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            // Use a different key below RO than we deleted; this will go pending to retrieve it
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

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
        public void SimpleObjectTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var deleteKey = 42;
            var tailAddress = fkv.Log.TailAddress;
            session.Delete(new MyKey { key = deleteKey });
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;

            var key = new MyKey { key = updateKey };
            var value = new MyValue { value = key.key + valueMult };
            var input = new MyInput { value = value.value };

            RevivificationTestUtils.WaitForSafeRecords(fkv, want: true);
            Assert.IsTrue(fkv.FreeRecordPoolHasSafeRecords, "Expected a free record after delete and WaitForSafeEpoch");

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(key, value);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(key, input);

            RevivificationTestUtils.WaitForSafeRecords(fkv, want: false);
            Assert.AreEqual(tailAddress, fkv.Log.TailAddress, "Expected tail address not to grow (record was revivified)");
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
                return base.InitialUpdater(ref key, ref input, ref newValue, ref output, ref rmwInfo);
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKeyAndValue(ref key, ref oldValue);
                return base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKey(ref key);
                return base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
                => base.SingleDeleter(ref key, ref value, ref deleteInfo);

            public unsafe override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo)
                => base.ConcurrentDeleter(ref key, ref value, ref deleteInfo);
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
            for (var ii = 0; ii < bin.recordCount; ++ii)
            {
                if ((bin.records + ii)->IsSet)
                    result.Add(ii);
            }
            return result;
        }

        const int AddressIncrement = 1_000_000; // must be > ReadOnlyAddress

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void ArtificialFreeBinThreadStressTest()
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");
            const int numIterations = 10;
            const int numItems = 1000;
            var flags = new long[numItems];
            const int size = 48;    // size doesn't matter in this test, but must be a multiple of 8
            const int numAddThreads = 1;
            const int numTakeThreads = 1;

            // TODO < numItems; set flag according to Add() return
            // For this test we are bypassing the FreeRecordPool in fkv.
            var binDef = new RevivificationBin()
            {
                RecordSize = size,
                NumberOfRecords = numItems * 4
            };
            using var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(fkv, binDef);

            bool done = false;

            unsafe void runAddThread(int tid)
            {
                try
                {
                    for (var iteration = 0; iteration < numIterations; ++iteration)
                    {
                        // Start the loop at 1 so 0 is clearly invalid.
                        for (var ii = 1; ii < numItems; ++ii)
                        {
                            flags[ii] = 1;
                            Assert.IsTrue(freeRecordPool.TryAdd(ii + AddressIncrement, size), $"Failed to add free record {ii}, iteration {iteration}");
                        }

                        // Do not wait here; the loop will do that with retries

                        // Continue until all are Taken or we hit the timeout, sleeping to let the Take() threads catch up
                        var startMs = Native32.GetTickCount64();
                        List<int> strayFlags = new();
                        while (Native32.GetTickCount64() - startMs < RevivificationTestUtils.DefaultSafeWaitTimeout)
                        {
                            Thread.Sleep(20);
                            strayFlags.Clear();
                            for (var ii = 1; ii < numItems; ++ii)
                            {
                                if (flags[ii] != 0)
                                    strayFlags.Add(ii);
                            }
                            if (strayFlags.Count == 0)
                                break;
                        }

                        var stopMs = Native32.GetTickCount64();
                        var strayRecords = EnumerateSetRecords(freeRecordPool.bins[0]);
                        Assert.IsTrue(strayFlags.Count + strayRecords.Count == 0, $"strayflags {strayFlags.Count}, strayRecords {strayRecords.Count}, iteration {iteration}");
                    }
                }
                finally
                {
                    done = true;
                }
            }

            unsafe void runTakeThread(int tid)
            {
                while (!done)
                {
                    if (freeRecordPool.bins[0].TryTake(size, 0, fkv, out long address))
                    {
                        var prevFlag = Interlocked.CompareExchange(ref flags[address - AddressIncrement], 0, 1);
                        Assert.AreEqual(1, prevFlag);
                    }
                }
            }

            // Task rather than Thread for propagation of exception.
            List<Task> tasks = new();
            for (int t = 0; t < numAddThreads; t++)
            { 
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runAddThread(tid)));
            }
            for (int t = 0; t < numTakeThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runTakeThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }

        public enum WrapMode { Wrap, NoWrap };
        const int TakeSize = 40;

        private FreeRecordPool<SpanByte, SpanByte> CreateBestFitTestPool(int scanLimit, WrapMode wrapMode)
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = TakeSize + 8,
                NumberOfRecords = 64,
                BestFitScanLimit = scanLimit
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(fkv, binDef);

            const int minAddress = AddressIncrement - 10;
            if (wrapMode == WrapMode.Wrap)
            {
                // Add too-small records to wrap around the end of the bin records. Use lower addresses so we don't mix up the "real" results.
                const int smallSize = TakeSize - 4;
                for (var ii = 0; ii < freeRecordPool.bins[0].recordCount - 2; ++ii)
                    Assert.IsTrue(freeRecordPool.TryAdd(minAddress + ii + 1, smallSize));

                // Now take out the four at the beginning.
                RevivificationTestUtils.WaitForSafeLatestAddedEpoch(fkv, TakeSize, freeRecordPool);
                for (var ii = 0; ii < 4; ++ii)
                    Assert.IsTrue(freeRecordPool.TryTake(smallSize, minAddress, out _));
            }

            long address = AddressIncrement;
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 1));
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 2));
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 3));
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize));    // 4
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize));    // 5 
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize));

            RevivificationTestUtils.WaitForSafeLatestAddedEpoch(fkv, TakeSize, freeRecordPool);
            return freeRecordPool;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialBestFitTest([Values] WrapMode wrapMode)
        {
            // We should first Take the first 20-length due to exact fit, then skip over the empty to take the next 20, then we have
            // no exact fit within the scan limit, so we grab the best fit before that (21).
            using var freeRecordPool = CreateBestFitTestPool(scanLimit: 4, wrapMode);
            var minAddress = AddressIncrement;
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out var address));
            Assert.AreEqual(4, address -= AddressIncrement);
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address));
            Assert.AreEqual(5, address -= AddressIncrement);
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address));
            Assert.AreEqual(1, address -= AddressIncrement);

            // Now that we've taken the first item, the new first-fit will be moved up one, which brings the last exact-fit into scanLimit range.
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address));
            Assert.AreEqual(6, address -= AddressIncrement);

            // Now Take will return them in order until we have no more
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address));
            Assert.AreEqual(2, address -= AddressIncrement);
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address));
            Assert.AreEqual(3, address -= AddressIncrement);
            Assert.IsFalse(freeRecordPool.TryTake(TakeSize, minAddress, out address));
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialFirstFitTest([Values] WrapMode wrapMode)
        {
            // We should Take the addresses in order.
            using var freeRecordPool = CreateBestFitTestPool(scanLimit: RevivificationBin.UseFirstFit, wrapMode);
            var minAddress = AddressIncrement;

            long address = -1;
            for (var ii = 0; ii < 6; ++ii)
            {
                long epoch = fkv.epoch.CurrentEpoch, safeEpoch = fkv.epoch.SafeToReclaimEpoch;
                if (!freeRecordPool.TryTake(TakeSize, minAddress, out address))
                {
                    Debugger.Launch();
                    Assert.Fail($"Take failed at ii {ii}: currentEpoch {fkv.epoch.CurrentEpoch}, safeEpoch {fkv.epoch.SafeToReclaimEpoch}, pool.HasSafeRecords {freeRecordPool.HasSafeRecords}");
                }
                Assert.AreEqual(ii + 1, address -= AddressIncrement, $"address comparison failed at ii {ii}");
            }
            Assert.IsFalse(freeRecordPool.TryTake(TakeSize, minAddress, out address));
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialThreadContentionOnOneRecordTest()
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = 32,
                NumberOfRecords = 32
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(fkv, binDef);
            const long TestAddress = AddressIncrement, minAddress = AddressIncrement - 10;
            long counter = 0, globalAddress = 0;
            const int size = 20;
            const int numIterations = 10000;

            unsafe void runThread(int tid)
            {
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    if (freeRecordPool.TryTake(size, minAddress, out long address))
                    {
                        ++counter;
                    }
                    else if (globalAddress == TestAddress && Interlocked.CompareExchange(ref globalAddress, 0, TestAddress) == TestAddress)
                    {
                        Assert.IsTrue(freeRecordPool.TryAdd(TestAddress, size), $"Failed TryAdd on iter {iteration}");
                        ++counter;
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < 8; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());

            Assert.IsTrue(counter == 0);
        }

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void LiveThreadContentionOnOneRecordTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            const int numIterations = 10000;
            const int numDeleteThreads = 5, numUpdateThreads = 5;
            const int keyRange = numDeleteThreads;

            unsafe void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = fkv.For(new RevivificationStressFunctions(keyComparer: null)).NewSession<RevivificationStressFunctions>();

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numDeleteThreads)
                    {
                        var kk = rng.Next(keyRange);
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

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numUpdateThreads)
                    {
                        var kk = rng.Next(keyRange);
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

        public enum ThreadingPattern { SameKeys, RandomKeys };

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void LiveFreeListThreadStressTest([Values] CollisionRange collisionRange,
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
        public void LiveInChainThreadStressTest([Values(CollisionRange.Ten)] CollisionRange collisionRange,
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
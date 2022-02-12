// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test.Revivification
{
    public enum DeleteDest { FreeList, InChain };

    public enum CollisionRange { Ten = 10, None = int.MaxValue }

    internal struct RevivificationSpanByteComparer : IFasterEqualityComparer<SpanByte>
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
        public long GetHashCode64(ref SpanByte k) => (defaultComparer.GetHashCode64(ref k) >> 4) % (long)this.collisionRange;
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

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, RevivificationFixedLenFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 },
                                            disableLocking: false, maxFreeRecordsInBin: 1024);
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
                Assert.AreEqual(Status.OK, session.Upsert(key, key * valueMult));
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleFixedLenTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var deleteKey = 42;
            var tailAddress = fht.Log.TailAddress;
            session.Delete(deleteKey);
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;
            var updateValue = updateKey + valueMult;

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(updateKey, updateValue);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(updateKey, updateValue);
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }
    }

    [TestFixture]
    class RevivificationVarLenTests
    {
        const int InitialLength = 50;
        const int GrowLength = InitialLength + 75;      // Must be large enough to go to next bin
        const int ShrinkLength = InitialLength - 25;    // Must be small enough to go to previous bin

        const int OversizeLength = FreeRecord.kMaxSize + 42;

        internal class RevivificationSpanByteFunctions : SpanByteFunctions<Empty>
        {
            internal FasterKV<SpanByte, SpanByte> fht;
            internal RevivificationVLS vls;

            internal int expectedConcurrentDestLength = InitialLength;
            internal int expectedSingleDestLength = InitialLength;
            internal int expectedConcurrentFullValueLength = -1;
            internal int expectedSingleFullValueLength = -1;
            internal int expectedInputLength = InitialLength;

            internal bool readCcCalled, rmwCcCalled;

            internal RevivificationSpanByteFunctions(FasterKV<SpanByte, SpanByte> fht, RevivificationVLS vls)
            {
                this.fht = fht;
                this.vls = vls;
            }

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address, WriteReason reason) 
                => InitialUpdater(ref key, ref input, ref dst, ref output, ref recordInfo, ref updateInfo, address);

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) 
                => InPlaceUpdater(ref key, ref input, ref dst, ref output, ref recordInfo, ref updateInfo, address);

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                // SpanByte reads the available length as value.Length
                Assert.AreEqual(recordInfo.Filler ? updateInfo.FullValueLength : expectedSingleDestLength, value.Length);
                Assert.AreEqual(expectedSingleFullValueLength, updateInfo.FullValueLength);
                Assert.AreEqual(expectedSingleFullValueLength, updateInfo.UsedValueLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);
                updateInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                // SpanByte reads the available length as newValue.Length
                Assert.AreEqual(recordInfo.Filler ? updateInfo.FullValueLength : expectedSingleDestLength, newValue.Length);
                Assert.AreEqual(expectedSingleFullValueLength, updateInfo.FullValueLength);
                Assert.AreEqual(expectedSingleFullValueLength, updateInfo.UsedValueLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                if (input.Length > newValue.Length)
                    return false;
                input.CopyTo(ref newValue);
                updateInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
            {
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, updateInfo.FullValueLength);
                Assert.AreEqual(RoundUpSpanByteUsedLength(expectedConcurrentDestLength), updateInfo.UsedValueLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);      // Does not change dst.Length, which is fine for everything except shrinking (we've allocated sufficient space in other cases)
                if (input.Length < value.Length)
                    value.Length = input.Length;
                updateInfo.UsedValueLength = input.TotalSize;
                return true;
            }

            public override void SingleDeleter(ref SpanByte key, ref SpanByte value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
            {
                Assert.AreEqual(expectedSingleDestLength, value.Length);
                Assert.AreEqual(expectedSingleFullValueLength, updateInfo.FullValueLength);
                Assert.AreEqual(RoundUpSpanByteUsedLength(expectedConcurrentDestLength), updateInfo.UsedValueLength);  // concurrent bc this has data in it
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                updateInfo.UsedValueLength = 0;
                value = default;
            }

            public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address)
            {
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, updateInfo.FullValueLength);
                Assert.AreEqual(RoundUpSpanByteUsedLength(expectedConcurrentDestLength), updateInfo.UsedValueLength);
                Assert.GreaterOrEqual(address, fht.hlog.ReadOnlyAddress);
                updateInfo.UsedValueLength = 0;
                value = default;
                return true;
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
        public class RevivificationVLS : IVariableLengthStruct<SpanByte, SpanByte>
        {
            public int GetInitialLength(ref SpanByte input) => input.TotalSize;

            public int GetLength(ref SpanByte value, ref SpanByte input) => input.TotalSize;
        }

        static int RoundUpSpanByteFullValueLength(SpanByte input) => RoundupTotalSizeFullValue(input.TotalSize);

        static int RoundUpSpanByteFullValueLength(int dataLength) => RoundupTotalSizeFullValue(sizeof(int) + dataLength);

        internal static int RoundupTotalSizeFullValue(int length) => (length + VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment - 1) & (~(VariableLengthBlittableAllocator<SpanByte, SpanByte>.kRecordAlignment - 1));

        static int RoundUpSpanByteUsedLength(int dataLength) => RoundUpTotalSizeUsed(sizeof(int) + dataLength);

        static int RoundUpTotalSizeUsed(int totalSize) => FasterKV<SpanByte, SpanByte>.RoundupLength(totalSize);

        const int numRecords = 200;
        const int DefaultMaxRecsPerBin = 1024;

        RevivificationSpanByteFunctions functions;
        RevivificationSpanByteComparer comparer;

        private FasterKV<SpanByte, SpanByte> fht;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            int maxRecsPerBin = DefaultMaxRecsPerBin;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 17, MemorySizeBits = 22 };
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
                if (arg is PendingOp)
                {
                    logSettings.CopyReadsToTail = CopyReadsToTail.FromStorage;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            fht = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings, comparer: comparer, disableLocking: false, maxFreeRecordsInBin: maxRecsPerBin);

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

            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);

            SpanByteAndMemory output = new();

            for (int ii = from; ii < to; ++ii)
            {
                keyVec.Fill((byte)ii);

                Assert.AreEqual(Status.OK, session.Upsert(ref key, ref input, ref input, ref output));
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
            functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(functions.expectedInputLength);
            functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

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
                // What's there now will be what is passed to ConcurrentWriter/IPU (if Shrink, we kept the same value we allocated initially)
                functions.expectedConcurrentFullValueLength = growth == Growth.Shrink ? RoundUpSpanByteFullValueLength(InitialLength) : functions.expectedSingleFullValueLength;

                // Now let's see if we have the correct expected extra length in the destination.
                inputVec = stackalloc byte[InitialLength / 2];  // Grow this from ShrinkLength to InitialLength
                input = SpanByte.FromFixedSpan(inputVec);
                functions.expectedInputLength = InitialLength / 2;
                functions.expectedConcurrentDestLength = InitialLength / 2;
                functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(functions.expectedInputLength);

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
            Assert.AreEqual(Status.OK, session.Delete(ref key));

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            Span<byte> inputVec = stackalloc byte[InitialLength / 2];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength / 2;
            functions.expectedSingleDestLength = InitialLength / 2;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

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
            Assert.AreEqual(Status.OK, session.Delete(ref key));

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
            fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);

            Span<byte> inputVec = stackalloc byte[InitialLength / 2];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength / 2;
            functions.expectedSingleDestLength = InitialLength / 2;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.Greater(fht.Log.TailAddress, tailAddress);
        }

        public enum UpdateKey { Unfound, DeletedAboveRO, DeletedBelowRO, CopiedBelowRO };

        const byte unfound = numRecords + 2;
        const byte delBelowRO = numRecords / 2 - 4;
        const byte copiedBelowRO = numRecords / 2 - 5;

        private long PrepareDeletes(bool stayInChain, byte delAboveRO, FlushMode flushMode)
        {
            Populate(0, numRecords / 2);

            FreeRecordPool pool = default;
            if (stayInChain)
                pool = this.fht.SwapFreeRecordPool(pool);

            // Delete key below (what will be) the readonly line. This is for a target for the test; the record should not be revivified.
            Span<byte> keyVecDelBelowRO = stackalloc byte[10];
            keyVecDelBelowRO.Fill(delBelowRO);
            var delKeyBelowRO = SpanByte.FromFixedSpan(keyVecDelBelowRO);
            Assert.AreEqual(Status.OK, session.Delete(ref delKeyBelowRO));

            if (flushMode == FlushMode.ReadOnly)
                fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
            else if (flushMode == FlushMode.OnDisk)
                fht.Log.FlushAndEvict(wait: true);

            Populate(numRecords / 2 + 1, numRecords);

            var tailAddress = fht.Log.TailAddress;

            // Delete key above the readonly line. This is the record that will be revivified.
            // If not stayInChain, this also puts two elements in the free list; one should be skipped over on dequeue as it is below readonly.
            Span<byte> keyVecDelAboveRO = stackalloc byte[10];
            keyVecDelAboveRO.Fill(delAboveRO);
            var delKeyAboveRO = SpanByte.FromFixedSpan(keyVecDelAboveRO);
            Assert.AreEqual(Status.OK, session.Delete(ref delKeyAboveRO));

            if (stayInChain)
            {
                Assert.AreEqual(0, pool.NumberOfRecords);
                pool = this.fht.SwapFreeRecordPool(pool);
            }

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

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

            long tailAddress = PrepareDeletes(stayInChain, delAboveRO, FlushMode.ReadOnly);

            Span<byte> inputVec = stackalloc byte[InitialLength / 2];
            var input = SpanByte.FromFixedSpan(inputVec);
            SpanByteAndMemory output = new();

            Span<byte> keyVecToTest = stackalloc byte[10];
            var keyToTest = SpanByte.FromFixedSpan(keyVecToTest);

            bool expectReviv;
            if (updateKey == UpdateKey.Unfound || updateKey == UpdateKey.CopiedBelowRO)
            {
                // Unfound key should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain.
                // CopiedBelowRO should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain
                //      (but exercises a different code path than Unfound).
                // CollisionRange.Ten has a valid PreviousAddress so it is not elided from the cache.
                keyVecToTest.Fill(updateKey == UpdateKey.Unfound ? unfound : copiedBelowRO);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedBelowRO)
            {
                // DeletedBelowRO will not match the key for the in-chain above-RO slot, and we cannot reviv below RO or retrieve below-RO from the
                // freelist, so we will always allocate a new record unless we're using the freelist.
                keyVecToTest.Fill(delBelowRO);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedAboveRO)
            {
                // DeletedAboveRO means we will reuse an in-chain record, or will get it from the freelist if deleteDest is FreeList.
                keyVecToTest.Fill(delAboveRO);
                expectReviv = true;
            }
            else 
            {
                Assert.Fail($"Unexpected updateKey {updateKey}");
                expectReviv = false;    // make the compiler happy
            }

            functions.expectedInputLength = InitialLength / 2;
            functions.expectedSingleDestLength = InitialLength / 2;

            // A revivified record will have the full initial value length. A new record here will be created with the half-size input
            // (which we do in these tests because we retrieve from the "next higher bin"
            if (!expectReviv)
                functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref keyToTest, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref keyToTest, ref input);

            if (expectReviv)
                Assert.AreEqual(tailAddress, fht.Log.TailAddress);
            else
                Assert.Greater(fht.Log.TailAddress, tailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleMidChainRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                               [Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;
            FreeRecordPool pool = default;
            if (stayInChain)
                pool = this.fht.SwapFreeRecordPool(pool);

            // This freed record stays in the hash chain.
            byte chainKey = numRecords / 2 - 1;
            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);

            Assert.AreEqual(Status.OK, session.Delete(ref key));

            var tailAddress = fht.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
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
            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);
            var hash = comparer.GetHashCode64(ref key);

            List<byte> deletedSlots = new();
            for (int ii = chainKey + 1; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                if (comparer.GetHashCode64(ref key) != hash)
                    continue;
                Assert.AreEqual(Status.OK, session.Delete(ref key));
                deletedSlots.Add((byte)ii);
            }

            // For this test we're still limiting to byte repetition
            Assert.Greater(255 - numRecords, deletedSlots.Count);
            Assert.AreEqual(0, fht.FreeRecordPool.NumberOfRecords);
            Assert.Greater(deletedSlots.Count, 5);    // should be about Ten
            var tailAddress = fht.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            for (int ii = 0; ii < deletedSlots.Count; ++ii)
            {
                keyVec.Fill(deletedSlots[ii]);
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
            }
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.None)] CollisionRange collisionRange,
                                                    [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            long tailAddress = fht.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[10];
            var key = SpanByte.FromFixedSpan(keyVec);

            // Delete
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                Assert.AreEqual(Status.OK, session.Delete(ref key));
            }
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            Assert.Greater(fht.FreeRecordPool.NumberOfRecords, 0);

            // Again, allocate at half-size due to "retrieve from next-highest bin".
            Span<byte> inputVec = stackalloc byte[InitialLength / 2];
            var input = SpanByte.FromFixedSpan(inputVec);
            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength / 2;
            functions.expectedSingleDestLength = InitialLength / 2;

            // These come from the existing initial allocation so keep the full length
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            // Revivify
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                Assert.AreEqual(tailAddress, fht.Log.TailAddress, $"unexpected new record for key {ii}");
            }

            Assert.AreEqual(0, fht.FreeRecordPool.NumberOfRecords);

            // Confirm
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                Assert.AreEqual(Status.OK, session.Read(ref key, ref output));
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void EnqueueBinSelectionTest()
        {
            int expectedBin = 0, actualBin;

            int size = FreeRecordPool.InitialBinSize;
            for ( ; size <= FreeRecord.kMaxSize; size *= 2)
            {
                Assert.IsTrue(fht.FreeRecordPool.GetEnqueueBinIndex(size - 1, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                Assert.IsTrue(fht.FreeRecordPool.GetEnqueueBinIndex(size, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);

                if (size == FreeRecord.kMaxSize)
                    break;

                Assert.IsTrue(fht.FreeRecordPool.GetEnqueueBinIndex(size + 1, out actualBin));
                Assert.AreEqual(expectedBin + 1, actualBin);
                ++expectedBin;
            }

            // The last increment in the loop above went to oversize
            Assert.IsFalse(fht.FreeRecordPool.GetEnqueueBinIndex(size + 1, out actualBin));
            Assert.AreEqual(-1, actualBin);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DequeueBinSelectionTest()
        {
            int expectedBin = 0;

            // First bin is special because it is fixed size, and the second bin is used only as an overflow from the first.
            var size = FreeRecordPool.InitialBinSize;
            Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(size - 1, out int actualBin));
            Assert.AreEqual(expectedBin, actualBin);
            Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(size, out actualBin));
            Assert.AreEqual(expectedBin, actualBin);

            expectedBin = 2;
            Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(size + 1, out actualBin));
            Assert.AreEqual(expectedBin, actualBin);

            for (size *= 2; size <= FreeRecord.kMaxSize / 2; size *= 2)
            {
                Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(size - 1, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(size, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);

                if (size == FreeRecord.kMaxSize / 2)
                    break;

                Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(size + 1, out actualBin));
                Assert.AreEqual(expectedBin + 1, actualBin);
                ++expectedBin;
            }

            // The last increment in the loop above went to oversize
            Assert.IsFalse(fht.FreeRecordPool.GetDequeueBinIndex(size + 1, out actualBin));
            Assert.AreEqual(-1, actualBin);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialBinWrappingTest()
        {
            Populate();

            // Different sizes as we dequeue from the next-higher bin
            const int enqueueRecordSize = 42;
            const int dequeueRecordSize = 31;

            FreeRecordBin.GetPartitionSizes(DefaultMaxRecsPerBin, out _, out int partitionSize, out _);
            Assert.IsTrue(fht.FreeRecordPool.GetEnqueueBinIndex(enqueueRecordSize, out int binIndex));
            Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(dequeueRecordSize, out int binIndex2));
            Assert.AreEqual(binIndex, binIndex2);
            var bin = fht.FreeRecordPool.bins[binIndex];

            var initialPartition = bin.GetInitialPartitionIndex();

            const int minAddress = 1_000;
            int logicalAddress = 1_000_000;

            var partitionStart = bin.GetPartitionStart(initialPartition);
            ref int head = ref Unsafe.AsRef<int>((int*)partitionStart);
            ref int tail = ref Unsafe.AsRef<int>((int*)partitionStart + 1);
            Assert.AreEqual(1, head);
            Assert.AreEqual(1, tail);

            // Fill the partition.
            var count = 0;
            for (var ii = 1; ii < partitionSize - 1; ++ii)
            {
                Assert.IsTrue(bin.Enqueue(logicalAddress + ii, enqueueRecordSize));
                Assert.AreEqual(1, head);
                Assert.AreEqual(1 + ii, tail);
                ++count;
            }
            Assert.AreEqual(partitionSize - 1, tail);

            // partitionSize - 2 because:
            //   The first element of the partition is head/tail
            //   The tail cannot be incremented to be equal to head (or the list would be considered empty), so we lose one element of capacity
            Assert.AreEqual(partitionSize - 2, count);  

            // Dequeue one to open up a space in the partition. Note: the type specification for the function is not needed here as we do not pass hlog)
            Assert.IsTrue(bin.Dequeue<int, int>(dequeueRecordSize, minAddress, out _));
            Assert.AreEqual(2, head);
            Assert.AreEqual(partitionSize - 1, tail);
            --count;

            // Now wrap with an enqueue
            Assert.IsTrue(bin.Enqueue(logicalAddress + partitionSize, enqueueRecordSize));
            Assert.AreEqual(2, head);
            Assert.AreEqual(1, tail);
            ++count;

            // Dequeue to the end of the bin.
            for (var ii = 2; ii < partitionSize - 1; ++ii)
            {
                Assert.IsTrue(bin.Dequeue<int, int>(dequeueRecordSize, minAddress, out _));
                Assert.AreEqual(1 + ii, head);
                Assert.AreEqual(1, tail);
                --count;
            }
            Assert.AreEqual(partitionSize - 1, head);
            Assert.AreEqual(1, tail);
            Assert.AreEqual(1, count);

            // And now wrap the dequeue to empty the bin.
            Assert.IsTrue(bin.Dequeue<int, int>(dequeueRecordSize, minAddress, out _));
            Assert.AreEqual(1, head);
            Assert.AreEqual(1, tail);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialPartitionWrappingTest()
        {
            Populate();

            // Different sizes as we dequeue from the next-higher bin
            const int enqueueRecordSize = 42;
            const int dequeueRecordSize = 31;

            FreeRecordBin.GetPartitionSizes(DefaultMaxRecsPerBin, out int partitionCount, out int partitionSize, out _);
            Assert.IsTrue(fht.FreeRecordPool.GetEnqueueBinIndex(enqueueRecordSize, out int binIndex));
            Assert.IsTrue(fht.FreeRecordPool.GetDequeueBinIndex(dequeueRecordSize, out int binIndex2));
            Assert.AreEqual(binIndex, binIndex2);
            var bin = fht.FreeRecordPool.bins[binIndex];

            var initialPartition = bin.GetInitialPartitionIndex();

            const int minAddress = 1_000;
            int logicalAddress = 1_000_000;
            var count = 0;

            // Fill up all partitions to the end of the bin.
            for (var iPart = initialPartition; iPart < partitionCount; ++iPart)
            {
                // Fill the partition; partitionSize - 2 because:
                //   The first element of the partition is head/tail
                //   The tail cannot be incremented to be equal to head (or the list would be considered empty), so we lose one element of capacity
                for (var ii = 0; ii < partitionSize - 2; ++ii)
                {
                    Assert.IsTrue(bin.Enqueue(logicalAddress + ii, enqueueRecordSize), $"Failed to enqueue ii {ii}, iPart {iPart}, initialPart {initialPartition}");
                    ++count;
                }
            }

            // Prepare for wrap: Get partition 0's info
            var partitionStart = bin.GetPartitionStart(0);
            ref int head = ref Unsafe.AsRef<int>((int*)partitionStart);
            ref int tail = ref Unsafe.AsRef<int>((int*)partitionStart + 1);
            Assert.AreEqual(1, head);
            Assert.AreEqual(1, tail);

            // Add to the bin, which will cause this to wrap.
            Assert.IsTrue(bin.Enqueue(logicalAddress + 1, enqueueRecordSize));
            Assert.IsTrue(bin.Enqueue(logicalAddress + 2, enqueueRecordSize));
            Assert.AreEqual(1, head);
            Assert.AreEqual(3, tail);
            count += 2;

            // Now dequeue everything
            for (; count > 0; --count)
                Assert.IsTrue(bin.Dequeue<int, int>(dequeueRecordSize, minAddress, out _));

            Assert.AreEqual(3, head);
            Assert.AreEqual(3, tail);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void LiveBinWrappingTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            // Note: this test assumes no collisions (every delete goes to the FreeList)

            FreeRecordBin.GetPartitionSizes(DefaultMaxRecsPerBin, out int partitionCount, out int partitionSize, out int recordCount);

            long tailAddress = fht.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[10];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            // Smaller input for revivification, due to the next-higher-bin dequeueing
            Span<byte> revivInputVec = stackalloc byte[InitialLength / 2];
            var revivInput = SpanByte.FromFixedSpan(revivInputVec);

            SpanByteAndMemory output = new();

            // Pick some number that won't align with the bin size, so we wrap
            var numKeys = partitionSize - 3;

            for (var iter = 0; iter < 100; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < numKeys; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    Assert.AreEqual(Status.OK, session.Delete(ref key));
                    Assert.AreEqual(ii + 1, fht.FreeRecordPool.NumberOfRecords);
                    Assert.AreEqual(tailAddress, fht.Log.TailAddress);
                }

                // Revivify
                functions.expectedInputLength = InitialLength / 2;
                functions.expectedSingleDestLength = InitialLength / 2;
                functions.expectedConcurrentDestLength = InitialLength / 2;
                for (var ii = 0; ii < numKeys; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    if (updateOp == UpdateOp.Upsert)
                        session.Upsert(ref key, ref revivInput, ref input, ref output);
                    else if (updateOp == UpdateOp.RMW)
                        session.RMW(ref key, ref revivInput);
                    session.Upsert(ref key, ref revivInput, ref input, ref output);
                    Assert.AreEqual(tailAddress, fht.Log.TailAddress, $"unexpected new record for key {ii} iter {iter}");
                    Assert.AreEqual(numKeys - ii - 1, fht.FreeRecordPool.NumberOfRecords);
                }

                Assert.AreEqual(0, fht.FreeRecordPool.NumberOfRecords);
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
            FreeRecordPool pool = default;
            if (stayInChain)
                pool = this.fht.SwapFreeRecordPool(pool);

            Span<byte> inputVec = stackalloc byte[OversizeLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            SpanByteAndMemory output = new();

            byte chainKey = numRecords + 1;
            Span<byte> keyVec = stackalloc byte[10];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);

            // Oversize records do not go to "next higher" bin (there is no next-higher)
            functions.expectedInputLength = OversizeLength;
            functions.expectedSingleDestLength = OversizeLength;
            functions.expectedConcurrentDestLength = OversizeLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(OversizeLength);

            // Initial insert of the oversize record
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            // Delete it
            Assert.AreEqual(Status.OK, session.Delete(ref key));

            var tailAddress = fht.Log.TailAddress;

            // Revivify in the chain. Because this is oversize, the expectedFullValueLength remains the same
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }

        public enum PendingOp { Read, RMW };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimplePendingOpsRevivifyTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.None)] CollisionRange collisionRange,
                                                 [Values] PendingOp pendingOp)
        {
            byte delAboveRO = (byte)(numRecords - 2);   // Will be sent to free list
            byte targetRO = (byte)numRecords / 2 - 15;

            long tailAddress = PrepareDeletes(stayInChain: false, delAboveRO, FlushMode.OnDisk);

            // We always want freelist for this test.
            Assert.IsTrue(fht.FreeRecordPool.HasRecords);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength / 2;
            functions.expectedSingleDestLength = InitialLength / 2;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            // Use a different key below RO than we deleted; this will go pending to retrieve it
            Span<byte> keyVec = stackalloc byte[10];
            var key = SpanByte.FromFixedSpan(keyVec);

            session.ctx.phase = phase;
            if (pendingOp == PendingOp.Read)
            {
                // Because of the "next-higher bin" dequeue in the FreeRecordPool, add a larger record here, then delete it so it can be dequeued.
                Span<byte> inputVec = stackalloc byte[GrowLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                // Set to expect the longer input length
                functions.expectedInputLength = GrowLength;
                functions.expectedSingleDestLength = GrowLength;
                functions.expectedConcurrentDestLength = GrowLength;
                functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(GrowLength);

                keyVec.Fill(numRecords + 1);
                Assert.AreEqual(Status.OK, session.Upsert(ref key, ref input, ref input, ref output));
                Assert.AreEqual(Status.OK, session.Delete(ref key));
                tailAddress = fht.Log.TailAddress;

                // Now restore to expect the shorter input length (Same Value Length because we're reusing the old record)
                functions.expectedInputLength = InitialLength / 2;
                functions.expectedSingleDestLength = InitialLength;
                functions.expectedConcurrentDestLength = InitialLength;

                var spanSlice = inputVec.Slice(0, InitialLength / 2);
                var inputSlice = SpanByte.FromFixedSpan(spanSlice);

                keyVec.Fill(targetRO);
                Assert.AreEqual(Status.PENDING, session.Read(ref key, ref inputSlice, ref output));
                session.CompletePending(wait: true);
                Assert.IsTrue(functions.readCcCalled);
            }
            else if (pendingOp == PendingOp.RMW)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength / 2];
                var input = SpanByte.FromFixedSpan(inputVec);

                keyVec.Fill(targetRO);

                session.RMW(ref key, ref input);
                session.CompletePending(wait: true);
                Assert.IsTrue(functions.rmwCcCalled);
            }
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }
    }

    [TestFixture]
    class RevivificationObjectTests
    {
        const int numRecords = 1000;
        internal const int valueMult = 1_000_000;

        private MyFunctions functions;
        private FasterKV<MyKey, MyValue> fht;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions> session;
        private IDevice log;
        private IDevice objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: true);

            fht = new FasterKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 22, PageSizeBits = 12 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() },
                disableLocking: false, maxFreeRecordsInBin: 1024);

            functions = new MyFunctions();
            session = fht.For(functions).NewSession<MyFunctions>();
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
                Assert.AreEqual(Status.OK, session.Upsert(keyObj, valueObj));
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleObjectTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var deleteKey = 42;
            var tailAddress = fht.Log.TailAddress;
            session.Delete(new MyKey { key = deleteKey });
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;

            var key = new MyKey { key = updateKey };
            var value = new MyValue { value = key.key + valueMult };
            var input = new MyInput { value = value.value };

            session.ctx.phase = phase;
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(key, value);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(key, input);
            Assert.AreEqual(tailAddress, fht.Log.TailAddress);
        }
    }

#if false // TODOtest
    [TestFixture]
    class RevivificationVarLenStressTests
    {
        const int InitialLength = 50;

        internal class RevivificationStressFunctions : SpanByteFunctions<Empty>
        {
            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address) 
                => InitialUpdater(ref key, ref input, ref dst, ref output, ref recordInfo, ref usedValueLength, fullValueLength, address);

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address)
                => InPlaceUpdater(ref key, ref input, ref dst, ref output, ref recordInfo, ref usedValueLength, fullValueLength, address);

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address)
            {
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);
                usedValueLength = input.TotalSize;
                return true;
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address)
            {
                if (input.Length > newValue.Length)
                    return false;
                input.CopyTo(ref newValue);
                usedValueLength = input.TotalSize;
                return true;
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address)
            {
                if (input.Length > value.Length)
                    return false;
                input.CopyTo(ref value);      // Does not change dst.Length, which is fine for everything except shrinking (we've allocated sufficient space in other cases)
                if (input.Length < value.Length)
                    value.Length = input.Length;
                usedValueLength = input.TotalSize;
                return true;
            }

            public override void SingleDeleter(ref SpanByte key, ref SpanByte value, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address)
            {
                usedValueLength = 0;
                value = default;
            }

            public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address)
            {
                usedValueLength = 0;
                value = default;
                return true;
            }
        }

        const int numRecords = 1000;
        const int DefaultMaxRecsPerBin = 1024;

        RevivificationStressFunctions functions;
        RevivificationSpanByteComparer comparer;

        private FasterKV<SpanByte, SpanByte> fht;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            int maxRecsPerBin = DefaultMaxRecsPerBin;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 17, MemorySizeBits = 22 };
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            fht = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings, comparer: comparer, disableLocking: false, maxFreeRecordsInBin: maxRecsPerBin);

            var valueVLS = new RevivificationVLS();
            functions = new RevivificationStressFunctions();
            session = fht.For(functions).NewSession<RevivificationStressFunctions>(
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

        unsafe void Populate()
        {
            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            for (int ii = 0; ii < numRecords; ++ii)
            {
                var keyVec = BitConverter.GetBytes(ii);
                fixed (byte* _ = keyVec)
                {
                    var key = SpanByte.FromFixedSpan(keyVec);
                    Assert.AreEqual(Status.OK, session.Upsert(ref key, ref input, ref input, ref output));
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void ArtificialFreeBinThreadStressTest()
        {
            const int numIterations = 100;
            const int numItems = 10000;
            var flags = new long[numItems];
            const int size = 42;    // size doesn't matter in this test

            var bin = new FreeRecordBin(numItems + 1, size);
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
                            bin.Enqueue(ii, size);
                        }

                        // Make sure all were dequeued. Sleep a bit for the dequeue threads to pull out the last records.
                        Thread.Sleep(10);
                        List<int> strays = new();
                        for (var ii = 1; ii < numItems; ++ii)
                        {
                            if (flags[ii] != 0)
                                strays.Add(ii);
                        }
                        Assert.AreEqual(0, strays.Count);
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
                    if (bin.Dequeue<int, int>(size, 0, out long address))
                    {
                        var prevFlag = Interlocked.CompareExchange(ref flags[address], 0, 1);
                        Assert.AreEqual(1, prevFlag);
                    }
                }
            }

            int numDequeueThreads = 5;

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            tasks.Add(Task.Factory.StartNew(() => runEnqueueThread(0)));
            for (int t = 1; t < numDequeueThreads; t++)
            {
                var tid = t;
                tasks.Add(Task.Factory.StartNew(() => runDequeueThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }

        public enum ThreadingPattern { SameKeys, RandomKeys };

        [Test]
        [Category(RevivificationCategory)]
        public void LiveFreeListThreadStressTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] CollisionRange collisionRange,
                                             [Values] ThreadingPattern threadingPattern, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            int numIterations = 100;
            int numThreadsEachOp = 1;

            unsafe void runDeleteThread(int tid)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                using var localSession = fht.For(new RevivificationStressFunctions()).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numThreadsEachOp)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(numRecords) : ii;
                        var keyVec = BitConverter.GetBytes(kk);
                        fixed (byte* _ = keyVec)
                        {
                            var key = SpanByte.FromFixedSpan(keyVec);
                            Thread.Sleep(rng.Next(10));
                            localSession.Delete(key);
                        }
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength / 2];       // /2 because of "next-highest bin" dequeueing
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                using var localSession = fht.For(new RevivificationStressFunctions()).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numThreadsEachOp)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(numRecords) : ii;
                        var keyVec = BitConverter.GetBytes(kk);
                        fixed (byte* _ = keyVec)
                        {
                            var key = SpanByte.FromFixedSpan(keyVec);
                            if (updateOp == UpdateOp.Upsert)
                                localSession.Upsert(key, input);
                            else
                                localSession.RMW(key, input);
                        }
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numThreadsEachOp; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numThreadsEachOp; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }

        [Test]
        [Category(RevivificationCategory)]
        public void LiveInChainThreadStressTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                                [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            // Turn off freelist.
            fht.SwapFreeRecordPool(null);

            int numIterations = 100;
            int numThreadsEachOp = 1;

            unsafe void runDeleteThread(int tid)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                using var localSession = fht.For(new RevivificationStressFunctions()).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numThreadsEachOp)
                    {
                        var keyVec = BitConverter.GetBytes(ii);
                        fixed (byte* _ = keyVec)
                        {
                            var key = SpanByte.FromFixedSpan(keyVec);
                            localSession.Delete(key);
                        }
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength / 2];       // /2 because of "next-highest bin" dequeueing
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                using var localSession = fht.For(new RevivificationStressFunctions()).NewSession<RevivificationStressFunctions>();
                localSession.ctx.phase = phase;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numThreadsEachOp)
                    {
                        var keyVec = BitConverter.GetBytes(ii);
                        fixed (byte* _ = keyVec)
                        {
                            var key = SpanByte.FromFixedSpan(keyVec);
                            if (updateOp == UpdateOp.Upsert)
                                localSession.Upsert(key, input);
                            else
                                localSession.RMW(key, input);
                        }
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numThreadsEachOp; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numThreadsEachOp; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
#endif
}
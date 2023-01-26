// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;
using FASTER.test.ReadCacheTests;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;
using System.Diagnostics;
using FASTER.test.LockTable;

namespace FASTER.test.LockableUnsafeContext
{
    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    internal class LockableUnsafeFunctions : SimpleFunctions<long, long>
    {
        internal long recordAddress;

        public override void PostSingleDeleter(ref long key, ref DeleteInfo deleteInfo)
        {
            recordAddress = deleteInfo.Address;
        }

        public override bool ConcurrentDeleter(ref long key, ref long value, ref DeleteInfo deleteInfo)
        {
            recordAddress = deleteInfo.Address;
            return true;
        }
    }

    internal class LockableUnsafeComparer : IFasterEqualityComparer<long>
    {
        internal int maxSleepMs;
        readonly Random rng = new(101);

        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        public long GetHashCode64(ref long k)
        {
            if (maxSleepMs > 0)
                Thread.Sleep(rng.Next(maxSleepMs));
            return Utility.GetHashCode(k);
        }
    }

    public enum ResultLockTarget { MutableLock, LockTable }

    internal struct BucketLockTracker
    {
        Dictionary<int /* bucketIndex */, (int x, int s)> buckets;

        public BucketLockTracker()
        {
            buckets = new();
        }

        internal void Increment(int bucketIndex, LockType lockType)
        {
            if (lockType == LockType.Exclusive)
                IncrementX(bucketIndex);
            else
                IncrementS(bucketIndex);
        }
        internal void Decrement(int bucketIndex, LockType lockType)
        {
            if (lockType == LockType.Exclusive)
                DecrementX(bucketIndex);
            else
                DecrementS(bucketIndex);
        }

        internal void IncrementX(int bucketIndex) => AddX(bucketIndex, 1);
        internal void DecrementX(int bucketIndex) => AddX(bucketIndex, -1);
        internal void IncrementS(int bucketIndex) => AddS(bucketIndex, 1);
        internal void DecrementS(int bucketIndex) => AddS(bucketIndex, -1);

        private void AddX(int bucketIndex, int addend)
        {
            if (!buckets.TryGetValue(bucketIndex, out var counts))
                counts = default;
            counts.x += addend;
            Assert.GreaterOrEqual(counts.x, 0);
            buckets[bucketIndex] = counts;
        }

        private void AddS(int bucketIndex, int addend)
        {
            if (!buckets.TryGetValue(bucketIndex, out var counts))
                counts = default;
            counts.s += addend;
            Assert.GreaterOrEqual(counts.s, 0);
            buckets[bucketIndex] = counts;
        }

        internal bool GetLockCounts(int bucketIndex, out (int x, int s) counts)
        {
            if (!buckets.TryGetValue(bucketIndex, out counts))
            {
                counts = default;
                return false;
            }
            return true;
        }

        internal void AssertNoLocks()
        {
            foreach (var kvp in buckets)
            {
                Assert.AreEqual(0, kvp.Value.x);
                Assert.AreEqual(0, kvp.Value.s);
            }
        }
    }

    [TestFixture]
    class LockableUnsafeContextTests
    {
        const int numRecords = 1000;
        const int useNewKey = 1010;
        const int useExistingKey = 200;

        const int valueMult = 1_000_000;

        LockableUnsafeFunctions functions;
        LockableUnsafeComparer comparer;

        private FasterKV<long, long> fht;
        private ClientSession<long, long, long, long, Empty, LockableUnsafeFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup() => Setup(forRecovery: false);

        public void Setup(bool forRecovery)
        {
            if (!forRecovery)
            {
                DeleteDirectory(MethodTestDir, wait: true);
            }
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false, recoverDevice: forRecovery);

            ReadCacheSettings readCacheSettings = default;
            CheckpointSettings checkpointSettings = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    break;
                }
                if (arg is CheckpointType chktType)
                {
                    checkpointSettings = new CheckpointSettings { CheckpointDir = MethodTestDir };
                    break;
                }
            }

            comparer = new LockableUnsafeComparer();
            functions = new LockableUnsafeFunctions();

            fht = new FasterKV<long, long>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                            checkpointSettings: checkpointSettings, comparer: comparer,
                                            lockingMode: LockingMode.SessionControlled);
            session = fht.For(functions).NewSession<LockableUnsafeFunctions>();
        }

        [TearDown]
        public void TearDown() => TearDown(forRecovery: false);

        public void TearDown(bool forRecovery)
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;

            if (!forRecovery)
            {
                DeleteDirectory(MethodTestDir);
            }
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
        }

        void AssertIsLocked(LockableUnsafeContext<long, long, long, long, Empty, LockableUnsafeFunctions> luContext, long key, bool xlock, bool slock)
        {
            OverflowBucketLockTableTests.AssertLockCounts(fht, ref key, xlock, slock);
        }

        void PrepareRecordLocation(FlushMode recordLocation) => PrepareRecordLocation(this.fht, recordLocation);

        static void PrepareRecordLocation(FasterKV<long, long> fht, FlushMode recordLocation)
        {
            if (recordLocation == FlushMode.ReadOnly)
                fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
            else if (recordLocation == FlushMode.OnDisk)
                fht.Log.FlushAndEvict(wait: true);
        }

        static void ClearCountsOnError(ClientSession<long, long, long, long, Empty, LockableUnsafeFunctions> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        static void ClearCountsOnError(ClientSession<long, long, long, long, Empty, IFunctions<long, long, long, long, Empty>> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        void GetBucket(ref HashEntryInfo hei) => OverflowBucketLockTableTests.GetBucket(fht, ref hei);

        void AssertTotalLockCounts(long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertTotalLockCounts(fht, expectedX, expectedS);

        internal void AssertBucketLockCount(long key, long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertBucketLockCount(fht, key, expectedX, expectedS);

        internal int GetBucketIndex(long key) => OverflowBucketLockTableTests.GetBucketIndex(fht, key);

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task TestShiftHeadAddressLUC([Values] SyncMode syncMode)
        {
            long input = default;
            const int RandSeed = 10;
            const int RandRange = numRecords;
            const int NumRecs = 200;

            Random r = new(RandSeed);
            var sw = Stopwatch.StartNew();

            // Copied from UnsafeContextTests to test Async.
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                for (int c = 0; c < NumRecs; c++)
                {
                    long key1 = r.Next(RandRange);
                    luContext.Lock(key1, LockType.Exclusive);
                    AssertBucketLockCount(key1, 1, 0);

                    var value = key1 + numRecords;
                    if (syncMode == SyncMode.Sync)
                    {
                        luContext.Upsert(ref key1, ref value, Empty.Default, 0);
                    }
                    else
                    {
                        luContext.EndUnsafe();
                        var status = (await luContext.UpsertAsync(ref key1, ref value)).Complete();
                        luContext.BeginUnsafe();
                        Assert.IsFalse(status.IsPending);
                    }
                    luContext.Unlock(key1, LockType.Exclusive);
                    AssertBucketLockCount(key1, 0, 0);
                }

                AssertTotalLockCounts(NumRecs, 0);

                r = new Random(RandSeed);
                sw.Restart();

                for (int c = 0; c < NumRecs; c++)
                {
                    long key1 = r.Next(RandRange);
                    var value = key1 + numRecords;
                    long output = 0;

                    luContext.Lock(key1, LockType.Shared);
                    AssertBucketLockCount(key1, 1, 0);
                    Status status;
                    if (syncMode == SyncMode.Sync || (c % 1 == 0))  // in .Async mode, half the ops should be sync to test CompletePendingAsync
                    {
                        status = luContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                    }
                    else
                    {
                        luContext.EndUnsafe();
                        (status, output) = (await luContext.ReadAsync(ref key1, ref input)).Complete();
                        luContext.BeginUnsafe();
                    }
                    luContext.Unlock(key1, LockType.Shared);
                    AssertBucketLockCount(key1, 0, 0);
                    Assert.IsFalse(status.IsPending);
                }

                AssertTotalLockCounts(0, 0);

                if (syncMode == SyncMode.Sync)
                {
                    luContext.CompletePending(true);
                }
                else
                {
                    luContext.EndUnsafe();
                    await luContext.CompletePendingAsync();
                    luContext.BeginUnsafe();
                }

                // Shift head and retry - should not find in main memory now
                fht.Log.FlushAndEvict(true);

                r = new Random(RandSeed);
                sw.Restart();

                // Since we do random selection with replacement, we may not lock all keys--so need to track which we do
                // Similarly, we need to track bucket counts.
                List<long> lockKeys = new();
                BucketLockTracker blt = new();

                for (int c = 0; c < NumRecs; c++)
                {
                    long key1 = r.Next(RandRange);
                    long output = 0;
                    luContext.Lock(key1, LockType.Shared);
                    lockKeys.Add(key1);
                    blt.IncrementS(GetBucketIndex(key1));
                    Status foundStatus = luContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                    Assert.IsTrue(foundStatus.IsPending);
                }

                AssertTotalLockCounts(0, NumRecs);

                CompletedOutputIterator<long, long, long, long, Empty> outputs;
                if (syncMode == SyncMode.Sync)
                {
                    luContext.CompletePendingWithOutputs(out outputs, wait: true);
                }
                else
                {
                    luContext.EndUnsafe();
                    outputs = await luContext.CompletePendingWithOutputsAsync();
                    luContext.BeginUnsafe();
                }

                foreach (var key in lockKeys)
                {
                    luContext.Unlock(key, LockType.Shared);
                    blt.DecrementS(GetBucketIndex(key));
                }

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);

                int count = 0;
                while (outputs.Next())
                {
                    count++;
                    Assert.AreEqual(outputs.Current.Key + numRecords, outputs.Current.Output);
                }
                outputs.Dispose();
                Assert.AreEqual(NumRecs, count);
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget,
                                              [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                              [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            var useRMW = updateOp == UpdateOp.RMW;
            const int readKey24 = 24, readKey51 = 51;
            long resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : readKey24 + readKey51;
            long resultValue = -1;
            long expectedResult = (readKey24 + readKey51) * valueMult;
            Status status;
            Dictionary<long, LockType> locks = new();
            BucketLockTracker blt = new();

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {
                {   // key scope
                    // Get initial source values
                    int key = readKey24;
                    luContext.Lock(key, LockType.Shared);
                    AssertIsLocked(luContext, key, xlock: false, slock: true);
                    locks[key] = LockType.Shared;
                    blt.Increment(GetBucketIndex(key), locks[key]);

                    key = readKey51;
                    luContext.Lock(key, LockType.Shared);
                    locks[key] = LockType.Shared;
                    AssertIsLocked(luContext, key, xlock: false, slock: true);
                    blt.Increment(GetBucketIndex(key), locks[key]);

                    // Lock destination value.
                    luContext.Lock(resultKey, LockType.Exclusive);
                    locks[resultKey] = LockType.Exclusive;
                    AssertIsLocked(luContext, resultKey, xlock: true, slock: false);
                    blt.Increment(GetBucketIndex(key), locks[key]);

                    AssertTotalLockCounts(1, 2);

                    // Re-get source values, to verify (e.g. they may be in readcache now).
                    // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
                    status = luContext.Read(readKey24, out var readValue24);
                    if (flushMode == FlushMode.OnDisk)
                    {
                        if (status.IsPending)
                        {
                            luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            Assert.True(completedOutputs.Next());
                            readValue24 = completedOutputs.Current.Output;
                            Assert.AreEqual(24 * valueMult, readValue24);
                            Assert.False(completedOutputs.Next());
                            completedOutputs.Dispose();
                        }
                    }
                    else
                    {
                        Assert.IsFalse(status.IsPending, status.ToString());
                    }

                    status = luContext.Read(readKey51, out var readValue51);
                    if (flushMode == FlushMode.OnDisk)
                    {
                        if (status.IsPending)
                        {
                            luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            Assert.True(completedOutputs.Next());
                            readValue51 = completedOutputs.Current.Output;
                            Assert.AreEqual(51 * valueMult, readValue51);
                            Assert.False(completedOutputs.Next());
                            completedOutputs.Dispose();
                        }
                    }
                    else
                    {
                        Assert.IsFalse(status.IsPending, status.ToString());
                    }

                    // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                    session.ctx.phase = phase;
                    long dummyInOut = 0;
                    status = useRMW
                        ? luContext.RMW(ref resultKey, ref expectedResult, ref dummyInOut, out RecordMetadata recordMetadata)
                        : luContext.Upsert(ref resultKey, ref dummyInOut, ref expectedResult, ref dummyInOut, out recordMetadata);
                    if (flushMode == FlushMode.OnDisk)
                    {
                        if (status.IsPending)
                        {
                            luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            Assert.True(completedOutputs.Next());
                            resultValue = completedOutputs.Current.Output;
                            Assert.AreEqual(expectedResult, resultValue);
                            Assert.False(completedOutputs.Next());
                            completedOutputs.Dispose();
                        }
                    }
                    else
                    {
                        Assert.IsFalse(status.IsPending, status.ToString());
                    }

                    // Reread the destination to verify
                    status = luContext.Read(resultKey, out resultValue);
                    Assert.IsFalse(status.IsPending, status.ToString());
                    Assert.AreEqual(expectedResult, resultValue);
                }
                foreach (var key in locks.Keys.OrderBy(key => -key))
                {
                    luContext.Unlock(key, locks[key]);
                    blt.Decrement(GetBucketIndex(key), locks[key]);
                }

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(expectedResult, resultValue);
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemoryLongLockTest([Values] ResultLockTarget resultLockTarget, [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                         [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            const int readKey24 = 24, readKey51 = 51, valueMult2 = 10;
            long resultKey = initialDestWillBeLockTable ? numRecords + 1 : readKey24 + readKey51;
            long resultValue;
            int expectedResult = (readKey24 + readKey51) * valueMult * valueMult2;
            var useRMW = updateOp == UpdateOp.RMW;
            Status status;
            BucketLockTracker blt = new();

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                luContext.Lock(readKey24, LockType.Shared);
                blt.Increment(GetBucketIndex(readKey24), LockType.Shared);
                luContext.Lock(readKey51, LockType.Shared);
                blt.Increment(GetBucketIndex(readKey51), LockType.Shared);
                luContext.Lock(resultKey, LockType.Exclusive);
                blt.Increment(GetBucketIndex(resultKey), LockType.Exclusive);

                AssertTotalLockCounts(1, 2);

                status = luContext.Read(readKey24, out var readValue24);
                if (flushMode == FlushMode.OnDisk)
                {
                    Assert.IsTrue(status.IsPending, status.ToString());
                    luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, readValue24) = GetSinglePendingResult(completedOutputs, out var recordMetadata);
                    Assert.IsTrue(status.Found, status.ToString());
                }
                else
                    Assert.IsFalse(status.IsPending, status.ToString());
                Assert.AreEqual(readKey24 * valueMult, readValue24);

                // We just locked this above, but for FlushMode.OnDisk it will still be PENDING.
                status = luContext.Read(readKey51, out var readValue51);
                if (flushMode == FlushMode.OnDisk)
                {
                    Assert.IsTrue(status.IsPending, status.ToString());
                    luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    Assert.True(completedOutputs.Next());
                    readValue51 = completedOutputs.Current.Output;
                    Assert.False(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
                else
                    Assert.IsFalse(status.IsPending, status.ToString());
                Assert.AreEqual(readKey51 * valueMult, readValue51);

                if (!initialDestWillBeLockTable)
                {
                    status = luContext.Read(resultKey, out var initialResultValue);
                    if (flushMode == FlushMode.OnDisk)
                    {
                        Assert.IsTrue(status.IsPending, status.ToString());
                        luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, initialResultValue) = GetSinglePendingResult(completedOutputs, out var recordMetadata);
                        Assert.IsTrue(status.Found, status.ToString());
                    }
                    else
                        Assert.IsFalse(status.IsPending, status.ToString());
                    Assert.AreEqual(resultKey * valueMult, initialResultValue);
                }

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.phase = phase;
                status = useRMW
                    ? luContext.RMW(resultKey, (readValue24 + readValue51) * valueMult2)
                    : luContext.Upsert(resultKey, (readValue24 + readValue51) * valueMult2);
                Assert.IsFalse(status.IsPending, status.ToString());

                status = luContext.Read(resultKey, out resultValue);
                Assert.IsFalse(status.IsPending, status.ToString());
                Assert.AreEqual(expectedResult, resultValue);

                luContext.Unlock(resultKey, LockType.Exclusive);
                blt.Decrement(GetBucketIndex(resultKey), LockType.Exclusive);
                luContext.Unlock(readKey51, LockType.Shared);
                blt.Decrement(GetBucketIndex(readKey51), LockType.Shared);
                luContext.Unlock(readKey24, LockType.Shared);
                blt.Decrement(GetBucketIndex(readKey24), LockType.Shared);

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }

            // Verify from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(expectedResult, resultValue);
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemoryDeleteTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                       [Values(FlushMode.NoFlush, FlushMode.ReadOnly)] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            // Phase.INTERMEDIATE is to test the non-Phase.REST blocks
            Populate();
            PrepareRecordLocation(flushMode);

            Dictionary<long, LockType> locks = new();
            BucketLockTracker blt = new();

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            long resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : 75;
            Status status;

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                // Lock destination value.
                luContext.Lock(resultKey, LockType.Exclusive);
                locks[resultKey] = LockType.Exclusive;
                blt.Increment(GetBucketIndex(resultKey), LockType.Exclusive);
                AssertIsLocked(luContext, resultKey, xlock: true, slock: false);

                AssertTotalLockCounts(1, 0);

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.phase = phase;
                status = luContext.Delete(ref resultKey);
                Assert.IsFalse(status.IsPending, status.ToString());

                // Reread the destination to verify
                status = luContext.Read(resultKey, out var _);
                Assert.IsFalse(status.Found, status.ToString());

                foreach (var key in locks.Keys.OrderBy(key => key))
                {
                    luContext.Unlock(key, locks[key]);
                    blt.Decrement(GetBucketIndex(key), locks[key]);
                }

                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out var _);
            Assert.IsFalse(status.Found, status.ToString());
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void StressManualLocks([Values(1, 8)] int numLockThreads, [Values(1, 8)] int numOpThreads)
        {
            Populate();

            // Lock in ordered sequence (avoiding deadlocks)
            const int baseKey = 42;
            const int numKeys = 20;
            const int numIncrement = 5;
            const int numIterations = 1000;

            void runManualLockThread(int tid)
            {
                Dictionary<int, LockType> locks = new();
                BucketLockTracker blt = new();

                Random rng = new(tid + 101);

                using var localSession = fht.For(new LockableUnsafeFunctions()).NewSession<LockableUnsafeFunctions>();
                var luContext = localSession.LockableUnsafeContext;
                luContext.BeginUnsafe();
                luContext.BeginLockable();

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    {
                        var lockType = rng.Next(100) < 60 ? LockType.Shared : LockType.Exclusive;
                        luContext.Lock(key, lockType);
                        locks[key] = lockType;
                        blt.Increment(GetBucketIndex(key), lockType);
                    }

                    foreach (var key in locks.Keys.OrderBy(key => key))
                    { 
                        luContext.Unlock(key, locks[key]);
                        blt.Increment(GetBucketIndex(key), locks[key]);
                    }
                    locks.Clear();
                }

                luContext.EndLockable();
                luContext.EndUnsafe();
            }

            void runLEphemeralLockOpThread(int tid)
            {
                Random rng = new(tid + 101);

                using var localSession = fht.For(new LockableUnsafeFunctions()).NewSession<LockableUnsafeFunctions>();
                var basicContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    {
                        var rand = rng.Next(100);
                        if (rand < 33)
                            basicContext.Read(key);
                        else if (rand < 66)
                            basicContext.Upsert(key, key * valueMult);
                        else
                            basicContext.RMW(key, key * valueMult);
                    }
                }
            }

            // Run a mix of luContext and normal ClientSession operations
            int numThreads = numLockThreads + numOpThreads;
            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t <= numLockThreads)
                    tasks[t] = Task.Factory.StartNew(() => runManualLockThread(tid));
                else
                    tasks[t] = Task.Factory.StartNew(() => runLEphemeralLockOpThread(tid));
            }
            Task.WaitAll(tasks);

            AssertTotalLockCounts(0, 0);
        }

        void AddLockTableEntry(LockableUnsafeContext<long, long, long, long, Empty, IFunctions<long, long, long, long, Empty>> luContext, long key)
        {
            luContext.Lock(key, LockType.Exclusive);

            HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
            GetBucket(ref hei);

            var lockState = fht.LockTable.GetLockState(ref key, ref hei);

            Assert.IsTrue(lockState.IsFound);
            Assert.IsTrue(lockState.IsLockedExclusive);
        }

        void VerifyAndUnlockSplicedInKey(LockableUnsafeContext<long, long, long, long, Empty, IFunctions<long, long, long, long, Empty>> luContext, long expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = ChainTests.SkipReadCacheChain(fht, expectedKey);
            var storedKey = fht.hlog.GetKey(pa);
            Assert.AreEqual(expectedKey, storedKey);

            luContext.Unlock(expectedKey, LockType.Exclusive);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyLocksAfterReadAndCTTTest()
        {
            Populate();
            fht.Log.FlushAndEvict(wait: true);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            long input = 0, output = 0, key = 24;
            ReadOptions readOptions = new() { ReadFlags = ReadFlags.CopyReadsToTail};
            BucketLockTracker blt = new();

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {
                AddLockTableEntry(luContext, key);
                blt.Increment(GetBucketIndex(key), LockType.Exclusive);

                var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
                Assert.IsTrue(status.IsPending, status.ToString());
                luContext.CompletePending(wait: true);

                VerifyAndUnlockSplicedInKey(luContext, key);
                blt.Decrement(GetBucketIndex(key), LockType.Exclusive);
                blt.AssertNoLocks();
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountsAfterFlushAndEvict()
        {
            Populate();

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            BucketLockTracker blt = new();
            long key = 24;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {
                luContext.Lock(ref key, LockType.Exclusive);
                blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                AssertTotalLockCounts(1, 0);

                fht.Log.FlushAndEvict(wait: true);
                AssertTotalLockCounts(1, 0);

                luContext.Unlock(ref key, LockType.Exclusive);
                blt.Decrement(GetBucketIndex(key), LockType.Exclusive);

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        void PopulateAndEvict(bool immutable = false)
        {
            Populate();

            if (immutable)
                fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
            else
                fht.Log.FlushAndEvict(true);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterUpsertToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            BucketLockTracker blt = new();
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = useExistingKey;
                    AddLockTableEntry(luContext, key);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    var status = luContext.Upsert(key, key * valueMult);
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }
                else
                {
                    key = useNewKey;
                    AddLockTableEntry(luContext, key);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    var status = luContext.Upsert(key, key * valueMult);
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }

                VerifyAndUnlockSplicedInKey(luContext, key);
                blt.Decrement(GetBucketIndex(key), LockType.Exclusive);
                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterRMWToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            BucketLockTracker blt = new();
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = useExistingKey;
                    AddLockTableEntry(luContext, key);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    var status = luContext.RMW(key, key * valueMult);
                    Assert.IsTrue(recordRegion == ChainTests.RecordRegion.OnDisk ? status.IsPending : status.Found);
                    luContext.CompletePending(wait: true);
                }
                else
                {
                    key = useNewKey;
                    AddLockTableEntry(luContext, key);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    var status = luContext.RMW(key, key * valueMult);
                    Assert.IsFalse(status.Found, status.ToString());
                }

                VerifyAndUnlockSplicedInKey(luContext, key);
                blt.Decrement(GetBucketIndex(key), LockType.Exclusive);
                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterDeleteToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            BucketLockTracker blt = new();
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = useExistingKey;
                    AddLockTableEntry(luContext, key);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    var status = luContext.Delete(key);

                    // Delete does not search outside mutable region so the key will not be found
                    Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
                else
                {
                    key = useNewKey;
                    AddLockTableEntry(luContext, key);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    var status = luContext.Delete(key);
                    Assert.IsFalse(status.Found, status.ToString());
                }

                VerifyAndUnlockSplicedInKey(luContext, key);
                blt.Decrement(GetBucketIndex(key), LockType.Exclusive);
                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void LockAndUnlockInLockTableOnlyTest()
        {
            // For this, just don't load anything, and it will happen in lock table.
            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            BucketLockTracker blt = new();

            Dictionary<long, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range(0, numRecords).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {
                // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                foreach (var key in locks.Keys.OrderBy(k => k))
                { 
                    luContext.Lock(key, locks[key]);
                    blt.Increment(GetBucketIndex(key), locks[key]);
                }

                AssertTotalLockCounts(numRecords / 2, numRecords / 2);

                foreach (var key in locks.Keys.OrderBy(k => -k))
                {
                    var localKey = key;     // can't ref the iteration variable
                    HashEntryInfo hei = new(fht.comparer.GetHashCode64(ref localKey));
                    GetBucket(ref hei);
                    var lockState = fht.LockTable.GetLockState(ref localKey, ref hei);
                    Assert.IsTrue(lockState.IsFound);
                    var lockType = locks[key];
                    Assert.AreEqual(lockType == LockType.Exclusive, lockState.IsLockedExclusive);
                    Assert.AreEqual(lockType != LockType.Exclusive, lockState.IsLockedShared);

                    luContext.Unlock(key, lockType);
                    blt.Decrement(GetBucketIndex(key), lockType);
                }

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterReadOnlyToUpdateRecordTest([Values] UpdateOp updateOp)
        {
            Populate();
            this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);

            const int key = 42;
            static int getValue(int key) => key + valueMult;
            BucketLockTracker blt = new();

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                luContext.Lock(key, LockType.Exclusive);
                blt.Increment(GetBucketIndex(key), LockType.Exclusive);

                var status = updateOp switch
                {
                    UpdateOp.Upsert => luContext.Upsert(key, getValue(key)),
                    UpdateOp.RMW => luContext.RMW(key, getValue(key)),
                    UpdateOp.Delete => luContext.Delete(key),
                    _ => new(StatusCode.Error)
                };
                Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                if (updateOp == UpdateOp.RMW)
                    Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
                else
                    Assert.IsTrue(status.Record.Created, status.ToString());

                OverflowBucketLockTableTests.AssertLockCounts(fht, key, true, 0);

                luContext.Unlock(key, LockType.Exclusive);
                blt.Decrement(GetBucketIndex(key), LockType.Exclusive);
                OverflowBucketLockTableTests.AssertLockCounts(fht, key, false, 0);

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        public void LockNewRecordThenUpdateAndUnlockTest([Values] UpdateOp updateOp)
        {
            const int numNewRecords = 100;

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;

            int getValue(int key) => key + valueMult;

            // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
            if (updateOp == UpdateOp.Delete)
            {
                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                    Assert.IsFalse(this.session.Upsert(key, key * valueMult).IsPending);
                fht.Log.FlushAndEvict(wait: true);
            }

            // Now populate the main area of the log.
            Populate();
            BucketLockTracker blt = new();

            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                // We don't sleep in this test
                comparer.maxSleepMs = 0;

                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                {
                    luContext.Lock(key, LockType.Exclusive);
                    blt.Increment(GetBucketIndex(key), LockType.Exclusive);
                    for (var iter = 0; iter < 2; ++iter)
                    {
                        OverflowBucketLockTableTests.AssertLockCounts(fht, key, true, 0);
                        updater(key, iter);
                    }
                    luContext.Unlock(key, LockType.Exclusive);
                    blt.Decrement(GetBucketIndex(key), LockType.Exclusive);

                    blt.AssertNoLocks();
                    AssertTotalLockCounts(0, 0);
                }
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }

            void updater(int key, int iter)
            {
                try
                {
                    Status status;
                    switch (updateOp)
                    {
                        case UpdateOp.Upsert:
                            status = luContext.Upsert(key, getValue(key));
                            if (iter == 0)
                                Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.RMW:
                            status = luContext.RMW(key, getValue(key));
                            if (iter == 0)
                                Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.Delete:
                            status = luContext.Delete(key);
                            Assert.IsTrue(status.NotFound, status.ToString());
                            if (iter == 0)
                                Assert.IsTrue(status.Record.Created, status.ToString());
                            break;
                        default:
                            Assert.Fail($"Unexpected updateOp {updateOp}");
                            return;
                    };
                    Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                }
                catch (Exception)
                {
                    ClearCountsOnError(session);
                    throw;
                }
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        //[Repeat(100)]
        public void LockNewRecordThenUnlockThenUpdateTest([Values] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            const int numNewRecords = 100;

            using var lockSession = fht.NewSession(new SimpleFunctions<long, long>());
            var lockLuContext = lockSession.LockableUnsafeContext;

            using var updateSession = fht.NewSession(new SimpleFunctions<long, long>());
            var basicContext = updateSession.BasicContext;

            int getValue(int key) => key + valueMult;

            // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
            if (updateOp == UpdateOp.Delete)
            {
                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                    Assert.IsFalse(this.session.Upsert(key, key * valueMult).IsPending);
                fht.Log.FlushAndEvict(wait: true);
            }

            // Now populate the main area of the log.
            Populate();

            lockLuContext.BeginUnsafe();
            lockLuContext.BeginLockable();

            // These are for debugging
            int[] lastLockerKeys = new int[6], lastUpdaterKeys = new int[3];

            // Randomize the start and lock-hold wait times
            int maxSleepMs = 10;
            Random lockRng = new(101), updateRng = new(107);

            try
            {
                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                {
                    for (var iter = 0; iter < 2; ++iter)
                    {
                        // Use Task instead of Thread because this propagates exceptions (such as Assert.* failures) back to this thread.
                        // BasicContext's ephemeral lock will wait for the lock/unlock combo to complete, or the lock/unlock will wait for basicContext to finish if it wins.
                        Task.WaitAll(Task.Run(() => locker(key)), Task.Run(() => updater(key, iter)));
                    }

                    AssertTotalLockCounts(0, 0);
                }
            }
            catch (Exception)
            {
                ClearCountsOnError(lockSession);
                throw;
            }
            finally
            {
                lockLuContext.EndLockable();
                lockLuContext.EndUnsafe();
            }

            void locker(int key)
            {
                try
                {
                    // Begin/EndLockable are called outside this function; we could not EndLockable in here as the lock lifetime is beyond that.
                    // (BeginLockable's scope is the session; BeginUnsafe's scope is the thread. The session is still "mono-threaded" here because
                    // only one thread at a time is making calls on it.)
                    lastLockerKeys[0] = key;
                    lockLuContext.BeginUnsafe();
                    lastLockerKeys[1] = key;
                    Thread.Sleep(lockRng.Next(maxSleepMs));
                    lastLockerKeys[2] = key;
                    lockLuContext.Lock(key, LockType.Exclusive);
                    lastLockerKeys[3] = key;
                    Thread.Sleep(lockRng.Next(maxSleepMs));
                    lastLockerKeys[4] = key;
                    lockLuContext.Unlock(key, LockType.Exclusive);
                    lastLockerKeys[5] = key;
                }
                catch (Exception)
                {
                    ClearCountsOnError(lockSession);
                    throw;
                }
                finally
                {
                    lockLuContext.EndUnsafe();
                }
            }

            void updater(int key, int iter)
            {
                try
                {
                    lastUpdaterKeys[0] = key;
                    Thread.Sleep(updateRng.Next(maxSleepMs));
                    lastUpdaterKeys[1] = key;
                    Status status;
                    switch (updateOp)
                    {
                        case UpdateOp.Upsert:
                            status = basicContext.Upsert(key, getValue(key));
                            if (iter == 0)
                                Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.RMW:
                            status = basicContext.RMW(key, getValue(key));
                            if (iter == 0)
                                Assert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                Assert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.Delete:
                            status = basicContext.Delete(key);
                            Assert.IsTrue(status.NotFound, status.ToString());
                            if (iter == 0)
                                Assert.IsTrue(status.Record.Created, status.ToString());
                            break;
                        default:
                            Assert.Fail($"Unexpected updateOp {updateOp}");
                            return;
                    };
                    Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                    lastUpdaterKeys[2] = key;
                }
                catch (Exception)
                {
                    ClearCountsOnError(lockSession);
                    throw;
                }
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void MultiSharedLockTest()
        {
            Populate();

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;
            BucketLockTracker blt = new();

            const int key = 42;
            var maxLocks = 63;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {

                for (var ii = 0; ii < maxLocks; ++ii)
                {
                    luContext.Lock(key, LockType.Shared);
                    blt.Increment(GetBucketIndex(key), LockType.Shared);
                    OverflowBucketLockTableTests.AssertLockCounts(fht, key, false, ii + 1);
                }

                for (var ii = 0; ii < maxLocks; ++ii)
                {
                    luContext.Unlock(key, LockType.Shared);
                    blt.Decrement(GetBucketIndex(key), LockType.Shared);
                    OverflowBucketLockTableTests.AssertLockCounts(fht, key, false, maxLocks - ii - 1);
                }

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(CheckpointRestoreCategory)]
        [Ignore("Should not hold LUC while calling sync checkpoint")]
        public async ValueTask CheckpointRecoverTest([Values] CheckpointType checkpointType, [Values] SyncMode syncMode)
        {
            Populate();

            Dictionary<int, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range(0, numRecords / 5).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            Guid fullCheckpointToken;
            bool success = true;
            {
                using var session = fht.NewSession(new SimpleFunctions<long, long>());
                var luContext = session.LockableUnsafeContext;

                try
                {
                    // We must retain this BeginLockable across the checkpoint, because we can't call EndLockable with locks held.
                    luContext.BeginUnsafe();
                    luContext.BeginLockable();

                    // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                    foreach (var key in locks.Keys.OrderBy(k => k))
                        luContext.Lock(key, locks[key]);
                }
                catch (Exception)
                {
                    ClearCountsOnError(session);
                    luContext.EndLockable();
                    throw;
                }
                finally
                {
                    luContext.EndUnsafe();
                }

                this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);

                if (syncMode == SyncMode.Sync)
                {
                    this.fht.TryInitiateFullCheckpoint(out fullCheckpointToken, checkpointType);
                    await this.fht.CompleteCheckpointAsync();
                }
                else
                    (success, fullCheckpointToken) = await fht.TakeFullCheckpointAsync(checkpointType);
                Assert.IsTrue(success);

                try
                {
                    luContext.BeginUnsafe();
                    foreach (var key in locks.Keys.OrderBy(k => -k))
                        luContext.Unlock(key, locks[key]);
                }
                catch (Exception)
                {
                    ClearCountsOnError(session);
                    throw;
                }
                finally
                {
                    luContext.EndLockable();
                    luContext.EndUnsafe();
                }
            }

            TearDown(forRecovery: true);
            Setup(forRecovery: true);

            if (syncMode == SyncMode.Sync)
                this.fht.Recover(fullCheckpointToken);
            else
                await this.fht.RecoverAsync(fullCheckpointToken);

            {
                var luContext = this.session.LockableUnsafeContext;
                luContext.BeginUnsafe();

                try
                {
                    foreach (var key in locks.Keys.OrderBy(k => k))
                    {
                        OverflowBucketLockTableTests.AssertLockCounts(fht, key, false, 0);
                    }
                }
                catch (Exception)
                {
                    ClearCountsOnError(session);
                    throw;
                }
                finally
                {
                    luContext.EndUnsafe();
                }
            }
        }
    }
}

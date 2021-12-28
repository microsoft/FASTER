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

namespace FASTER.test.LockableUnsafeContext
{
    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    class LockableUnsafeFunctions : SimpleFunctions<int, int>
    {
        internal long deletedRecordAddress;

        public override bool SupportsPostOperations => true;

        public override void PostSingleDeleter(ref int key, ref RecordInfo recordInfo, long address)
        {
            deletedRecordAddress = address;
        }

        public override bool ConcurrentDeleter(ref int key, ref int value, ref RecordInfo recordInfo, long address)
        {
            deletedRecordAddress = address;
            return true;
        }
    }

    public enum ResultLockTarget { MutableLock, LockTable }

    public enum ReadCopyDestination { Tail, ReadCache }

    public enum FlushMode { NoFlush, ReadOnly, OnDisk }

    public enum UpdateOp { Upsert, RMW }

    [TestFixture]
    class LockableUnsafeContextTests
    {
        const int numRecords = 1000;
        const int transferToNewKey = 1010;
        const int transferToExistingKey = 200;

        const int valueMult = 1_000_000;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, LockableUnsafeFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(Path.Combine(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);

            ReadCacheSettings readCacheSettings = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    break;
                }
            }

            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                         supportsLocking: true );
            session = fht.For(new LockableUnsafeFunctions()).NewSession<LockableUnsafeFunctions>();
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

            // Clean up log 
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
            {
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
            }
        }

        static void AssertIsLocked(LockableUnsafeContext<int, int, int, int, Empty, LockableUnsafeFunctions> luContext, int key, LockType lockType) 
            => AssertIsLocked(luContext, key, lockType == LockType.Exclusive, lockType == LockType.Shared);

        static void AssertIsLocked(LockableUnsafeContext<int, int, int, int, Empty, LockableUnsafeFunctions> luContext, int key, bool xlock, bool slock)
        {
            var (isX, isS) = luContext.IsLocked(key);
            Assert.AreEqual(xlock, isX, "xlock mismatch");
            Assert.AreEqual(slock, isS, "slock mismatch");
        }

        void PrepareRecordLocation(FlushMode recordLocation)
        {
            if (recordLocation == FlushMode.ReadOnly)
                this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);
            else if (recordLocation == FlushMode.OnDisk)
                this.fht.Log.FlushAndEvict(wait: true);
        }

        static void ClearCountsOnError(LockableUnsafeContext<int, int, int, int, Empty, LockableUnsafeFunctions> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        void EnsureNoLocks()
        {
            using var iter = this.fht.Log.Scan(this.fht.Log.BeginAddress, this.fht.Log.TailAddress);
            long count = 0;
            while (iter.GetNext(out var recordInfo, out var key, out var value))
            {
                ++count;
                Assert.False(recordInfo.IsLocked, $"Unexpected Locked record: {(recordInfo.IsLockedShared ? "S" : "")} {(recordInfo.IsLockedExclusive ? "X" : "")}");
            }

            // We delete some records so just make sure the test worked.
            Assert.Greater(count, numRecords - 10);
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                              [Values]FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            var useRMW = updateOp == UpdateOp.RMW;
            int resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : 75;
            int resultValue = -1;
            int expectedResult = (24 + 51) * valueMult;
            Status status;
            Dictionary<int, LockType> locks = new();

            using (var luContext = session.GetLockableUnsafeContext())
            {
                luContext.ResumeThread(out var epoch);

                try
                {
                    {   // key scope
                        // Get initial source values
                        int key = 24;
                        luContext.Lock(key, LockType.Shared);
                        AssertIsLocked(luContext, key, xlock: false, slock: true);
                        locks[key] = LockType.Shared;

                        key = 51;
                        luContext.Lock(key, LockType.Shared);
                        locks[key] = LockType.Shared;
                        AssertIsLocked(luContext, key, xlock: false, slock: true);

                        // Lock destination value.
                        luContext.Lock(resultKey, LockType.Exclusive);
                        locks[resultKey] = LockType.Exclusive;
                        AssertIsLocked(luContext, resultKey, xlock: true, slock: false);

                        // Re-get source values, to verify (e.g. they may be in readcache now).
                        // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
                        status = luContext.Read(24, out var value24);
                        if (flushMode == FlushMode.OnDisk)
                        {
                            if (status == Status.PENDING)
                            {
                                luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                                Assert.True(completedOutputs.Next());
                                value24 = completedOutputs.Current.Output;
                                Assert.False(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedExclusive);
                                Assert.True(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedShared);
                                Assert.False(completedOutputs.Next());
                                completedOutputs.Dispose();
                            }
                        }
                        else
                        {
                            Assert.AreNotEqual(Status.PENDING, status);
                        }

                        status = luContext.Read(51, out var value51);
                        if (flushMode == FlushMode.OnDisk)
                        {
                            if (status == Status.PENDING)
                            {
                                luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                                Assert.True(completedOutputs.Next());
                                value51 = completedOutputs.Current.Output;
                                Assert.False(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedExclusive);
                                Assert.True(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedShared);
                                Assert.False(completedOutputs.Next());
                                completedOutputs.Dispose();
                            }
                        }
                        else
                        {
                            Assert.AreNotEqual(Status.PENDING, status);
                        }

                        // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                        session.ctx.phase = phase;
                        int dummyInOut = 0;
                        status = useRMW
                            ? luContext.RMW(ref resultKey, ref expectedResult, ref dummyInOut, out RecordMetadata recordMetadata)
                            : luContext.Upsert(ref resultKey, ref dummyInOut, ref expectedResult, ref dummyInOut, out recordMetadata);
                        if (flushMode == FlushMode.OnDisk)
                        {
                            if (status == Status.PENDING)
                            {
                                luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                                Assert.True(completedOutputs.Next());
                                resultValue = completedOutputs.Current.Output;
                                Assert.True(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedExclusive);
                                Assert.False(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedShared);
                                Assert.False(completedOutputs.Next());
                                completedOutputs.Dispose();
                            }
                        }
                        else
                        {
                            Assert.AreNotEqual(Status.PENDING, status);
                        }

                        // Reread the destination to verify
                        status = luContext.Read(resultKey, out resultValue);
                        Assert.AreNotEqual(Status.PENDING, status);
                        Assert.AreEqual(expectedResult, resultValue);
                    }
                    foreach (var key in locks.Keys.OrderBy(key => -key))
                        luContext.Unlock(key, locks[key]);
                }
                catch (Exception)
                {
                    ClearCountsOnError(luContext);
                    throw;
                }
                finally
                {
                    luContext.SuspendThread();
                }
            }

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.AreNotEqual(Status.PENDING, status);
            Assert.AreEqual(expectedResult, resultValue);
            EnsureNoLocks();
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemoryLongLockTest([Values] ResultLockTarget resultLockTarget, [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            int resultKey = initialDestWillBeLockTable ? numRecords + 1 : 75;
            int resultValue;
            const int expectedResult = (24 + 51) * valueMult;
            var useRMW = updateOp == UpdateOp.RMW;
            Status status;

            using var luContext = session.GetLockableUnsafeContext();
            luContext.ResumeThread();

            try
            {
                luContext.Lock(51, LockType.Exclusive);

                status = luContext.Read(24, out var value24);
                if (flushMode == FlushMode.OnDisk)
                {
                    Assert.AreEqual(Status.PENDING, status);
                    luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, value24) = TestUtils.GetSinglePendingResult(completedOutputs);
                    Assert.AreEqual(Status.OK, status);
                    Assert.AreEqual(24 * valueMult, value24);
                }
                else
                    Assert.AreNotEqual(Status.PENDING, status);

                // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
                status = luContext.Read(51, out var value51);
                if (flushMode == FlushMode.OnDisk)
                {
                    if (status == Status.PENDING)
                    {
                        luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        Assert.True(completedOutputs.Next());
                        value51 = completedOutputs.Current.Output;
                        Assert.True(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedExclusive);
                        Assert.False(completedOutputs.Current.RecordMetadata.RecordInfo.IsLockedShared);
                        Assert.False(completedOutputs.Next());
                        completedOutputs.Dispose();
                    }
                }
                else
                {
                    Assert.AreNotEqual(Status.PENDING, status);
                }
                Assert.AreEqual(51 * valueMult, value51);

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.phase = phase;
                status = useRMW
                    ? luContext.RMW(resultKey, value24 + value51)
                    : luContext.Upsert(resultKey, value24 + value51);
                Assert.AreNotEqual(Status.PENDING, status);

                status = luContext.Read(resultKey, out resultValue);
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(expectedResult, resultValue);

                luContext.Unlock(51, LockType.Exclusive);
            }
            catch (Exception)
            {
                ClearCountsOnError(luContext);
                throw;
            }
            finally
            {
                luContext.SuspendThread();
            }

            // Verify from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.AreNotEqual(Status.PENDING, status);
            Assert.AreEqual(expectedResult, resultValue);
            EnsureNoLocks();
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemoryDeleteTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                       [Values(FlushMode.NoFlush, FlushMode.ReadOnly)] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            // Phase.INTERMEDIATE is to test the non-Phase.REST blocks
            Populate();
            PrepareRecordLocation(flushMode);

            Dictionary<int, LockType> locks = new();

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            int resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : 75;
            Status status;

            using (var luContext = session.GetLockableUnsafeContext())
            {
                luContext.ResumeThread(out var epoch);

                try
                {
                    // Lock destination value.
                    luContext.Lock(resultKey, LockType.Exclusive);
                    locks[resultKey] = LockType.Exclusive;
                    AssertIsLocked(luContext, resultKey, xlock: true, slock: false);

                    // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                    session.ctx.phase = phase;
                    status = luContext.Delete(ref resultKey);
                    Assert.AreNotEqual(Status.PENDING, status);

                    // Reread the destination to verify
                    status = luContext.Read(resultKey, out var _);
                    Assert.AreEqual(Status.NOTFOUND, status);

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        luContext.Unlock(key, locks[key]);
                }
                catch (Exception)
                {
                    ClearCountsOnError(luContext);
                    throw;
                }
                finally
                {
                    luContext.SuspendThread();
                }
            }

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out var _);
            Assert.AreEqual(Status.NOTFOUND, status);
            EnsureNoLocks();
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void StressLocks([Values(1, 8)] int numLockThreads, [Values(1, 8)] int numOpThreads)
        {
            Populate();

            // Lock in ordered sequence (avoiding deadlocks)
            const int baseKey = 42;
            const int numKeys = 20;
            const int numIncrement = 5;
            const int numIterations = 1000;

            void runLockThread(int tid)
            {
                Dictionary<int, LockType> locks = new();
                Random rng = new(tid + 101);

                using var localSession = fht.For(new LockableUnsafeFunctions()).NewSession<LockableUnsafeFunctions>();
                using var luContext = localSession.GetLockableUnsafeContext();
                luContext.ResumeThread();

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    {
                        var lockType = rng.Next(100) < 60 ? LockType.Shared : LockType.Exclusive;
                        luContext.Lock(key, lockType);
                        locks[key] = lockType;
                    }

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        luContext.Unlock(key, locks[key]);
                    locks.Clear();
                }

                luContext.SuspendThread();
            }

            void runOpThread(int tid)
            {
                Random rng = new(tid + 101);

                using var localSession = fht.For(new LockableUnsafeFunctions()).NewSession<LockableUnsafeFunctions>();

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    {
                        var rand = rng.Next(100);
                        if (rand < 33)
                            localSession.Read(key);
                        else if (rand < 66)
                            localSession.Upsert(key, key * valueMult);
                        else
                            localSession.RMW(key, key * valueMult);
                    }
                }
            }

            // Run a mix of luContext and normal ClientSession operations
            int numThreads = numLockThreads + numOpThreads;
            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                threads[t] = new Thread(() => { if (tid < numLockThreads) runLockThread(tid); else runOpThread(tid); });
            }
            for (int t = 0; t < numThreads; t++)
                threads[t].Start();
            for (int t = 0; t < numThreads; t++)
                threads[t].Join();

            EnsureNoLocks();
        }

        void AddLockTableEntry(LockableUnsafeContext<int, int, int, int, Empty, IFunctions<int, int, int, int, Empty>> luContext, int key, bool immutable)
        {
            luContext.Lock(key, LockType.Exclusive);
            var found = fht.LockTable.Get(key, out RecordInfo recordInfo);

            // Immutable locks in the ReadOnly region; it does NOT create a LockTable entry
            if (immutable)
            {
                Assert.IsFalse(found);
                return;
            }
            Assert.IsTrue(found);
            Assert.IsTrue(recordInfo.IsLockedExclusive);
        }

        void VerifySplicedInKey(LockableUnsafeContext<int, int, int, int, Empty, IFunctions<int, int, int, int, Empty>> luContext, int expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = ChainTests.SkipReadCacheChain(fht, expectedKey);
            var storedKey = fht.hlog.GetKey(pa);
            Assert.AreEqual(expectedKey, storedKey);

            // This is called after we've transferred from LockTable to log.
            Assert.False(fht.LockTable.Get(expectedKey, out _));

            // Verify we've transferred the expected locks.
            ref RecordInfo recordInfo = ref fht.hlog.GetInfo(pa);
            Assert.IsTrue(recordInfo.IsLockedExclusive);
            Assert.IsFalse(recordInfo.IsLockedShared);

            // Now unlock it; we're done.
            luContext.Unlock(expectedKey, LockType.Exclusive);
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void TransferFromLockTableToCTTTest()
        {
            Populate();
            fht.Log.FlushAndEvict(wait: true);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();
            int input = 0, output = 0, key = transferToExistingKey;
            RecordMetadata recordMetadata = default;
            AddLockTableEntry(luContext, key, immutable:false);

            var status = session.Read(ref key, ref input, ref output, ref recordMetadata, ReadFlags.CopyToTail);
            Assert.AreEqual(Status.PENDING, status);
            session.CompletePending(wait: true);

            VerifySplicedInKey(luContext, key);
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
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void TransferFromLockTableToUpsertTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();
            luContext.ResumeThread();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = transferToExistingKey;
                    AddLockTableEntry(luContext, key, recordRegion == ChainTests.RecordRegion.Immutable);
                    var status = luContext.Upsert(key, key * valueMult);
                    Assert.AreEqual(Status.OK, status);
                }
                else
                {
                    key = transferToNewKey;
                    AddLockTableEntry(luContext, key, immutable: false);
                    var status = luContext.Upsert(key, key * valueMult);
                    Assert.AreEqual(Status.OK, status);
                }
            }
            finally
            {
                luContext.SuspendThread();
            }
            VerifySplicedInKey(luContext, key);
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void TransferFromLockTableToRMWTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();
            luContext.ResumeThread();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = transferToExistingKey;
                    AddLockTableEntry(luContext, key, recordRegion == ChainTests.RecordRegion.Immutable);
                    var status = luContext.RMW(key, key * valueMult);
                    Assert.AreEqual(recordRegion == ChainTests.RecordRegion.OnDisk ? Status.PENDING : Status.OK, status);
                    luContext.CompletePending(wait: true);
                }
                else
                {
                    key = transferToNewKey;
                    AddLockTableEntry(luContext, key, immutable: false);
                    var status = luContext.RMW(key, key * valueMult);
                    Assert.AreEqual(Status.NOTFOUND, status);
                }
            }
            finally
            {
                luContext.SuspendThread();
            }

            VerifySplicedInKey(luContext, key);
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void TransferFromLockTableToDeleteTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();
            luContext.ResumeThread();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = transferToExistingKey;
                    AddLockTableEntry(luContext, key, recordRegion == ChainTests.RecordRegion.Immutable);
                    var status = luContext.Delete(key);
                    Assert.AreEqual(Status.OK, status);
                }
                else
                {
                    key = transferToNewKey;
                    AddLockTableEntry(luContext, key, immutable: false);
                    var status = luContext.Delete(key);
                    Assert.AreEqual(Status.OK, status);
                }
            }
            finally
            {
                luContext.SuspendThread();
            }

            VerifySplicedInKey(luContext, key);
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void LockAndUnlockInLockTableOnlyTest()
        {
            // For this, just don't load anything, and it will happen in lock table.
            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();

            Dictionary<int, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range( 0, numRecords).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
            foreach (var key in locks.Keys.OrderBy(k => k))
                luContext.Lock(key, locks[key]);

            Assert.IsTrue(fht.LockTable.IsActive);
            Assert.AreEqual(locks.Count, fht.LockTable.dict.Count);

            foreach (var key in locks.Keys)
            {
                var found = fht.LockTable.Get(key, out RecordInfo recordInfo);
                Assert.IsTrue(found);
                var lockType = locks[key];
                Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.IsLockedShared);

                luContext.Unlock(key, lockType);
                Assert.IsFalse(fht.LockTable.Get(key, out _));
            }

            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);
        }

        [Test]
        [Category(TestUtils.LockableUnsafeContextTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void EvictFromMainLogToLockTableTest()
        {
            Populate();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();

            Dictionary<int, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range(0, numRecords / 5).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
            foreach (var key in locks.Keys.OrderBy(k => k))
                luContext.Lock(key, locks[key]);

            // All locking should have been done in main log.
            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);

            // Now evict main log which should transfer records to the LockTable.
            fht.Log.FlushAndEvict(wait: true);

            Assert.IsTrue(fht.LockTable.IsActive);
            Assert.AreEqual(locks.Count, fht.LockTable.dict.Count);

            // Verify LockTable
            foreach (var key in locks.Keys)
            {
                var found = fht.LockTable.Get(key, out RecordInfo recordInfo);
                Assert.IsTrue(found);
                var lockType = locks[key];
                Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.IsLockedShared);

                // Just a little more testing of Read/CTT transferring from LockTable
                int input = 0, output = 0, localKey = key;
                RecordMetadata recordMetadata = default;
                var status = session.Read(ref localKey, ref input, ref output, ref recordMetadata, ReadFlags.CopyToTail);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(wait: true);

                Assert.IsFalse(fht.LockTable.Get(key, out _));
                var (isLockedExclusive, isLockedShared) = luContext.IsLocked(localKey);
                Assert.AreEqual(lockType == LockType.Exclusive, isLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, isLockedShared);

                luContext.Unlock(key, lockType);
                (isLockedExclusive, isLockedShared) = luContext.IsLocked(localKey);
                Assert.IsFalse(isLockedExclusive);
                Assert.IsFalse(isLockedShared);
            }

            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);
        }
    }
}

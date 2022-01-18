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

namespace FASTER.test.LockableUnsafeContext
{
    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    internal class LockableUnsafeFunctions : SimpleFunctions<int, int>
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

    internal class LockableUnsafeComparer : IFasterEqualityComparer<int>
    {
        internal int maxSleepMs;
        readonly Random rng = new(101);

        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        public long GetHashCode64(ref int k)
        {
            if (maxSleepMs > 0)
                Thread.Sleep(rng.Next(maxSleepMs));
            return Utility.GetHashCode(k);
        }
    }

    public enum ResultLockTarget { MutableLock, LockTable }

    public enum ReadCopyDestination { Tail, ReadCache }

    public enum FlushMode { NoFlush, ReadOnly, OnDisk }

    public enum UpdateOp { Upsert, RMW, Delete }

    [TestFixture]
    class LockableUnsafeContextTests
    {
        const int numRecords = 1000;
        const int transferToNewKey = 1010;
        const int transferToExistingKey = 200;

        const int valueMult = 1_000_000;

        LockableUnsafeFunctions functions;
        LockableUnsafeComparer comparer;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, LockableUnsafeFunctions> session;
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

            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                            checkpointSettings: checkpointSettings, comparer: comparer,
                                            supportsLocking: true);
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
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
        }

        static void AssertIsLocked(LockableUnsafeContext<int, int, int, int, Empty, LockableUnsafeFunctions> luContext, int key, bool xlock, bool slock)
        {
            var (isX, isS) = luContext.IsLocked(key);
            Assert.AreEqual(xlock, isX, "xlock mismatch");
            Assert.AreEqual(slock, isS > 0, "slock mismatch");
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
                Assert.False(recordInfo.IsLocked, $"Unexpected Locked record: {(recordInfo.NumLockedShared > 0 ? "S" : "")} {(recordInfo.IsLockedExclusive ? "X" : "")}");
            }

            // We delete some records so just make sure the test worked.
            Assert.Greater(count, numRecords - 10);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                              [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                              [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
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
                                Assert.Less(0, completedOutputs.Current.RecordMetadata.RecordInfo.NumLockedShared);
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
                                Assert.Less(0, completedOutputs.Current.RecordMetadata.RecordInfo.NumLockedShared);
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
                                Assert.AreEqual(0, completedOutputs.Current.RecordMetadata.RecordInfo.NumLockedShared);
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
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemoryLongLockTest([Values] ResultLockTarget resultLockTarget, [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                         [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
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
                    (status, value24) = GetSinglePendingResult(completedOutputs);
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
                        Assert.AreEqual(0, completedOutputs.Current.RecordMetadata.RecordInfo.NumLockedShared);
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
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
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
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
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
            Assert.AreEqual(0, recordInfo.NumLockedShared);

            // Now unlock it; we're done.
            luContext.Unlock(expectedKey, LockType.Exclusive);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TransferFromLockTableToCTTTest()
        {
            Populate();
            fht.Log.FlushAndEvict(wait: true);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();
            int input = 0, output = 0, key = transferToExistingKey;
            RecordMetadata recordMetadata = default;
            AddLockTableEntry(luContext, key, immutable: false);

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
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
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
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
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
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
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
                    Assert.AreEqual(Status.NOTFOUND, status);
                    luContext.Unlock(key, LockType.Exclusive);  // TODO Delete should do this
                }
            }
            finally
            {
                luContext.SuspendThread();
            }

            if (recordRegion != ChainTests.RecordRegion.NotFound)
                VerifySplicedInKey(luContext, key);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void LockAndUnlockInLockTableOnlyTest()
        {
            // For this, just don't load anything, and it will happen in lock table.
            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();

            Dictionary<int, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range(0, numRecords).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
            foreach (var key in locks.Keys.OrderBy(k => k))
                luContext.Lock(key, locks[key]);

            Assert.IsTrue(fht.LockTable.IsActive);
            Assert.AreEqual(locks.Count, fht.LockTable.dict.Count);

            foreach (var key in locks.Keys.OrderBy(k => -k))
            {
                var found = fht.LockTable.Get(key, out RecordInfo recordInfo);
                Assert.IsTrue(found);
                var lockType = locks[key];
                Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.NumLockedShared > 0);

                luContext.Unlock(key, lockType);
                Assert.IsFalse(fht.LockTable.Get(key, out _));
            }

            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TransferFromReadOnlyToUpdateRecordTest([Values] UpdateOp updateOp)
        {
            Populate();
            this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);

            using var luContext = session.GetLockableUnsafeContext();

            const int key = 42;
            luContext.Lock(key, LockType.Exclusive);

            int getValue(int key) => key + valueMult;

            luContext.ResumeThread();

            try
            {
                var status = updateOp switch
                {
                    UpdateOp.Upsert => luContext.Upsert(key, getValue(key)),
                    UpdateOp.RMW => luContext.RMW(key, getValue(key)),
                    UpdateOp.Delete => luContext.Delete(key),
                    _ => Status.ERROR
                };
                Assert.AreNotEqual(Status.ERROR, status, $"Unexpected UpdateOp {updateOp}");
                Assert.AreEqual(Status.OK, status);

                var (xlock, slock) = luContext.IsLocked(key);
                Assert.IsTrue(xlock);
                Assert.AreEqual(0, slock);
            }
            finally
            {
                luContext.SuspendThread();
            }

            luContext.Unlock(key, LockType.Exclusive);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void LockNewRecordCompeteWithUpdateTest([Values(LockOperationType.Lock, LockOperationType.Unlock)] LockOperationType lockOp, [Values] UpdateOp updateOp)
        {
            const int numNewRecords = 100;

            using var updateSession = fht.NewSession(new SimpleFunctions<int, int>());
            using var lockSession = fht.NewSession(new SimpleFunctions<int, int>());

            using var updateLuContext = updateSession.GetLockableUnsafeContext();
            using var lockLuContext = lockSession.GetLockableUnsafeContext();

            LockType getLockType(int key) => ((key & 1) == 0) ? LockType.Exclusive : LockType.Shared;
            int getValue(int key) => key + valueMult;

            // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
            if (updateOp == UpdateOp.Delete)
            {
                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                    Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
                fht.Log.FlushAndEvict(wait: true);
            }

            // Now populate the main area of the log.
            Populate();

            HashSet<int> locks = new();
            void lockKey(int key)
            {
                lockLuContext.Lock(key, getLockType(key));
                locks.Add(key);
            }
            void unlockKey(int key)
            {
                lockLuContext.Unlock(key, getLockType(key));
                locks.Remove(key);
            }

            // If we are testing unlocking, then we need to lock first.
            if (lockOp == LockOperationType.Unlock)
            {
                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                    lockKey(key);
            }

            // Sleep at varying durations for each call to comparer.GetHashCode, which is called at the start of Lock/Unlock and Upsert/RMW/Delete.
            comparer.maxSleepMs = 20;

            for (var key = numRecords; key < numRecords + numNewRecords; ++key)
            {
                // Use Task instead of Thread because this propagates exceptions (such as Assert.* failures) back to this thread.
                Task.WaitAll(Task.Run(() => locker(key)), Task.Run(() => updater(key)));
                var (xlock, slockCount) = lockLuContext.IsLocked(key);
                var expectedXlock = getLockType(key) == LockType.Exclusive && lockOp != LockOperationType.Unlock;
                var expectedSlock = getLockType(key) == LockType.Shared && lockOp != LockOperationType.Unlock;
                Assert.AreEqual(expectedXlock, xlock);
                Assert.AreEqual(expectedSlock, slockCount > 0);

                if (lockOp == LockOperationType.Lock)
                {
                    // There should be no entries in the locktable now; they should all be on the RecordInfo.
                    Assert.IsFalse(fht.LockTable.IsActive, $"count = {fht.LockTable.dict.Count}");
                }
                else
                {
                    // We are unlocking so should remove one record for each iteration.
                    Assert.AreEqual(numNewRecords + numRecords - key - 1, fht.LockTable.dict.Count);
                }
            }

            // Unlock all the keys we are expecting to unlock, which ensures all the locks were applied to RecordInfos as expected.
            foreach (var key in locks.ToArray())
                unlockKey(key);

            void locker(int key)
            {
                try
                {
                    lockLuContext.ResumeThread();
                    if (lockOp == LockOperationType.Lock)
                        lockKey(key);
                    else
                        unlockKey(key);
                }
                finally
                {
                    lockLuContext.SuspendThread();
                }
            }

            void updater(int key)
            {
                updateLuContext.ResumeThread();

                try
                {
                    // Use the LuContext here even though we're not doing locking, because we don't want the ephemeral locks to be tried for this test
                    // (the test will hang as we try to acquire the lock).
                    var status = updateOp switch
                    {
                        UpdateOp.Upsert => updateLuContext.Upsert(key, getValue(key)),
                        UpdateOp.RMW => updateLuContext.RMW(key, getValue(key)),
                        UpdateOp.Delete => updateLuContext.Delete(key),
                        _ => Status.ERROR
                    };
                    Assert.AreNotEqual(Status.ERROR, status, $"Unexpected UpdateOp {updateOp}");
                    if (updateOp == UpdateOp.RMW)
                        Assert.AreEqual(Status.NOTFOUND, status);
                    else
                        Assert.AreEqual(Status.OK, status);
                }
                finally
                {
                    updateLuContext.SuspendThread();
                }
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void MultiSharedLockTest()
        {
            Populate();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();

            const int key = 42;
            var maxLocks = 63;

            for (var ii = 0; ii < maxLocks; ++ii)
            {
                luContext.Lock(key, LockType.Shared);
                var (xlock, slockCount) = luContext.IsLocked(key);
                Assert.IsFalse(xlock);
                Assert.AreEqual(ii + 1, slockCount);
            }

            for (var ii = 0; ii < maxLocks; ++ii)
            {
                luContext.Unlock(key, LockType.Shared);
                var (xlock, slockCount) = luContext.IsLocked(key);
                Assert.IsFalse(xlock);
                Assert.AreEqual(maxLocks - ii - 1, slockCount);
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
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
            foreach (var key in locks.Keys.OrderBy(k => -k))
            {
                var found = fht.LockTable.Get(key, out RecordInfo recordInfo);
                Assert.IsTrue(found);
                var lockType = locks[key];
                Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.NumLockedShared > 0);

                // Just a little more testing of Read/CTT transferring from LockTable
                int input = 0, output = 0, localKey = key;
                RecordMetadata recordMetadata = default;
                var status = session.Read(ref localKey, ref input, ref output, ref recordMetadata, ReadFlags.CopyToTail);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(wait: true);

                Assert.IsFalse(fht.LockTable.Get(key, out _));
                var (isLockedExclusive, numLockedShared) = luContext.IsLocked(localKey);
                Assert.AreEqual(lockType == LockType.Exclusive, isLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, numLockedShared > 0);

                luContext.Unlock(key, lockType);
                (isLockedExclusive, numLockedShared) = luContext.IsLocked(localKey);
                Assert.IsFalse(isLockedExclusive);
                Assert.AreEqual(0, numLockedShared);
            }

            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(CheckpointRestoreCategory)]
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
                using var session = fht.NewSession(new SimpleFunctions<int, int>());
                using var luContext = session.GetLockableUnsafeContext();

                // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                foreach (var key in locks.Keys.OrderBy(k => k))
                    luContext.Lock(key, locks[key]);

                this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);

                if (syncMode == SyncMode.Sync)
                {
                    this.fht.TryInitiateFullCheckpoint(out fullCheckpointToken, checkpointType);
                    await this.fht.CompleteCheckpointAsync();
                }
                else
                    (success, fullCheckpointToken) = await fht.TakeFullCheckpointAsync(checkpointType);
                Assert.IsTrue(success);

                foreach (var key in locks.Keys.OrderBy(k => -k))
                    luContext.Unlock(key, locks[key]);
            }

            TearDown(forRecovery: true);
            Setup(forRecovery: true);

            if (syncMode == SyncMode.Sync)
                this.fht.Recover(fullCheckpointToken);
            else
                await this.fht.RecoverAsync(fullCheckpointToken);

            {
                using var luContext = this.session.GetLockableUnsafeContext();

                foreach (var key in locks.Keys.OrderBy(k => k))
                {
                    var (exclusive, numShared) = luContext.IsLocked(key);
                    Assert.IsFalse(exclusive, $"key: {key}");
                    Assert.AreEqual(0, numShared, $"key: {key}");
                }
            }
        }

        const int numSecondaryReaderKeys = 1500;
        const int checkpointFreq = 250;

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(CheckpointRestoreCategory)]
        async public Task SecondaryReaderTest([Values] SyncMode syncMode)
        {
            // This test is taken from the SecondaryReaderStore sample

            var path = MethodTestDir;
            DeleteDirectory(path, wait: true);

            var log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);

            var primaryStore = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            var secondaryStore = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            // Use Task instead of Thread because this propagates exceptions (such as Assert.* failures) back to this thread.
            await Task.WhenAll(Task.Run(() => PrimaryWriter(primaryStore, syncMode)),
                               Task.Run(() => SecondaryReader(secondaryStore, syncMode)));

            log.Dispose();
            DeleteDirectory(path, wait: true);
        }

        async static Task PrimaryWriter(FasterKV<long, long> primaryStore, SyncMode syncMode)
        {
            using var s1 = primaryStore.NewSession(new SimpleFunctions<long, long>());
            using var luc1 = s1.GetLockableUnsafeContext();

            // Upserting keys at primary starting from key 0
            for (long key = 0; key < numSecondaryReaderKeys; key++)
            {
                if (key > 0 && key % checkpointFreq == 0)
                {
                    // Checkpointing primary until key {key - 1}
                    if (syncMode == SyncMode.Sync)
                    {
                        primaryStore.TryInitiateHybridLogCheckpoint(out _, CheckpointType.Snapshot);
                        await primaryStore.CompleteCheckpointAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        var (success, _) = await primaryStore.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).ConfigureAwait(false);
                        Assert.IsTrue(success);
                    }
                    Thread.Sleep(10);
                }

                var status = s1.Upsert(ref key, ref key);
                Assert.AreEqual(Status.OK, status);
                luc1.Lock(key, LockType.Shared);
            }

            // Checkpointing primary until key {numSecondaryReaderOps - 1}
            await primaryStore.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).ConfigureAwait(false);

            // Unlock everything before we Dispose() luc1
            for (long key = 0; key < numSecondaryReaderKeys; key++)
            {
                luc1.Unlock(key, LockType.Shared);
            }
        }

        async static Task SecondaryReader(FasterKV<long, long> secondaryStore, SyncMode syncMode)
        {
            using var s1 = secondaryStore.NewSession(new SimpleFunctions<long, long>());
            using var luc1 = s1.GetLockableUnsafeContext();

            long key = 0, output = 0;
            while (true)
            {
                try
                {
                    // read-only recovery, no writing back undos
                    if (syncMode == SyncMode.Sync)
                        secondaryStore.Recover(undoNextVersion: false);
                    else
                        await secondaryStore.RecoverAsync(undoNextVersion: false).ConfigureAwait(false);
                }
                catch (FasterException)
                {
                    // Nothing to recover to at secondary, retrying
                    Thread.Sleep(500);
                    continue;
                }

                while (true)
                {
                    var status = s1.Read(ref key, ref output);
                    if (status == Status.NOTFOUND)
                    {
                        // Key {key} not found at secondary; performing recovery to catch up
                        Thread.Sleep(500);
                        break;
                    }
                    Assert.AreEqual(key, output);
                    var (xlock, slock) = luc1.IsLocked(key);
                    Assert.IsFalse(xlock);
                    Assert.AreEqual(0, slock);

                    key++;
                    if (key == numSecondaryReaderKeys)
                        return;
                }
            }
        }
    }
}

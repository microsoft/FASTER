// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    class ManualFunctions : SimpleFunctions<int, int>
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

    public enum ResultLockTarget { MutableLock, Stub }

    public enum ReadCopyDestination { Tail, ReadCache }

    public enum FlushMode { NoFlush, ReadOnly, OnDisk }

    public enum UpdateOp { Upsert, RMW }

    [TestFixture]
    class ManualOperationsTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;

        private FasterKV<int, int> fkv;
        private ClientSession<int, int, int, int, Empty, ManualFunctions> session;
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

            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                         fasterSettings: new FasterSettings { SupportsLocking = true });
            session = fkv.For(new ManualFunctions()).NewSession<ManualFunctions>();
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

        (bool xlock, bool slock) IsLocked(ManualFasterOperations<int, int, int, int, Empty, ManualFunctions> manualOps, int key, long logicalAddress, bool stub, out RecordInfo recordInfo)
        {
            // We have the epoch protected so can access the address directly. For ReadCache, which does not expose addresses, we must look up the key
            if (logicalAddress != Constants.kInvalidAddress)
            {
                var physicalAddress = fkv.hlog.GetPhysicalAddress(logicalAddress);
                recordInfo = fkv.hlog.GetInfo(physicalAddress);
                Assert.AreEqual(stub, recordInfo.Stub, "stub mismatch, valid Address");
            }
            else
            {
                int inoutDummy = default;
                RecordMetadata recordMetadata = default;
                var status = manualOps.Read(ref key, ref inoutDummy, ref inoutDummy, ref recordMetadata);
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(logicalAddress, recordMetadata.Address);    // Either kInvalidAddress for readCache, or the expected address

                recordInfo = recordMetadata.RecordInfo;
                Assert.AreEqual(stub, recordInfo.Stub, "stub mismatch");
            }
            return (recordInfo.IsLockedExclusive, recordInfo.IsLockedShared);
        }

        void AssertIsLocked(ManualFasterOperations<int, int, int, int, Empty, ManualFunctions> manualOps, int key, long logicalAddress, bool xlock, bool slock, bool stub)
        {
            var (isX, isS) = IsLocked(manualOps, key, logicalAddress, stub, out var recordInfo);
            Assert.AreEqual(xlock, isX, "xlock mismatch");
            Assert.AreEqual(slock, isS, "slock mismatch");
        }

        void PrepareRecordLocation(FlushMode recordLocation)
        {
            if (recordLocation == FlushMode.ReadOnly)
                this.fkv.Log.ShiftReadOnlyAddress(this.fkv.Log.TailAddress, wait: true);
            else if (recordLocation == FlushMode.OnDisk)
                this.fkv.Log.FlushAndEvict(wait: true);
        }

        static void ClearCountsOnError(ManualFasterOperations<int, int, int, int, Empty, ManualFunctions> manualOps)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            manualOps.sharedLockCount = 0;
            manualOps.exclusiveLockCount = 0;
        }

        void EnsureNoLocks()
        {
            using var iter = this.fkv.Log.Scan(this.fkv.Log.BeginAddress, this.fkv.Log.TailAddress);
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
        [Category(TestUtils.ManualOpsTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                              [Values]FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            Dictionary<int, LockInfo> locks = new();
            LockInfo lockInfo = default;

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            var useRMW = updateOp == UpdateOp.RMW;
            bool initialDestWillBeStub = resultLockTarget == ResultLockTarget.Stub || flushMode == FlushMode.OnDisk;
            int resultKey = resultLockTarget == ResultLockTarget.Stub ? numRecords + 1 : 75;
            int resultValue = -1;
            int expectedResult = (24 + 51) * valueMult;
            Status status;

            using (var manualOps = session.GetManualOperations())
            {
                manualOps.UnsafeResumeThread(out var epoch);

                try
                {
                    {   // key scope
                        // Get initial source values
                        int key = 24;
                        manualOps.Lock(key, LockType.Shared, retrieveData: true, ref lockInfo);
                        Assert.AreEqual(useReadCache, lockInfo.Address == Constants.kInvalidAddress);
                        locks[key] = lockInfo;
                        AssertIsLocked(manualOps, key, lockInfo.Address, xlock: false, slock: true, stub: false);
                        key = 51;
                        manualOps.Lock(key, LockType.Shared, retrieveData: true, ref lockInfo);
                        Assert.AreEqual(useReadCache, lockInfo.Address == Constants.kInvalidAddress);
                        locks[key] = lockInfo;
                        AssertIsLocked(manualOps, key, lockInfo.Address, xlock: false, slock: true, stub: false);

                        // Lock destination value (which may entail dropping a stub).
                        manualOps.Lock(resultKey, LockType.Exclusive, retrieveData: false, ref lockInfo);
                        Assert.AreEqual(useReadCache && !initialDestWillBeStub, lockInfo.Address == Constants.kInvalidAddress);
                        locks[resultKey] = lockInfo;
                        AssertIsLocked(manualOps, resultKey, lockInfo.Address, xlock: true, slock: false, stub: initialDestWillBeStub);

                        // Re-get source values, to verify (e.g. they may be in readcache now)
                        int value24 = -1, value51 = -1;
                        status = manualOps.Read(24, out value24);
                        Assert.AreNotEqual(Status.PENDING, status);
                        status = manualOps.Read(51, out value51);
                        Assert.AreNotEqual(Status.PENDING, status);

                        // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                        session.ctx.phase = phase;
                        int dummyInOut = 0;
                        RecordMetadata recordMetadata = default;
                        status = useRMW
                            ? manualOps.RMW(ref resultKey, ref expectedResult, ref dummyInOut, out recordMetadata)
                            : manualOps.Upsert(ref resultKey, ref dummyInOut, ref expectedResult, ref dummyInOut, out recordMetadata);
                        Assert.AreNotEqual(Status.PENDING, status);
                        if (initialDestWillBeStub || flushMode == FlushMode.ReadOnly)
                        {
                            // We initially created a stub for locking -or- we initially locked a RO record and then the update required RCU.
                            // Under these circumstances, we allocated a new record and transferred the lock to it.
                            Assert.AreNotEqual(locks[resultKey].Address, recordMetadata.Address);
                            AssertIsLocked(manualOps, resultKey, locks[resultKey].Address, xlock: false, slock: false, stub: initialDestWillBeStub);
                            AssertIsLocked(manualOps, resultKey, recordMetadata.Address, xlock: true, slock: false, stub: false);
                            lockInfo = locks[resultKey];
                            lockInfo.Address = recordMetadata.Address;
                            locks[resultKey] = lockInfo;
                        }
                        else
                            Assert.AreEqual(locks[resultKey].Address, recordMetadata.Address);

                        // Reread the destination to verify
                        status = manualOps.Read(resultKey, out resultValue);
                        Assert.AreNotEqual(Status.PENDING, status);
                        Assert.AreEqual(expectedResult, resultValue);
                    }
                    foreach (var key in locks.Keys.OrderBy(key => key))
                        manualOps.Unlock(key, locks[key].LockType);
                }
                catch (Exception)
                {
                    ClearCountsOnError(manualOps);
                    throw;
                }
                finally
                {
                    manualOps.UnsafeSuspendThread();
                }
            }

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.AreNotEqual(Status.PENDING, status);
            Assert.AreEqual(expectedResult, resultValue);
            EnsureNoLocks();
        }

        [Test]
        [Category(TestUtils.ManualOpsTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemoryLongLockTest([Values] ResultLockTarget resultLockTarget, [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase, [Values] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            LockInfo lockInfo = default;
            bool initialDestWillBeStub = resultLockTarget == ResultLockTarget.Stub || flushMode == FlushMode.OnDisk;
            int resultKey = initialDestWillBeStub ? numRecords + 1 : 75;
            int resultValue = -1;
            const int expectedResult = (24 + 51) * valueMult;
            var useRMW = updateOp == UpdateOp.RMW;
            Status status;

            using var manualOps = session.GetManualOperations();
            manualOps.UnsafeResumeThread();

            try
            {
                manualOps.Lock(51, LockType.Exclusive, retrieveData: true, ref lockInfo);

                status = manualOps.Read(24, out var value24);
                if (flushMode == FlushMode.OnDisk)
                {
                    Assert.AreEqual(Status.PENDING, status);
                    manualOps.UnsafeCompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, value24) = TestUtils.GetSinglePendingResult(completedOutputs);
                    Assert.AreEqual(Status.OK, status);
                    Assert.AreEqual(24 * valueMult, value24);
                }
                else
                    Assert.AreNotEqual(Status.PENDING, status);

                // We just locked this above, so it should not be PENDING
                status = manualOps.Read(51, out var value51);
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(51 * valueMult, value51);

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.phase = phase;
                status = useRMW
                    ? manualOps.RMW(resultKey, value24 + value51)
                    : manualOps.Upsert(resultKey, value24 + value51);
                Assert.AreNotEqual(Status.PENDING, status);

                status = manualOps.Read(resultKey, out resultValue);
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(expectedResult, resultValue);

                manualOps.Unlock(51, ref lockInfo);
            }
            catch (Exception)
            {
                ClearCountsOnError(manualOps);
                throw;
            }
            finally
            {
                manualOps.UnsafeSuspendThread();
            }

            // Verify from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.AreNotEqual(Status.PENDING, status);
            Assert.AreEqual(expectedResult, resultValue);
            EnsureNoLocks();
        }

        [Test]
        [Category(TestUtils.ManualOpsTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void InMemoryDeleteTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                       [Values(FlushMode.NoFlush, FlushMode.ReadOnly)] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            // Phase.INTERMEDIATE is to test the non-Phase.REST blocks
            Populate();
            PrepareRecordLocation(flushMode);

            Dictionary<int, LockInfo> locks = new();
            LockInfo lockInfo = default;

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            bool initialDestWillBeStub = resultLockTarget == ResultLockTarget.Stub || flushMode == FlushMode.OnDisk;
            int resultKey = resultLockTarget == ResultLockTarget.Stub ? numRecords + 1 : 75;
            Status status;

            using (var manualOps = session.GetManualOperations())
            {
                manualOps.UnsafeResumeThread(out var epoch);

                try
                {
                    // Lock destination value (which may entail dropping a stub).
                    manualOps.Lock(resultKey, LockType.Exclusive, retrieveData: false, ref lockInfo);
                    Assert.AreEqual(useReadCache && !initialDestWillBeStub, lockInfo.Address == Constants.kInvalidAddress);
                    locks[resultKey] = lockInfo;
                    AssertIsLocked(manualOps, resultKey, lockInfo.Address, xlock: true, slock: false, stub: initialDestWillBeStub);

                    // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                    session.ctx.phase = phase;
                    status = manualOps.Delete(ref resultKey);
                    Assert.AreNotEqual(Status.PENDING, status);

                    // If we initially created a stub for locking then we've updated it in place, unlike Upsert or RMW.
                    if (!initialDestWillBeStub && flushMode == FlushMode.ReadOnly)
                    {
                        // We initially locked a RO record and then the delete required inserting a new record.
                        // Under these circumstances, we allocated a new record and transferred the lock to it.
                        Assert.AreNotEqual(locks[resultKey].Address, session.functions.deletedRecordAddress);
                        AssertIsLocked(manualOps, resultKey, locks[resultKey].Address, xlock: false, slock: false, stub: initialDestWillBeStub);
                        AssertIsLocked(manualOps, resultKey, session.functions.deletedRecordAddress, xlock: true, slock: false, stub: false);
                        lockInfo = locks[resultKey];
                        lockInfo.Address = session.functions.deletedRecordAddress;
                        locks[resultKey] = lockInfo;
                    }
                    else
                        Assert.AreEqual(locks[resultKey].Address, session.functions.deletedRecordAddress);

                    // Reread the destination to verify
                    status = manualOps.Read(resultKey, out var _);
                    Assert.AreEqual(Status.NOTFOUND, status);

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        manualOps.Unlock(key, locks[key].LockType);
                }
                catch (Exception)
                {
                    ClearCountsOnError(manualOps);
                    throw;
                }
                finally
                {
                    manualOps.UnsafeSuspendThread();
                }
            }

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out var _);
            Assert.AreEqual(Status.NOTFOUND, status);
            EnsureNoLocks();
        }

        [Test]
        [Category(TestUtils.ManualOpsTestCategory)]
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
                Dictionary<int, LockInfo> locks = new();
                Random rng = new(tid + 101);

                using var localSession = fkv.For(new ManualFunctions()).NewSession<ManualFunctions>();
                using var manualOps = localSession.GetManualOperations();
                manualOps.UnsafeResumeThread();

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    {
                        var lockType = rng.Next(100) < 60 ? LockType.Shared : LockType.Exclusive;
                        LockInfo lockInfo = default;
                        manualOps.Lock(key, lockType, retrieveData: true, ref lockInfo);
                        locks[key] = lockInfo;
                    }

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        manualOps.Unlock(key, locks[key].LockType);
                    locks.Clear();
                }

                manualOps.UnsafeSuspendThread();
            }

            void runOpThread(int tid)
            {
                Random rng = new(tid + 101);

                using var localSession = fkv.For(new ManualFunctions()).NewSession<ManualFunctions>();

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

            // Run a mix of ManualOps and normal ClientSession operations
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
    }
}

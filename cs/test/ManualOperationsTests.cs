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

    public enum ResultLockTarget { MutableLock, LockTable }

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

        static void AssertIsLocked(ManualFasterOperations<int, int, int, int, Empty, ManualFunctions> manualOps, int key, LockType lockType) 
            => AssertIsLocked(manualOps, key, lockType == LockType.Exclusive, lockType == LockType.Shared);

        static void AssertIsLocked(ManualFasterOperations<int, int, int, int, Empty, ManualFunctions> manualOps, int key, bool xlock, bool slock)
        {
            var (isX, isS) = manualOps.IsLocked(key);
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

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            var useRMW = updateOp == UpdateOp.RMW;
            int resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : 75;
            int resultValue = -1;
            int expectedResult = (24 + 51) * valueMult;
            Status status;
            Dictionary<int, LockType> locks = new();

            using (var manualOps = session.GetManualOperations())
            {
                manualOps.UnsafeResumeThread(out var epoch);

                try
                {
                    {   // key scope
                        // Get initial source values
                        int key = 24;
                        manualOps.Lock(key, LockType.Shared);
                        AssertIsLocked(manualOps, key, xlock: false, slock: true);
                        locks[key] = LockType.Shared;

                        key = 51;
                        manualOps.Lock(key, LockType.Shared);
                        locks[key] = LockType.Shared;
                        AssertIsLocked(manualOps, key, xlock: false, slock: true);

                        // Lock destination value.
                        manualOps.Lock(resultKey, LockType.Exclusive);
                        locks[resultKey] = LockType.Exclusive;
                        AssertIsLocked(manualOps, resultKey, xlock: true, slock: false);

                        // Re-get source values, to verify (e.g. they may be in readcache now).
                        // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
                        status = manualOps.Read(24, out var value24);
                        if (flushMode == FlushMode.OnDisk)
                        {
                            if (status == Status.PENDING)
                            {
                                manualOps.UnsafeCompletePendingWithOutputs(out var completedOutputs, wait: true);
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

                        status = manualOps.Read(51, out var value51);
                        if (flushMode == FlushMode.OnDisk)
                        {
                            if (status == Status.PENDING)
                            {
                                manualOps.UnsafeCompletePendingWithOutputs(out var completedOutputs, wait: true);
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
                            ? manualOps.RMW(ref resultKey, ref expectedResult, ref dummyInOut, out RecordMetadata recordMetadata)
                            : manualOps.Upsert(ref resultKey, ref dummyInOut, ref expectedResult, ref dummyInOut, out recordMetadata);
                        if (flushMode == FlushMode.OnDisk)
                        {
                            if (status == Status.PENDING)
                            {
                                manualOps.UnsafeCompletePendingWithOutputs(out var completedOutputs, wait: true);
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
                        status = manualOps.Read(resultKey, out resultValue);
                        Assert.AreNotEqual(Status.PENDING, status);
                        Assert.AreEqual(expectedResult, resultValue);
                    }
                    foreach (var key in locks.Keys.OrderBy(key => -key))
                        manualOps.Unlock(key, locks[key]);
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

            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            int resultKey = initialDestWillBeLockTable ? numRecords + 1 : 75;
            int resultValue;
            const int expectedResult = (24 + 51) * valueMult;
            var useRMW = updateOp == UpdateOp.RMW;
            Status status;

            using var manualOps = session.GetManualOperations();
            manualOps.UnsafeResumeThread();

            try
            {
                manualOps.Lock(51, LockType.Exclusive);

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

                // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
                status = manualOps.Read(51, out var value51);
                if (flushMode == FlushMode.OnDisk)
                {
                    if (status == Status.PENDING)
                    {
                        manualOps.UnsafeCompletePendingWithOutputs(out var completedOutputs, wait: true);
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
                    ? manualOps.RMW(resultKey, value24 + value51)
                    : manualOps.Upsert(resultKey, value24 + value51);
                Assert.AreNotEqual(Status.PENDING, status);

                status = manualOps.Read(resultKey, out resultValue);
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(expectedResult, resultValue);

                manualOps.Unlock(51, LockType.Exclusive);
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

            Dictionary<int, LockType> locks = new();

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            int resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : 75;
            Status status;

            using (var manualOps = session.GetManualOperations())
            {
                manualOps.UnsafeResumeThread(out var epoch);

                try
                {
                    // Lock destination value.
                    manualOps.Lock(resultKey, LockType.Exclusive);
                    locks[resultKey] = LockType.Exclusive;
                    AssertIsLocked(manualOps, resultKey, xlock: true, slock: false);

                    // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                    session.ctx.phase = phase;
                    status = manualOps.Delete(ref resultKey);
                    Assert.AreNotEqual(Status.PENDING, status);

                    // Reread the destination to verify
                    status = manualOps.Read(resultKey, out var _);
                    Assert.AreEqual(Status.NOTFOUND, status);

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        manualOps.Unlock(key, locks[key]);
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
                Dictionary<int, LockType> locks = new();
                Random rng = new(tid + 101);

                using var localSession = fkv.For(new ManualFunctions()).NewSession<ManualFunctions>();
                using var manualOps = localSession.GetManualOperations();
                manualOps.UnsafeResumeThread();

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    {
                        var lockType = rng.Next(100) < 60 ? LockType.Shared : LockType.Exclusive;
                        manualOps.Lock(key, lockType);
                        locks[key] = lockType;
                    }

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        manualOps.Unlock(key, locks[key]);
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

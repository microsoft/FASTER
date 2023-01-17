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

namespace FASTER.test.LockableUnsafeContext
{
    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    internal class LockableUnsafeFunctions : SimpleFunctions<int, int>
    {
        internal long deletedRecordAddress;

        public override void PostSingleDeleter(ref int key, ref DeleteInfo deleteInfo)
        {
            deletedRecordAddress = deleteInfo.Address;
        }

        public override bool ConcurrentDeleter(ref int key, ref int value, ref DeleteInfo deleteInfo)
        {
            deletedRecordAddress = deleteInfo.Address;
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
                                            disableEphemeralLocking: false);
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

        static void ClearCountsOnError(ClientSession<int, int, int, int, Empty, LockableUnsafeFunctions> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        static void ClearCountsOnError(ClientSession<int, int, int, int, Empty, IFunctions<int, int, int, int, Empty>> luContext)
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

        void EnsureNoLocks()
        {
            using var iter = this.fht.Log.Scan(this.fht.Log.BeginAddress, this.fht.Log.TailAddress);
            long count = 0;
            while (iter.GetNext(out var recordInfo, out var key, out var value))
            {
                ++count;
                Assert.False(recordInfo.IsLocked, $"Unexpected Locked record for key {key}: {(recordInfo.IsLockedShared ? "S" : "")} {(recordInfo.IsLockedExclusive ? "X" : "")}");
            }

            // We delete some records so just make sure the test worked.
            Assert.Greater(count, numRecords - 10);
        }

        bool LockTableHasEntries() => LockTableTests.LockTableHasEntries(fht.ManualLockTable);
        int LockTableEntryCount() => LockTableTests.LockTableEntryCount(fht.ManualLockTable);

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task TestShiftHeadAddressLUC([Values] SyncMode syncMode)
        {
            int input = default;
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
                    var key1 = r.Next(RandRange);
                    luContext.Lock(key1, LockType.Exclusive);
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
                }

                r = new Random(RandSeed);
                sw.Restart();

                for (int c = 0; c < NumRecs; c++)
                {
                    var key1 = r.Next(RandRange);
                    var value = key1 + numRecords;
                    int output = 0;

                    luContext.Lock(key1, LockType.Shared);
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
                    if (!status.IsPending)
                    {
                        Assert.AreEqual(value, output);
                    }
                }
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
                List<int> lockKeys = new();

                for (int c = 0; c < NumRecs; c++)
                {
                    var key1 = r.Next(RandRange);
                    int output = 0;
                    luContext.Lock(key1, LockType.Shared);
                    lockKeys.Add(key1);
                    Status foundStatus = luContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                    Assert.IsTrue(foundStatus.IsPending);
                }

                CompletedOutputIterator<int, int, int, int, Empty> outputs;
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
                    luContext.Unlock(key, LockType.Shared);

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
        public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                              [Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                              [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            bool useReadCache = readCopyDestination == ReadCopyDestination.ReadCache && flushMode == FlushMode.OnDisk;
            var useRMW = updateOp == UpdateOp.RMW;
            const int readKey24 = 24, readKey51 = 51;
            int resultKey = resultLockTarget == ResultLockTarget.LockTable ? numRecords + 1 : readKey24 + readKey51;
            int resultValue = -1;
            int expectedResult = (readKey24 + readKey51) * valueMult;
            Status status;
            Dictionary<int, LockType> locks = new();

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

                    key = readKey51;
                    luContext.Lock(key, LockType.Shared);
                    locks[key] = LockType.Shared;
                    AssertIsLocked(luContext, key, xlock: false, slock: true);

                    // Lock destination value.
                    luContext.Lock(resultKey, LockType.Exclusive);
                    locks[resultKey] = LockType.Exclusive;
                    AssertIsLocked(luContext, resultKey, xlock: true, slock: false);

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
                    int dummyInOut = 0;
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

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
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
            const int readKey24 = 24, readKey51 = 51, valueMult2 = 10;
            int resultKey = initialDestWillBeLockTable ? numRecords + 1 : readKey24 + readKey51;
            int resultValue;
            int expectedResult = (readKey24 + readKey51) * valueMult * valueMult2;
            var useRMW = updateOp == UpdateOp.RMW;
            Status status;

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                luContext.Lock(readKey24, LockType.Shared);
                luContext.Lock(readKey51, LockType.Shared);
                luContext.Lock(resultKey, LockType.Exclusive);

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

                // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
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
                luContext.Unlock(readKey51, LockType.Shared);
                luContext.Unlock(readKey24, LockType.Shared);
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

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                // Lock destination value.
                luContext.Lock(resultKey, LockType.Exclusive);
                locks[resultKey] = LockType.Exclusive;
                AssertIsLocked(luContext, resultKey, xlock: true, slock: false);

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.phase = phase;
                status = luContext.Delete(ref resultKey);
                Assert.IsFalse(status.IsPending, status.ToString());

                // Reread the destination to verify
                status = luContext.Read(resultKey, out var _);
                Assert.IsFalse(status.Found, status.ToString());

                foreach (var key in locks.Keys.OrderBy(key => key))
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

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out var _);
            Assert.IsFalse(status.Found, status.ToString());
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
                    }

                    foreach (var key in locks.Keys.OrderBy(key => key))
                        luContext.Unlock(key, locks[key]);
                    locks.Clear();
                }

                luContext.EndLockable();
                luContext.EndUnsafe();
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
            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t <= numLockThreads)
                    tasks[t] = Task.Factory.StartNew(() => runLockThread(tid));
                else
                    tasks[t] = Task.Factory.StartNew(() => runOpThread(tid));
            }
            Task.WaitAll(tasks);

            EnsureNoLocks();
        }

        void AddLockTableEntry(LockableUnsafeContext<int, int, int, int, Empty, IFunctions<int, int, int, int, Empty>> luContext, int key, bool immutable)
        {
            luContext.Lock(key, LockType.Exclusive);
            var found = fht.ManualLockTable.TryGet(ref key, out RecordInfo recordInfo);

            // Immutable locks in the ReadOnly region; it does NOT create a LockTable entry
            if (immutable)
            {
                Assert.IsFalse(found);
                return;
            }
            Assert.IsTrue(found);
            Assert.IsTrue(recordInfo.IsLockedExclusive);
        }

        void VerifyAndUnlockSplicedInKey(LockableUnsafeContext<int, int, int, int, Empty, IFunctions<int, int, int, int, Empty>> luContext, int expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = ChainTests.SkipReadCacheChain(fht, expectedKey);
            var storedKey = fht.hlog.GetKey(pa);
            Assert.AreEqual(expectedKey, storedKey);

            // This is called after we've transferred from LockTable to log.
            Assert.False(fht.ManualLockTable.TryGet(ref expectedKey, out _));

            // Verify we've transferred the expected locks.
            ref RecordInfo recordInfo = ref fht.hlog.GetInfo(pa);
            Assert.IsTrue(recordInfo.IsLockedExclusive);
            Assert.IsFalse(recordInfo.IsLockedShared);

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
            var luContext = session.LockableUnsafeContext;
            int input = 0, output = 0, key = transferToExistingKey;
            ReadOptions readOptions = new() { ReadFlags = ReadFlags.CopyReadsToTail};

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {
                AddLockTableEntry(luContext, key, immutable: false);

                var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
                Assert.IsTrue(status.IsPending, status.ToString());
                luContext.CompletePending(wait: true);

                VerifyAndUnlockSplicedInKey(luContext, key);
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
        public void TransferFromEvictionToLockTable()
        {
            Populate();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;
            int key = transferToExistingKey;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {
                luContext.Lock(ref key, LockType.Exclusive);

                // Force the eviction which should transfer to lock table.
                fht.Log.FlushAndEvict(wait: true);

                // Verify the lock table entry.
                Assert.IsTrue(fht.ManualLockTable.IsActive, "Lock Table should be active");
                Assert.IsTrue(fht.ManualLockTable.ContainsKey(ref key, fht.Comparer.GetHashCode64(ref key)));

                luContext.Unlock(ref key, LockType.Exclusive);
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
        public void TransferFromLockTableToUpsertTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = transferToExistingKey;
                    AddLockTableEntry(luContext, key, recordRegion == ChainTests.RecordRegion.Immutable);
                    var status = luContext.Upsert(key, key * valueMult);
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }
                else
                {
                    key = transferToNewKey;
                    AddLockTableEntry(luContext, key, immutable: false);
                    var status = luContext.Upsert(key, key * valueMult);
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }

                VerifyAndUnlockSplicedInKey(luContext, key);
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
        public void TransferFromLockTableToRMWTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = transferToExistingKey;
                    AddLockTableEntry(luContext, key, recordRegion == ChainTests.RecordRegion.Immutable);
                    var status = luContext.RMW(key, key * valueMult);
                    Assert.IsTrue(recordRegion == ChainTests.RecordRegion.OnDisk ? status.IsPending : status.Found);
                    luContext.CompletePending(wait: true);
                }
                else
                {
                    key = transferToNewKey;
                    AddLockTableEntry(luContext, key, immutable: false);
                    var status = luContext.RMW(key, key * valueMult);
                    Assert.IsFalse(status.Found, status.ToString());
                }
                VerifyAndUnlockSplicedInKey(luContext, key);
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
        public void TransferFromLockTableToDeleteTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            int key = -1;
            try
            {
                if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
                {
                    key = transferToExistingKey;
                    AddLockTableEntry(luContext, key, recordRegion == ChainTests.RecordRegion.Immutable);
                    var status = luContext.Delete(key);

                    // Delete does not search outside mutable region so the key will not be found
                    Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                    VerifyAndUnlockSplicedInKey(luContext, key);
                }
                else
                {
                    key = transferToNewKey;
                    AddLockTableEntry(luContext, key, immutable: false);
                    var status = luContext.Delete(key);
                    Assert.IsFalse(status.Found, status.ToString());

                    // The mutable portion of this test does not transfer because the key is not found
                    luContext.Unlock(key, LockType.Exclusive);
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
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void LockAndUnlockInLockTableOnlyTest()
        {
            // For this, just don't load anything, and it will happen in lock table.
            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;

            Dictionary<int, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range(0, numRecords).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {

                // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                foreach (var key in locks.Keys.OrderBy(k => k))
                    luContext.Lock(key, locks[key]);

                Assert.IsTrue(LockTableHasEntries());
                Assert.AreEqual(locks.Count, LockTableEntryCount());

                foreach (var key in locks.Keys.OrderBy(k => -k))
                {
                    var localKey = key;     // can't ref the iteration variable
                    var found = fht.ManualLockTable.TryGet(ref localKey, out RecordInfo recordInfo);
                    Assert.IsTrue(found);
                    var lockType = locks[key];
                    Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                    Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.IsLockedShared);

                    luContext.Unlock(key, lockType);
                    Assert.IsFalse(fht.ManualLockTable.TryGet(ref localKey, out _));
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

            Assert.IsFalse(LockTableHasEntries());
            Assert.AreEqual(0, LockTableEntryCount());
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TransferFromReadOnlyToUpdateRecordTest([Values] UpdateOp updateOp)
        {
            Populate();
            this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);

            const int key = 42;
            static int getValue(int key) => key + valueMult;

            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                luContext.Lock(key, LockType.Exclusive);

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

                var (xlock, slock) = luContext.IsLocked(key);
                Assert.IsTrue(xlock);
                Assert.AreEqual(0, slock);

                luContext.Unlock(key, LockType.Exclusive);
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

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
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

            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                // We don't sleep in this test
                comparer.maxSleepMs = 0;

                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                {
                    luContext.Lock(key, LockType.Exclusive);
                    for (var iter = 0; iter < 2; ++iter)
                    {
                        var (xlock, slockCount) = luContext.IsLocked(key);
                        Assert.IsTrue(xlock, $"Expected xlock; iter {iter}, key {key}");
                        Assert.AreEqual(0, slockCount, $"Unexpected slock; iter {iter}, key {key}, count {slockCount}");
                        updater(key, iter);
                    }
                    luContext.Unlock(key, LockType.Exclusive);

                    // There should be no entries in the locktable now; they should all be on the RecordInfo.
                    Assert.IsFalse(LockTableHasEntries(), $"key {key}, count {LockTableEntryCount()}");
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

            using var lockSession = fht.NewSession(new SimpleFunctions<int, int>());
            var lockLuContext = lockSession.LockableUnsafeContext;

            using var updateSession = fht.NewSession(new SimpleFunctions<int, int>());
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

                    // There should be no entries in the locktable now; they should all be on the RecordInfo.
                    Assert.IsFalse(LockTableHasEntries(), $"key {key}, count {LockTableEntryCount()}");
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

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;

            const int key = 42;
            var maxLocks = 63;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            try
            {

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
        public void EvictFromMainLogToLockTableTest()
        {
            Populate();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            var luContext = session.LockableUnsafeContext;

            Dictionary<int, LockType> locks = new();
            var rng = new Random(101);
            foreach (var key in Enumerable.Range(0, numRecords / 5).Select(ii => rng.Next(numRecords)))
                locks[key] = (key & 1) == 0 ? LockType.Exclusive : LockType.Shared;

            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                foreach (var key in locks.Keys.OrderBy(k => k))
                    luContext.Lock(key, locks[key]);

                // All locking should have been done in main log.
                Assert.IsFalse(LockTableHasEntries());
                Assert.AreEqual(0, LockTableEntryCount());

                // Now evict main log which should transfer records to the LockTable.
                fht.Log.FlushAndEvict(wait: true);

                Assert.IsTrue(LockTableHasEntries());
                Assert.AreEqual(locks.Count, LockTableEntryCount());

                // Verify LockTable
                foreach (var key in locks.Keys.OrderBy(k => -k))
                {
                    int localKey = key;
                    var found = fht.ManualLockTable.TryGet(ref localKey, out RecordInfo recordInfo);
                    Assert.IsTrue(found);
                    var lockType = locks[key];
                    Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                    Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.IsLockedShared);

                    // Just a little more testing of Read/CTT transferring from LockTable
                    int input = 0, output = 0;
                    ReadOptions readOptions = new() { ReadFlags = ReadFlags.CopyReadsToTail};
                    var status = luContext.Read(ref localKey, ref input, ref output, ref readOptions, out _);
                    Assert.IsTrue(status.IsPending, status.ToString());
                    luContext.CompletePending(wait: true);

                    Assert.IsFalse(fht.ManualLockTable.TryGet(ref localKey, out _));
                    var (isLockedExclusive, numLockedShared) = luContext.IsLocked(localKey);
                    Assert.AreEqual(lockType == LockType.Exclusive, isLockedExclusive);
                    Assert.AreEqual(lockType != LockType.Exclusive, numLockedShared > 0);

                    luContext.Unlock(key, lockType);
                    (isLockedExclusive, numLockedShared) = luContext.IsLocked(localKey);
                    Assert.IsFalse(isLockedExclusive);
                    Assert.AreEqual(0, numLockedShared);
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

            Assert.IsFalse(LockTableHasEntries());
            Assert.AreEqual(0, LockTableEntryCount());
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
                using var session = fht.NewSession(new SimpleFunctions<int, int>());
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
                        var (exclusive, numShared) = luContext.IsLocked(key);
                        Assert.IsFalse(exclusive, $"key: {key}");
                        Assert.AreEqual(0, numShared, $"key: {key}");
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

        const int numSecondaryReaderKeys = 1500;
        const int checkpointFreq = 250;

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(CheckpointRestoreCategory)]
        [Ignore("Should not hold LUC while calling sync checkpoint")]
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
            var luc1 = s1.LockableUnsafeContext;

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
                Assert.IsTrue(status.Record.Created, status.ToString());

                try
                {
                    luc1.BeginUnsafe();
                    luc1.BeginLockable();
                    luc1.Lock(key, LockType.Shared);
                }
                catch (Exception)
                {
                    ClearCountsOnError(s1);
                    throw;
                }
                finally
                {
                    luc1.EndLockable();
                    luc1.EndUnsafe();
                }
            }

            // Checkpointing primary until key {numSecondaryReaderOps - 1}
            await primaryStore.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).ConfigureAwait(false);

            try
            {
                luc1.BeginUnsafe();
                luc1.BeginLockable();

                // Unlock everything before we Dispose() luc1
                for (long kk = 0; kk < numSecondaryReaderKeys; kk++)
                {
                    luc1.Unlock(kk, LockType.Shared);
                }
            }
            catch (Exception)
            {
                ClearCountsOnError(s1);
                throw;
            }
            finally
            {
                luc1.EndLockable();
                luc1.EndUnsafe();
            }
        }

        async static Task SecondaryReader(FasterKV<long, long> secondaryStore, SyncMode syncMode)
        {
            using var s1 = secondaryStore.NewSession(new SimpleFunctions<long, long>());
            var luc1 = s1.LockableUnsafeContext;

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

                luc1.BeginUnsafe();
                luc1.BeginLockable();
                try
                {
                    while (true)
                    {
                        var status = luc1.Read(ref key, ref output);
                        if (!status.Found)
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
                catch (Exception)
                {
                    ClearCountsOnError(s1);
                    throw;
                }
                finally
                {
                    luc1.EndLockable();
                    luc1.EndUnsafe();
                }
            }
        }
    }
}

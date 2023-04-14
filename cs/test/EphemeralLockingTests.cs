// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using System;
using System.IO;
using FASTER.core;
using NUnit.Framework;
using FASTER.test.ReadCacheTests;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test.EphemeralLocking
{
    // Functions for ephemeral locking--locking only for the duration of a concurrent IFunctions call.
    internal class EphemeralLockingTestFunctions : SimpleFunctions<long, long>
    {
        internal bool failInPlace;

        public override bool ConcurrentWriter(ref long key, ref long input, ref long src, ref long dst, ref long output, ref UpsertInfo upsertInfo)
            => !failInPlace && base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);

        public override bool InPlaceUpdater(ref long key, ref long input, ref long value, ref long output, ref RMWInfo rmwInfo)
            => !failInPlace && base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
    }

    [TestFixture]
    class EphemeralLockingTests
    {
        const int numRecords = 1000;
        const int useNewKey = 1010;
        const int useExistingKey = 200;

        const int valueMult = 1_000_000;

        EphemeralLockingTestFunctions functions;
        LongFasterEqualityComparer comparer;

        private FasterKV<long, long> fht;
        private ClientSession<long, long, long, long, Empty, EphemeralLockingTestFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup() => Setup(forRecovery: false);

        public void Setup(bool forRecovery)
        {
            if (!forRecovery)
                DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false, recoverDevice: forRecovery);

            ReadCacheSettings readCacheSettings = default;
            CheckpointSettings checkpointSettings = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    continue;
                }
                if (arg is CheckpointType chktType)
                {
                    checkpointSettings = new CheckpointSettings { CheckpointDir = MethodTestDir };
                    continue;
                }
            }

            comparer = new LongFasterEqualityComparer();
            functions = new EphemeralLockingTestFunctions();

            fht = new FasterKV<long, long>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                            checkpointSettings: checkpointSettings, comparer: comparer, lockingMode: LockingMode.Ephemeral);
            session = fht.For(functions).NewSession<EphemeralLockingTestFunctions>();
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
                DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
        }

        void AssertIsNotLocked(long key)
        {
            // Check *both* hlog and readcache
            OperationStackContext<long, long> stackCtx = new(comparer.GetHashCode64(ref key));
            fht.FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(fht.hlog);

            HashEntryInfo hei = new(fht.comparer.GetHashCode64(ref key));

            if (fht.UseReadCache && fht.FindInReadCache(ref key, ref stackCtx, minAddress: Constants.kInvalidAddress))
            {
                var recordInfo = fht.hlog.GetInfo(fht.hlog.GetPhysicalAddress(stackCtx.hei.AbsoluteAddress));
                Assert.IsFalse(recordInfo.IsLocked);
                fht.SkipReadCache(ref stackCtx, out _); // Ignore refresh
            }
            if (fht.TryFindRecordInMainLog(ref key, ref stackCtx, fht.hlog.BeginAddress))
            {
                var recordInfo = fht.hlog.GetInfo(fht.hlog.GetPhysicalAddress(hei.AbsoluteAddress));
                Assert.IsFalse(recordInfo.IsLocked);
            }
        }

        void PrepareRecordLocation(FlushMode recordLocation)
        {
            if (recordLocation == FlushMode.ReadOnly)
                this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);
            else if (recordLocation == FlushMode.OnDisk)
                this.fht.Log.FlushAndEvict(wait: true);
        }

        struct EnsureNoLock_ScanIteratorFunctions : IScanIteratorFunctions<long, long>
        {
            internal long count;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref long key, ref long value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress) 
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, nextAddress);

            public bool SingleReader(ref long key, ref long value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
            {
                ++count;
                Assert.False(recordMetadata.RecordInfo.IsLocked, $"Unexpected Locked record for key {key}: {(recordMetadata.RecordInfo.IsLockedShared ? "S" : "")} {(recordMetadata.RecordInfo.IsLockedExclusive ? "X" : "")}");
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        void AssertNoLocks()
        {
            EnsureNoLock_ScanIteratorFunctions scanFunctions = new();
            Assert.IsTrue(this.fht.Log.Scan(ref scanFunctions, this.fht.Log.BeginAddress, this.fht.Log.TailAddress), "Main log scan did not complete");

            // We delete some records so just make sure the test executed.
            Assert.Greater(scanFunctions.count, 0);

            if (this.fht.UseReadCache)
            {
                scanFunctions.count = 0;
                Assert.IsTrue(this.fht.ReadCache.Scan(ref scanFunctions, this.fht.readcache.BeginAddress, this.fht.readcache.GetTailAddress()), "Readcache scan did not complete");
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemorySimpleLockTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase,
                                           [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            var useRMW = updateOp == UpdateOp.RMW;
            const int readKey24 = 24, readKey51 = 51;
            long resultKey = readKey24 + readKey51;
            long resultValue = -1;
            long expectedResult = (readKey24 + readKey51) * valueMult;
            Status status;

            AssertNoLocks();

            // Re-get source values, to verify (e.g. they may be in readcache now).
            // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
            status = session.Read(readKey24, out var readValue24);
            if (flushMode == FlushMode.OnDisk)
            {
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
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
            AssertIsNotLocked(readKey24);
            Assert.AreEqual(24 * valueMult, readValue24);

            status = session.Read(readKey51, out var readValue51);
            if (flushMode == FlushMode.OnDisk)
            {
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
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
            AssertIsNotLocked(readKey51);
            Assert.AreEqual(51 * valueMult, readValue51);

            // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
            session.ctx.phase = phase;
            long dummyInOut = 0;
            status = useRMW
                ? session.RMW(ref resultKey, ref expectedResult, ref resultValue, out _)
                : session.Upsert(ref resultKey, ref dummyInOut, ref expectedResult, ref resultValue, out _);
            if (flushMode == FlushMode.OnDisk)
            {
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
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
                Assert.AreEqual(expectedResult, resultValue);
            }
            AssertIsNotLocked(resultKey);

            // Reread the destination to verify
            status = session.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(expectedResult, resultValue);

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out resultValue);
            Assert.IsFalse(status.IsPending, status.ToString());
            Assert.AreEqual(expectedResult, resultValue);
            AssertNoLocks();
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "readCopyDestination is used by Setup")]
        public void InMemoryDeleteTest([Values] ReadCopyDestination readCopyDestination,
                                       [Values(FlushMode.NoFlush, FlushMode.ReadOnly)] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            // Phase.INTERMEDIATE is to test the non-Phase.REST blocks
            Populate();
            PrepareRecordLocation(flushMode);

            // SetUp also reads this to determine whether to supply ReadCacheSettings. If ReadCache is specified it wins over CopyToTail.
            long resultKey = 75;
            Status status;

            AssertNoLocks();

            // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
            session.ctx.phase = phase;
            status = session.Delete(ref resultKey);
            Assert.IsFalse(status.IsPending, status.ToString());
            AssertIsNotLocked(resultKey);

            // Reread the destination to verify
            status = session.Read(resultKey, out var _);
            Assert.IsFalse(status.Found, status.ToString());

            AssertNoLocks();

            // Verify reading the destination from the full session.
            status = session.Read(resultKey, out var _);
            Assert.IsFalse(status.Found, status.ToString());
            AssertNoLocks();
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void StressEphemeralLocks([Values(2, 8)] int numThreads)
        {
            Populate();

            // Lock in ordered sequence (avoiding deadlocks)
            const int baseKey = 42;
            const int numKeys = 20;
            const int numIncrement = 5;
            const int numIterations = 1000;

            void runLEphemeralLockOpThread(int tid)
            {
                Random rng = new(tid + 101);

                using var localSession = fht.For(new EphemeralLockingTestFunctions()).NewSession<EphemeralLockingTestFunctions>();
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
            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() => runLEphemeralLockOpThread(tid));
            }
            Task.WaitAll(tasks);

            AssertNoLocks();
        }

        void VerifyKeyIsSplicedInAndHasNoLocks(long expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = ChainTests.SkipReadCacheChain(fht, expectedKey);
            var storedKey = fht.hlog.GetKey(pa);
            Assert.AreEqual(expectedKey, storedKey);

            // Verify we've no orphaned ephemeral lock.
            ref RecordInfo recordInfo = ref fht.hlog.GetInfo(pa);
            Assert.IsFalse(recordInfo.IsLocked);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void CopyToCTTTest()
        {
            Populate();
            fht.Log.FlushAndEvict(wait: true);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());
            long input = 0, output = 0, key = useExistingKey;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };

            var status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePending(wait: true);

            VerifyKeyIsSplicedInAndHasNoLocks(key);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountsAfterFlushAndEvict()
        {
            PopulateAndEvict(immutable: true);
            AssertNoLocks();
            fht.Log.FlushAndEvict(true);
            AssertNoLocks();
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
        public void VerifyNoLocksAfterToUpsertToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());

            int key = recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk
                ? useExistingKey : useNewKey;
            var status = session.Upsert(key, key * valueMult);
            Assert.IsTrue(status.Record.Created, status.ToString());

            VerifyKeyIsSplicedInAndHasNoLocks(key);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyNoLocksAfterRMWToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());

            int key = recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk
                ? useExistingKey : useNewKey;
            var status = session.RMW(key, key * valueMult);
            if (recordRegion == ChainTests.RecordRegion.OnDisk)
            {
                Assert.IsTrue(status.IsPending, status.ToString());
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                (status, _) = GetSinglePendingResult(completedOutputs);
                Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
            }
            else if (recordRegion == ChainTests.RecordRegion.Immutable)
                Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
            else
                Assert.IsTrue(status.Record.Created, status.ToString());

            VerifyKeyIsSplicedInAndHasNoLocks(key);
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyNoLocksAfterDeleteToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<long, long>());

            long key = -1;

            if (recordRegion == ChainTests.RecordRegion.Immutable || recordRegion == ChainTests.RecordRegion.OnDisk)
            {
                key = useExistingKey;
                var status = session.Delete(key);

                // Delete does not search outside mutable region so the key will not be found
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                VerifyKeyIsSplicedInAndHasNoLocks(key);
            }
            else
            {
                key = useNewKey;
                var status = session.Delete(key);
                Assert.IsFalse(status.Found, status.ToString());

                // This key was *not* inserted; Delete sees it does not exist so jumps out immediately.
                Assert.IsFalse(fht.FindHashBucketEntryForKey(ref key, out _));
            }
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifNoLocksAfterReadOnlyToUpdateRecordTest([Values] UpdateOp updateOp)
        {
            Populate();
            this.fht.Log.ShiftReadOnlyAddress(this.fht.Log.TailAddress, wait: true);

            const int key = 42;
            static int getValue(int key) => key + valueMult;

            var status = updateOp switch
            {
                UpdateOp.Upsert => session.Upsert(key, getValue(key)),
                UpdateOp.RMW => session.RMW(key, getValue(key)),
                UpdateOp.Delete => session.Delete(key),
                _ => new(StatusCode.Error)
            };
            Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
            if (updateOp == UpdateOp.RMW)
                Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
            else
                Assert.IsTrue(status.Record.Created, status.ToString());

            AssertNoLocks();
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void FailInPlaceAndSealTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            functions.failInPlace = true;

            const int key = 42;
            static int getValue(int key) => key + valueMult;

            var status = updateOp switch
            {
                UpdateOp.Upsert => session.Upsert(key, getValue(key)),
                UpdateOp.RMW => session.RMW(key, getValue(key)),
                _ => new(StatusCode.Error)
            };
            Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
            if (updateOp == UpdateOp.RMW)
                Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
            else
                Assert.IsTrue(status.Record.Created, status.ToString());

            long output;
            (status, output) = session.Read(key);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(getValue(key), output);

            AssertNoLocks();
        }
    }
}

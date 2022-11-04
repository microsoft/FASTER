// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Threading;
using FASTER.test.ReadCacheTests;
using static FASTER.test.TestUtils;
using System.Threading.Tasks;

namespace FASTER.test.LockTests
{
    [TestFixture]
    internal class AdvancedLockTests
    {
        const int numKeys = 1000;
        const int valueAdd = 1000000;
        const int mod = 100;

        public struct Input
        {
            internal LockFunctionFlags flags;
            internal int sleepRangeMs;
            public override string ToString() => $"{flags}, {sleepRangeMs}";
        }

        [Flags]
        internal enum LockFunctionFlags
        {
            None = 0,
            SetEvent = 1,
            WaitForEvent = 2,
            SleepAfterEventOperation = 4,
        }

        internal class Functions : FunctionsBase<int, int, Input, int, Empty>
        {
            internal readonly ManualResetEventSlim mres = new();
            readonly Random rng = new(101);

            internal Input readCacheInput;

            public override bool SingleWriter(ref int key, ref Input input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                // In the wait case we are waiting for a signal that something else has completed, e.g. a pending Read, by the thread with SetEvent.
                if ((input.flags & LockFunctionFlags.WaitForEvent) != 0)
                {
                    mres.Wait();
                    if ((input.flags & LockFunctionFlags.SleepAfterEventOperation) != 0)
                        Thread.Sleep(rng.Next(input.sleepRangeMs));
                }
                else if ((input.flags & LockFunctionFlags.SetEvent) != 0)
                {
                    mres.Set();
                    if ((input.flags & LockFunctionFlags.SleepAfterEventOperation) != 0)
                        Thread.Sleep(rng.Next(input.sleepRangeMs));
                }
                dst = src;
                return true;
            }

            public override bool SingleReader(ref int key, ref Input input, ref int value, ref int dst, ref ReadInfo readInfo)
            {
                // We should only be here if we are doing the initial read, before Upsert has taken place.
                Assert.AreEqual(key + valueAdd, value, $"Key = {key}");
                dst = value;
                return true;
            }

            public override bool ConcurrentReader(ref int key, ref Input input, ref int value, ref int dst, ref ReadInfo readInfo)
            {
                // We should only be here if the Upsert completed before the Read started; in this case we Read() the Upserted value.
                Assert.AreEqual(key + valueAdd * 2, value, $"Key = {key}");
                dst = value;
                return true;
            }
        }

        private FasterKV<int, int> fkv;
        private ClientSession<int, int, Input, int, Empty, Functions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/GenericStringTests.log", deleteOnClose: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 9 };
            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, ReadCacheSettings = readCacheSettings},
                comparer: new ChainTests.ChainComparer(mod), disableLocking: false);
            session = fkv.For(new Functions()).NewSession<Functions>();
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

        void Populate(bool evict = false)
        {
            using var session = fkv.NewSession(new Functions());

            for (int key = 0; key < numKeys; key++)
                session.Upsert(key, key + valueAdd);
            session.CompletePending(true);
            if (evict)
                fkv.Log.FlushAndEvict(wait: true);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(LockTestCategory)]
        //[Repeat(100)]
        public async ValueTask SameKeyInsertAndCTTTest()
        {
            Populate(evict: true);
            Functions functions = new();
            using var readSession = fkv.NewSession(functions);
            using var upsertSession = fkv.NewSession(functions);
            var iter = 0;

            await DoTwoThreadRandomKeyTest(numKeys,
                key =>
                {
                    int output = 0;
                    var sleepFlag = (iter % 5 == 0) ? LockFunctionFlags.None : LockFunctionFlags.SleepAfterEventOperation;
                    Input input = new() { flags = LockFunctionFlags.WaitForEvent | sleepFlag, sleepRangeMs = 10 };
                    var status = upsertSession.Upsert(key, input, key + valueAdd * 2, ref output);

                    // Don't test for .Found because we are doing random keys so may upsert one we have already seen, even on iter == 0
                    //Assert.IsTrue(status.Found, $"Key = {key}, status = {status}");
                },
                key =>
                {
                    var sleepFlag = (iter % 5 == 0) ? LockFunctionFlags.None : LockFunctionFlags.SleepAfterEventOperation;
                    functions.readCacheInput = new() { flags = LockFunctionFlags.SetEvent | sleepFlag, sleepRangeMs = 10 };
                    int output = 0;
                    ReadOptions readOptions = default;

                    // This will copy to ReadCache, and the test is trying to cause a race with the above Upsert.
                    var status = readSession.Read(ref key, ref functions.readCacheInput, ref output, ref readOptions, out _);

                    // If the Upsert completed before the Read started, we may Read() the Upserted value.
                    if (status.IsCompleted)
                    {
                        Assert.IsTrue(status.Found, $"Key = {key}, status {status}");
                        Assert.AreEqual(key + valueAdd * 2, output, $"Key = {key}");
                    }
                    else
                    {
                        Assert.IsTrue(status.IsPending, $"Key = {key}, status = {status}");
                        readSession.CompletePending(wait: true);

                        // Output is indeterminate as we will retry on Pending if the upserted value was inserted, so don't verify
                    }
                },
                key =>
                {
                    int output = default;
                    var status = readSession.Read(ref key, ref output);
                    Assert.IsTrue(status.Found, $"Key = {key}, status = {status}");
                    Assert.AreEqual(key + valueAdd * 2, output, $"Key = {key}");
                    functions.mres.Reset();
                    ++iter;
                }
            );
        }

        [TestFixture]
        class LockRecoveryTests
        {
            const int numKeys = 5000;

            string checkpointDir;

            private FasterKV<int, int> fht1;
            private FasterKV<int, int> fht2;
            private IDevice log;


            [SetUp]
            public void Setup()
            {
                DeleteDirectory(MethodTestDir, wait: true);
                checkpointDir = MethodTestDir + $"/checkpoints";
                log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);

                fht1 = new FasterKV<int, int>(128,
                    logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                    );

                fht2 = new FasterKV<int, int>(128,
                    logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                    );
            }

            [TearDown]
            public void TearDown()
            {
                fht1?.Dispose();
                fht1 = null;
                fht2?.Dispose();
                fht2 = null;
                log?.Dispose();
                log = null;

                DeleteDirectory(MethodTestDir);
            }

            [Test]
            [Category(FasterKVTestCategory), Category(CheckpointRestoreCategory), Category(LockTestCategory)]
            [Ignore("Should not hold LUC while calling sync checkpoint")]
            public async ValueTask NoLocksAfterRestoreTest([Values] CheckpointType checkpointType, [Values] SyncMode syncMode, [Values] bool incremental)
            {
                if (incremental && checkpointType != CheckpointType.Snapshot)
                    Assert.Ignore();
                const int lockKeyInterval = 10;

                static LockType getLockType(int key) => ((key / lockKeyInterval) & 0x1) == 0 ? LockType.Shared : LockType.Exclusive;
                static int getValue(int key) => key + numKeys * 10;
                Guid token;

                {   // Populate and Lock
                    using var session = fht1.NewSession(new SimpleFunctions<int, int>());
                    var luContext = session.LockableUnsafeContext;
                    var firstKeyEnd = incremental ? numKeys / 2 : numKeys;

                    luContext.BeginUnsafe();
                    for (int key = 0; key < firstKeyEnd; key++)
                    {
                        luContext.Upsert(key, getValue(key));
                        if ((key % lockKeyInterval) == 0)
                            luContext.Lock(key, getLockType(key));
                    }
                    luContext.EndUnsafe();

                    fht1.TryInitiateFullCheckpoint(out token, checkpointType);
                    await fht1.CompleteCheckpointAsync();

                    if (incremental)
                    {
                        luContext.BeginUnsafe();
                        for (int key = firstKeyEnd; key < numKeys; key++)
                        {
                            luContext.Upsert(key, getValue(key));
                            if ((key % lockKeyInterval) == 0)
                                luContext.Lock(key, getLockType(key));
                        }
                        luContext.EndUnsafe();

                        var _result1 = fht1.TryInitiateHybridLogCheckpoint(out var _token1, checkpointType, tryIncremental: true);
                        await fht1.CompleteCheckpointAsync();
                    }

                    luContext.BeginUnsafe();
                    for (int key = 0; key < numKeys; key += lockKeyInterval)
                    {
                        // This also verifies the locks are there--otherwise (in Debug) we'll AssertFail trying to unlock an unlocked record
                        luContext.Unlock(key, getLockType(key));
                    }
                    luContext.EndUnsafe();
                }

                if (syncMode == SyncMode.Async)
                    await fht2.RecoverAsync(token);
                else
                    fht2.Recover(token);

                {   // Ensure there are no locks
                    using var session = fht2.NewSession(new SimpleFunctions<int, int>());
                    var luContext = session.LockableUnsafeContext;
                    luContext.BeginUnsafe();
                    for (int key = 0; key < numKeys; key++)
                    {
                        (bool isExclusive, byte isShared) = luContext.IsLocked(key);
                        Assert.IsFalse(isExclusive);
                        Assert.AreEqual(0, isShared);
                    }
                    luContext.EndUnsafe();
                }
            }
        }
    }
}

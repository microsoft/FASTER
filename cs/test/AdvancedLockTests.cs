// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Threading;
using FASTER.test.LockTable;
using static FASTER.test.TestUtils;
using System.Threading.Tasks;
using System.Diagnostics;

#pragma warning disable IDE0060 // Remove unused parameter: used by Setup

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

        internal class ChainComparer : IFasterEqualityComparer<int>
        {
            readonly int mod;

            internal ChainComparer(int mod) => this.mod = mod;

            public bool Equals(ref int k1, ref int k2) => k1 == k2;

            public long GetHashCode64(ref int k) => k % mod;
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

            var lockingMode = LockingMode.None;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is LockingMode lm)
                {
                    lockingMode = lm;
                    continue;
                }
            }

            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, ReadCacheSettings = readCacheSettings },
                comparer: new ChainComparer(mod), lockingMode: lockingMode);
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
        public async ValueTask SameKeyInsertAndCTTTest([Values(LockingMode.None, LockingMode.Ephemeral /* Standard will hang */)] LockingMode lockingMode)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");
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
        }
    }
}

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

        [Flags]internal enum LockFunctionFlags
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
        private ClientSession<int, int, Input, int, Empty, Functions, DefaultStoreFunctions<int, int>> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericStringTests.log", deleteOnClose: true);
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

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
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
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.LockTestCategory)]
        public async ValueTask SameKeyInsertAndCTTTest()
        {
            Populate(evict: true);
            Functions functions = new();
            using var session = fkv.NewSession(functions);
            var iter = 0;

            await DoTwoThreadRandomKeyTest(numKeys,
                key =>
                {
                    int output = 0;
                    var sleepFlag = (iter % 5 == 0) ? LockFunctionFlags.None : LockFunctionFlags.SleepAfterEventOperation;
                    Input input = new() { flags = LockFunctionFlags.WaitForEvent | sleepFlag, sleepRangeMs = 10 };
                    var status = session.Upsert(key, input, key + valueAdd * 2, ref output);

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
                    var status = session.Read(ref key, ref functions.readCacheInput, ref output, ref readOptions, out _);

                    // If the Upsert completed before the Read started, we may Read() the Upserted value.
                    if (status.IsCompleted)
                    {
                        Assert.IsTrue(status.Found, $"Key = {key}, status {status}");
                        Assert.AreEqual(key + valueAdd * 2, output, $"Key = {key}");
                    }
                    else
                    {
                        Assert.IsTrue(status.IsPending, $"Key = {key}, status = {status}");
                        session.CompletePending(wait: true);
                        // Output is not clear here and we are testing only threading aspects, so don't verify
                    }
                },
                key =>
                {
                    int output = default;
                    var status = session.Read(ref key, ref output);
                    Assert.IsTrue(status.Found, $"Key = {key}, status = {status}");
                    Assert.AreEqual(key + valueAdd * 2, output, $"Key = {key}");
                    functions.mres.Reset();
                    ++iter;
                }
            );
        }
   }
}
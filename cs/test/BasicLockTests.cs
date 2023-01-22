// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test.LockTests
{
    [TestFixture]
    internal class BasicLockTests
    {
        internal class Functions : SimpleFunctions<int, int>
        {
            internal bool throwOnInitialUpdater;
            internal long initialUpdaterThrowAddress;

            static bool Increment(ref int dst)
            {
                ++dst;
                return true;
            }

            public override bool ConcurrentWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo) => Increment(ref dst);

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo) => Increment(ref value);

            public override bool InitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = rmwInfo.Address;
                    throw new FasterException(nameof(throwOnInitialUpdater));
                }
                return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            public override bool SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = upsertInfo.Address;
                    throw new FasterException(nameof(throwOnInitialUpdater));
                }
                return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            }

            public override bool SingleDeleter(ref int key, ref int value, ref DeleteInfo deleteInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = deleteInfo.Address;
                    throw new FasterException(nameof(throwOnInitialUpdater));
                }
                return base.SingleDeleter(ref key, ref value, ref deleteInfo);
            }
        }

        internal class LocalComparer : IFasterEqualityComparer<int>
        {
            internal int mod = numRecords;

            public bool Equals(ref int k1, ref int k2) => k1 == k2;

            public long GetHashCode64(ref int k) => Utility.GetHashCode(k % mod);
        }

        private FasterKV<int, int> fkv;
        private ClientSession<int, int, int, int, Empty, Functions> session;
        private IDevice log;

        const int numRecords = 100;
        const int valueMult = 1000000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericStringTests.log", deleteOnClose: true);
            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null }, comparer: new LocalComparer() );
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

        [Test]
        [Category("FasterKV")]
        public unsafe void RecordInfoLockTest([Values(1, 50)] int numThreads)
        {
            for (var ii = 0; ii < 5; ++ii)
            {
                RecordInfo recordInfo = new() { Valid = true };
                RecordInfo* ri = &recordInfo;

#pragma warning disable IDE0200 // The lambdas cannot be simplified as it causes struct temporaries
                XLockTest(numThreads, () => ri->TryLockExclusive(), () => { ri->UnlockExclusive(); return true; });
                SLockTest(numThreads, () => ri->TryLockShared(), () => ri->UnlockShared());
                XSLockTest(numThreads, () => ri->TryLockExclusive(), () => ri->UnlockExclusive(), () => ri->TryLockShared(), () => ri->UnlockShared());
#pragma warning restore IDE0200
            }
        }

        private void XLockTest(int numThreads, Func<bool> locker, Func<bool> unlocker)
        {
            long lockTestValue = 0;
            const int numIters = 1000;
            SpinWait sw = new();

            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(XLockTestFunc)).ToArray();
            Task.WaitAll(tasks);

            Assert.AreEqual(numThreads * numIters, lockTestValue);

            void XLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    while (!locker())
                        sw.SpinOnce(-1);
                    var temp = lockTestValue;
                    Thread.Yield();
                    lockTestValue = temp + 1;
                    Assert.IsTrue(unlocker());
                }
            }
        }

        private void SLockTest(int numThreads, Func<bool> locker, Action unlocker)
        {
            long lockTestValue = 1;
            long lockTestValueResult = 0;
            SpinWait sw = new();

            const int numIters = 1000;

            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(SLockTestFunc)).ToArray();
            Task.WaitAll(tasks);

            Assert.AreEqual(numThreads * numIters, lockTestValueResult);

            void SLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    while (!locker())
                        sw.SpinOnce(-1);
                    Interlocked.Add(ref lockTestValueResult, Interlocked.Read(ref lockTestValue));
                    Thread.Yield();
                    unlocker();
                }
            }
        }

        private void XSLockTest(int numThreads, Func<bool> xlocker, Action xunlocker, Func<bool> slocker, Action sunlocker)
        {
            long lockTestValue = 0;
            long lockTestValueResult = 0;
            SpinWait sw = new();

            const int numIters = 1000;

            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(XLockTestFunc))
                .Concat(Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(SLockTestFunc))).ToArray();
            Task.WaitAll(tasks);

            Assert.AreEqual(numThreads * numIters, lockTestValue);
            Assert.AreEqual(numThreads * numIters, lockTestValueResult);

            void XLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    while (!xlocker())
                        sw.SpinOnce(-1);
                    var temp = lockTestValue;
                    Thread.Yield();
                    lockTestValue = temp + 1;
                    xunlocker();
                }
            }

            void SLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    while (!slocker())
                        sw.SpinOnce(-1);
                    Interlocked.Add(ref lockTestValueResult, 1);
                    Thread.Yield();
                    sunlocker();
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public void FunctionsLockTest([Values(1, 20)] int numThreads)
        {
            // Populate
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
            }

            // Update
            const int numIters = 500;
            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(() => UpdateFunc((ii & 1) == 0, numRecords, numIters))).ToArray();
            Task.WaitAll(tasks);

            // Verify
            for (int key = 0; key < numRecords; key++)
            {
                var expectedValue = key * valueMult + numThreads * numIters;
                Assert.IsFalse(session.Read(key, out int value).IsPending);
                Assert.AreEqual(expectedValue, value);
            }
        }

        void UpdateFunc(bool useRMW, int numRecords, int numIters)
        {
            for (var key = 0; key < numRecords; ++key)
            {
                for (int iter = 0; iter < numIters; iter++)
                {
                    if ((iter & 7) == 7)
                        Assert.IsFalse(session.Read(key).status.IsPending);

                    // These will both just increment the stored value, ignoring the input argument.
                    if (useRMW)
                        session.RMW(key, default);
                    else
                        session.Upsert(key, default);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public unsafe void SealDeletedRecordTest([Values(UpdateOp.RMW, UpdateOp.Upsert)] UpdateOp updateOp, [Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode)
        {
            // Populate
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
            }

            // Insert a colliding key so we don't elide the deleted key from the hash chain.
            int deleteKey = numRecords / 2;
            int collidingKey = deleteKey + numRecords;
            Assert.IsFalse(session.Upsert(collidingKey, collidingKey * valueMult).IsPending);

            // Now make sure we did collide
            HashEntryInfo hei = new(fkv.comparer.GetHashCode64(ref deleteKey));
            Assert.IsTrue(fkv.FindTag(ref hei), "Cannot find deleteKey entry");
            Assert.Greater(hei.Address, Constants.kInvalidAddress, "Couldn't find deleteKey Address");
            long physicalAddress = this.fkv.hlog.GetPhysicalAddress(hei.Address);
            ref var recordInfo = ref this.fkv.hlog.GetInfo(physicalAddress);
            ref var lookupKey = ref this.fkv.hlog.GetKey(physicalAddress);
            Assert.AreEqual(collidingKey, lookupKey, "Expected collidingKey");

            // Backtrace to deleteKey
            physicalAddress = fkv.hlog.GetPhysicalAddress(recordInfo.PreviousAddress);
            recordInfo = ref this.fkv.hlog.GetInfo(physicalAddress);
            lookupKey = ref this.fkv.hlog.GetKey(physicalAddress);
            Assert.AreEqual(deleteKey, lookupKey, "Expected deleteKey");
            Assert.IsFalse(recordInfo.Tombstone, "Tombstone should be false");

            // In-place delete.
            Assert.IsFalse(session.Delete(deleteKey).IsPending);
            Assert.IsTrue(recordInfo.Tombstone, "Tombstone should be true after Delete");

            if (flushMode == FlushMode.ReadOnly)
                this.fkv.hlog.ShiftReadOnlyAddress(fkv.Log.TailAddress);

            var status = updateOp switch
            {
                UpdateOp.RMW => session.RMW(deleteKey, default),
                UpdateOp.Upsert => session.Upsert(deleteKey, default),
                UpdateOp.Delete => throw new InvalidOperationException("UpdateOp.Delete not expected in this test"),
                _ => throw new InvalidOperationException($"Unknown updateOp {updateOp}")
            };
            Assert.IsFalse(status.IsPending);

            Assert.IsTrue(recordInfo.Tombstone, "Tombstone should be true after Update");
            Assert.IsTrue(recordInfo.Sealed, "Sealed should be true after Update");
        }

        [Test]
        [Category("FasterKV")]
        public unsafe void SetInvalidOnException([Values] UpdateOp updateOp)
        {
            // Don't modulo the hash codes.
            (fkv.comparer as LocalComparer).mod = int.MaxValue;

            // Populate
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
            }

            long expectedThrowAddress = fkv.Log.TailAddress;
            session.functions.throwOnInitialUpdater = true;

            // Delete must try with an existing key; Upsert and Delete should insert a new key
            int deleteKey = numRecords / 2;
            var insertKey = numRecords + 1;

            // Make sure everything will create a new record.
            this.fkv.Log.FlushAndEvict(wait: true);

            var threw = false;
            try
            {
                var status = updateOp switch
                {
                    UpdateOp.RMW => session.RMW(insertKey, default),
                    UpdateOp.Upsert => session.Upsert(insertKey, default),
                    UpdateOp.Delete => session.Delete(deleteKey),
                    _ => throw new InvalidOperationException($"Unknown updateOp {updateOp}")
                };
                Assert.IsFalse(status.IsPending);
            }
            catch (FasterException ex)
            {
                Assert.AreEqual(nameof(session.functions.throwOnInitialUpdater), ex.Message);
                threw = true;
            }

            Assert.IsTrue(threw, "Test should have thrown");
            Assert.AreEqual(expectedThrowAddress, session.functions.initialUpdaterThrowAddress, "Unexpected throw address");

            long physicalAddress = this.fkv.hlog.GetPhysicalAddress(expectedThrowAddress);
            ref var recordInfo = ref this.fkv.hlog.GetInfo(physicalAddress);
            Assert.IsTrue(recordInfo.Invalid, "Expected Invalid record");
        }
    }
}
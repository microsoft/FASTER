// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.test
{
    [TestFixture]
    internal class LockTests
    {
        internal class Functions : AdvancedSimpleFunctions<int, int>
        {
            private readonly RecordAccessor<int, int> recordAccessor;

            internal Functions(RecordAccessor<int, int> accessor) => this.recordAccessor = accessor;

            public override void ConcurrentReader(ref int key, ref int input, ref int value, ref int dst, long address)
            {
                this.recordAccessor.SpinLock(address);
                dst = value;
                this.recordAccessor.Unlock(address);
            }

            bool LockAndIncrement(ref int dst, long address)
            {
                this.recordAccessor.SpinLock(address);
                ++dst;
                this.recordAccessor.Unlock(address);
                return true;
            }

            public override bool ConcurrentWriter(ref int key, ref int src, ref int dst, long address) => LockAndIncrement(ref dst, address);

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, long address) => LockAndIncrement(ref value, address);
        }

        private FasterKV<int, int> fkv;
        private AdvancedClientSession<int, int, int, int, Empty, Functions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/GenericStringTests.log", deleteOnClose: true);
            fkv = new FasterKV<int, int>( 1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null } );
            session = fkv.For(new Functions(fkv.RecordAccessor)).NewSession<Functions>();
        }

        [TearDown]
        public void TearDown()
        {
            session.Dispose();
            session = null;
            fkv.Dispose();
            fkv = null;
            log.Dispose();
            log = null;
        }

        public unsafe void RecordInfoLockTest()
        {
            // Re-entrancy check
            static void checkLatch(RecordInfo* ptr, long count)
            {
                Assert.IsTrue(RecordInfo.threadLockedRecord == ptr);
                Assert.IsTrue(RecordInfo.threadLockedRecordEntryCount == count);
            }
            RecordInfo recordInfo = new RecordInfo();
            RecordInfo* ri = (RecordInfo*)Unsafe.AsPointer(ref recordInfo);
            checkLatch(null, 0);
            recordInfo.SpinLock();
            checkLatch(ri, 1);
            recordInfo.SpinLock();
            checkLatch(ri, 2);
            recordInfo.Unlock();
            checkLatch(ri, 1);
            recordInfo.Unlock();
            checkLatch(null, 0);

            XLockTest(() => recordInfo.SpinLock(), () => recordInfo.Unlock());
        }

        private void XLockTest(Action locker, Action unlocker)
        {
            long lockTestValue = 0;
            const int numThreads = 50;
            const int numIters = 5000;

            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(XLockTestFunc)).ToArray();
            Task.WaitAll(tasks);

            Assert.AreEqual(numThreads * numIters, lockTestValue);

            void XLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    locker();
                    var temp = lockTestValue;
                    Thread.Yield();
                    lockTestValue = temp + 1;
                    unlocker();
                }
            }
        }

        [Test]
        public void IntExclusiveLockerTest()
        {
            int lockTestValue = 0;
            XLockTest(() => IntExclusiveLocker.SpinLock(ref lockTestValue), () => IntExclusiveLocker.Unlock(ref lockTestValue));
        }

        [Test]
        public void AdvancedFunctionsLockTest()
        {
            // Populate
            const int numRecords = 100;
            const int valueMult = 1000000;
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
            }

            // Update
            const int numThreads = 20;
            const int numIters = 500;
            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(() => UpdateFunc((ii & 1) == 0, numRecords, numIters))).ToArray();
            Task.WaitAll(tasks);

            // Verify
            for (int key = 0; key < numRecords; key++)
            {
                var expectedValue = key * valueMult + numThreads * numIters;
                Assert.AreNotEqual(Status.PENDING, session.Read(key, out int value));
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
                        Assert.AreNotEqual(Status.PENDING, session.Read(key));

                    // These will both just increment the stored value, ignoring the input argument.
                    if (useRMW)
                        session.RMW(key, default);
                    else
                        session.Upsert(key, default);
                }
            }
        }
    }
}

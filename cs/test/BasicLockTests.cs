// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.test.LockTests
{
    [TestFixture]
    internal class BasicLockTests
    {
        internal class Functions : SimpleFunctions<int, int>
        {
            static bool Increment(ref int dst)
            {
                ++dst;
                return true;
            }

            public override bool ConcurrentWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) => Increment(ref dst);

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, long address) => Increment(ref value);
        }

        private FasterKV<int, int> fkv;
        private ClientSession<int, int, int, int, Empty, Functions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericStringTests.log", deleteOnClose: true);
            fkv = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null }, disableLocking: false );
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
        public unsafe void RecordInfoLockTest()
        {
            for (var ii = 0; ii < 5; ++ii)
            {
                RecordInfo recordInfo = new();
                RecordInfo* ri = &recordInfo;

                // We are not sealing in this test, so there is no need to check the return
                XLockTest(() => ri->LockExclusive(), () => ri->UnlockExclusive());
                SLockTest(() => ri->LockShared(), () => ri->UnlockShared());
                XSLockTest(() => ri->LockExclusive(), () => ri->UnlockExclusive(), () => ri->LockShared(), () => ri->UnlockShared());
            }
        }

        private void XLockTest(Action locker, Action unlocker)
        {
            long lockTestValue = 0;
            const int numThreads = 50;
            const int numIters = 1000;

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

        private void SLockTest(Action locker, Action unlocker)
        {
            long lockTestValue = 1;
            long lockTestValueResult = 0;

            const int numThreads = 50;
            const int numIters = 1000;

            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(SLockTestFunc)).ToArray();
            Task.WaitAll(tasks);

            Assert.AreEqual(numThreads * numIters, lockTestValueResult);

            void SLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    locker();
                    Interlocked.Add(ref lockTestValueResult, Interlocked.Read(ref lockTestValue));
                    Thread.Yield();
                    unlocker();
                }
            }
        }

        private void XSLockTest(Action xlocker, Action xunlocker, Action slocker, Action sunlocker)
        {
            long lockTestValue = 0;
            long lockTestValueResult = 0;

            const int numThreads = 50;
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
                    xlocker();
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
                    slocker();
                    Interlocked.Add(ref lockTestValueResult, 1);
                    Thread.Yield();
                    sunlocker();
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        public void FunctionsLockTest()
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
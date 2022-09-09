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
using FASTER.test.LockableUnsafeContext;

namespace FASTER.test.ModifiedTests
{

    internal class LockableUnsafeComparer : IFasterEqualityComparer<int>
    {
        internal const int maxSleepMs = 1;
        readonly Random rng = new(101);

        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        public long GetHashCode64(ref int k)
        {
            if (maxSleepMs > 0)
                Thread.Sleep(rng.Next(maxSleepMs));
            return Utility.GetHashCode(k);
        }
    }

    public enum UpdateOp { Upsert, RMW, Delete }

    [TestFixture]
    class ModifiedBitTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;


        LockableUnsafeComparer comparer;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);


            comparer = new LockableUnsafeComparer();
            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 }, comparer: comparer, disableLocking: false);
            session = fht.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;

        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
        }

        static void AssertLockandModified(LockableUnsafeContext<int, int, int, int, Empty, SimpleFunctions<int, int>> luContext, int key, bool xlock, bool slock, bool modfied = false)
        {
            var (isX, isS) = luContext.IsLocked(key);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(xlock, isX, "xlock mismatch");
            Assert.AreEqual(slock, isS > 0, "slock mismatch");
            Assert.AreEqual(modfied, isM, "modfied mismatch");
        }

        static void AssertLockandModified(LockableContext<int, int, int, int, Empty, SimpleFunctions<int, int>> luContext, int key, bool xlock, bool slock, bool modfied = false)
        {
            var (isX, isS) = luContext.IsLocked(key);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(xlock, isX, "xlock mismatch");
            Assert.AreEqual(slock, isS > 0, "slock mismatch");
            Assert.AreEqual(modfied, isM, "modfied mismatch");
        }

        static void AssertLockandModified(ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session, int key, bool xlock, bool slock, bool modfied = false)
        {
            using (var luContext = session.GetLockableUnsafeContext())
            {
                luContext.ResumeThread();
                var (isX, isS) = luContext.IsLocked(key);
                var isM = luContext.IsModified(key);
                Assert.AreEqual(xlock, isX, "xlock mismatch");
                Assert.AreEqual(slock, isS > 0, "slock mismatch");
                Assert.AreEqual(modfied, isM, "Modified mismatch");
                luContext.SuspendThread();
            }
        }

        [Test]
        [Category(SmokeTestCategory)]
        public void LockAndNotModify()
        {
            Populate();
            Random r = new(100);
            int key = r.Next(numRecords);
            session.ResetModified(key);

            var LC = session.LockableContext;
            LC.Acquire();
            AssertLockandModified(LC, key, xlock: false, slock: false, modfied: false);

            LC.Lock(key, LockType.Exclusive);
            AssertLockandModified(LC, key, xlock: true, slock: false, modfied: false);

            LC.Unlock(key, LockType.Exclusive);
            AssertLockandModified(LC, key, xlock: false, slock: false, modfied: false);

            LC.Lock(key, LockType.Shared);
            AssertLockandModified(LC, key, xlock: false, slock: true, modfied: false);

            LC.Unlock(key, LockType.Shared);
            AssertLockandModified(LC, key, xlock: false, slock: false, modfied: false);
            LC.Release();

        }


        [Test]
        [Category(SmokeTestCategory)]
        public void ResetModifyForNonExistingKey()
        {
            Populate();
            int key = numRecords + 100;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modfied: false);
        }



        [Test]
        [Category(SmokeTestCategory)]
        public void ModifyClientSession([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modfied: false);

            if (flushToDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            Status status = default;
            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = session.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = session.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = session.Delete(key);
                    break;
                default:
                    break;
            }
            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        Assert.IsTrue(status.IsPending, status.ToString());
                        session.CompletePending(wait: true);
                        break;
                    default:
                        Assert.IsTrue(status.NotFound);
                        break;
                }
                (status, var _) = session.Read(key);
                Assert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
            }

            if (updateOp == UpdateOp.Delete)
                AssertLockandModified(session, key, xlock: false, slock: false, modfied: false);
            else
                AssertLockandModified(session, key, xlock: false, slock: false, modfied: true);


        }

        [Test]
        [Category(SmokeTestCategory)]
        public void ModifyLUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            using (var luContext = session.GetLockableUnsafeContext())
            {
                luContext.ResumeThread(out var epoch);
                AssertLockandModified(luContext, key, xlock: false, slock: false, modfied: false);
                luContext.SuspendThread();
            }

            if (flushToDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            Status status = default;
            using (var luContext = session.GetLockableUnsafeContext())
            {
                luContext.ResumeThread(out var epoch);

                switch (updateOp)
                {
                    case UpdateOp.Upsert:
                        status = luContext.Upsert(key, value);
                        break;
                    case UpdateOp.RMW:
                        status = luContext.RMW(key, value);
                        break;
                    case UpdateOp.Delete:
                        status = luContext.Delete(key);
                        break;
                    default:
                        break;
                }
                if (flushToDisk)
                {
                    switch (updateOp)
                    {
                        case UpdateOp.RMW:
                            Assert.IsTrue(status.IsPending, status.ToString());
                            luContext.CompletePending(wait: true);
                            break;
                        default:
                            Assert.IsTrue(status.NotFound);
                            break;
                    }
                    (status, var _) = luContext.Read(key);
                    Assert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
                }
                if (updateOp == UpdateOp.Delete)
                    AssertLockandModified(luContext, key, xlock: false, slock: false, modfied: false);
                else
                    AssertLockandModified(luContext, key, xlock: false, slock: false, modfied: true);
                luContext.SuspendThread();
            }

        }

        [Test]
        [Category(SmokeTestCategory)]
        public void ModifyUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modfied: false);

            if (flushToDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            Status status = default;
            using (var unsafeContext = session.GetLockableUnsafeContext())
            {
                unsafeContext.ResumeThread(out var epoch);
                switch (updateOp)
                {
                    case UpdateOp.Upsert:
                        status = unsafeContext.Upsert(key, value);
                        break;
                    case UpdateOp.RMW:
                        status = unsafeContext.RMW(key, value);
                        break;
                    case UpdateOp.Delete:
                        status = unsafeContext.Delete(key);
                        break;
                    default:
                        break;
                }
                if (flushToDisk)
                {
                    switch (updateOp)
                    {
                        case UpdateOp.RMW:
                            Assert.IsTrue(status.IsPending, status.ToString());
                            unsafeContext.CompletePending(wait: true);
                            break;
                        default:
                            Assert.IsTrue(status.NotFound);
                            break;
                    }
                    (status, var _) = unsafeContext.Read(key);
                    Assert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
                }
                unsafeContext.SuspendThread();
            }
            if (updateOp == UpdateOp.Delete)
                AssertLockandModified(session, key, xlock: false, slock: false, modfied: false);
            else
                AssertLockandModified(session, key, xlock: false, slock: false, modfied: true);
        }

        [Test]
        [Category(SmokeTestCategory)]
        public void ModifyLC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            var LC = session.LockableContext;
            LC.Acquire();
            AssertLockandModified(LC, key, xlock: false, slock: false, modfied: false);

            if (flushToDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            Status status = default;

            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = LC.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = LC.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = LC.Delete(key);
                    break;
                default:
                    break;
            }
            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        Assert.IsTrue(status.IsPending, status.ToString());
                        LC.CompletePending(wait: true);
                        break;
                    default:
                        Assert.IsTrue(status.NotFound);
                        break;
                }
                (status, var _) = LC.Read(key);
                Assert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
            }
            if (updateOp == UpdateOp.Delete)
                AssertLockandModified(LC, key, xlock: false, slock: false, modfied: false);
            else
                AssertLockandModified(LC, key, xlock: false, slock: false, modfied: true);
            LC.Release();
        }


        [Test]
        [Category(SmokeTestCategory)]
        public void CopyToTailTest()
        {
            Populate();
            fht.Log.FlushAndEvict(wait: true);

            using (var luContext = session.GetLockableUnsafeContext())
            {
                int input = 0, output = 0, key = 200;
                ReadOptions readOptions = new() { ReadFlags = ReadFlags.CopyReadsToTail };

                luContext.ResumeThread();

                // Check Read Copy to Tail resets the modfied
                var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
                Assert.IsTrue(status.IsPending, status.ToString());
                luContext.CompletePending(wait: true);
                AssertLockandModified(luContext, key, xlock: false, slock: false, modfied: true);

                // Check Read Copy to Tail resets the modfied on locked key
                key += 10;
                luContext.Lock(key, LockType.Exclusive);
                status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
                Assert.IsTrue(status.IsPending, status.ToString());
                luContext.CompletePending(wait: true);
                AssertLockandModified(luContext, key, xlock: true, slock: false, modfied: true);
                luContext.Unlock(key, LockType.Exclusive);


                luContext.SuspendThread();
            }
        }

    }
}

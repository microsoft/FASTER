// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;
using FASTER.test.LockTable;
using NUnit.Framework;
using static FASTER.test.TestUtils;

namespace FASTER.test.ModifiedTests
{
    internal class ModifiedBitTestComparer : IFasterEqualityComparer<int>
    {
        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        public long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }

    [TestFixture]
    class ModifiedBitTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;


        ModifiedBitTestComparer comparer;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            comparer = new ModifiedBitTestComparer();
            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 }, comparer: comparer, lockingMode: LockingMode.Standard);
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

        void AssertLockandModified(LockableUnsafeContext<int, int, int, int, Empty, SimpleFunctions<int, int>> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(fht, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(LockableContext<int, int, int, int, Empty, SimpleFunctions<int, int>> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(fht, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session, int key, bool xlock, bool slock, bool modified = false)
        {
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();

            OverflowBucketLockTableTests.AssertLockCounts(fht, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(modified, isM, "Modified mismatch");

            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void LockAndNotModify()
        {
            Populate();
            Random r = new(100);
            int key = r.Next(numRecords);
            session.ResetModified(key);

            var LC = session.LockableContext;
            LC.BeginLockable();
            AssertLockandModified(LC, key, xlock: false, slock: false, modified: false);

            LC.Lock(key, LockType.Exclusive);
            AssertLockandModified(LC, key, xlock: true, slock: false, modified: false);

            LC.Unlock(key, LockType.Exclusive);
            AssertLockandModified(LC, key, xlock: false, slock: false, modified: false);

            LC.Lock(key, LockType.Shared);
            AssertLockandModified(LC, key, xlock: false, slock: true, modified: false);

            LC.Unlock(key, LockType.Shared);
            AssertLockandModified(LC, key, xlock: false, slock: false, modified: false);
            LC.EndLockable();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ResetModifyForNonExistingKey()
        {
            Populate();
            int key = numRecords + 100;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyClientSession([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

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
                AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
            else
                AssertLockandModified(session, key, xlock: false, slock: false, modified: true);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyLUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: false);
            luContext.EndLockable();
            luContext.EndUnsafe();

            if (flushToDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            Status status = default;

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            luContext.Lock(key, LockType.Exclusive);

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
            }

            luContext.Unlock(key, LockType.Exclusive);

            if (flushToDisk)
            {
                luContext.Lock(key, LockType.Shared);
                (status, var _) = luContext.Read(key);
                Assert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                luContext.Unlock(key, LockType.Shared);
            }

            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);

            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

            if (flushToDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            Status status = default;
            var unsafeContext = session.UnsafeContext;

            unsafeContext.BeginUnsafe();
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
            unsafeContext.EndUnsafe();

            AssertLockandModified(session, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyLC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            var LC = session.LockableContext;
            LC.BeginLockable();
            AssertLockandModified(LC, key, xlock: false, slock: false, modified: false);
            LC.Lock(key, LockType.Exclusive);

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
            }

            LC.Unlock(key, LockType.Exclusive);

            if (flushToDisk)
            {
                LC.Lock(key, LockType.Shared);
                (status, var _) = LC.Read(key);
                Assert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                LC.Unlock(key, LockType.Shared);
            }

            AssertLockandModified(LC, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
            LC.EndLockable();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void CopyToTailTest()
        {
            Populate();
            fht.Log.FlushAndEvict(wait: true);

            var luContext = session.LockableUnsafeContext;

            int input = 0, output = 0, key = 200;
            ReadOptions readOptions = new() { ReadFlags = ReadFlags.CopyReadsToTail };

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            luContext.Lock(key, LockType.Shared);
            AssertLockandModified(luContext, key, xlock: false, slock: true, modified: true);

            // Check Read Copy to Tail resets the modified
            var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            luContext.CompletePending(wait: true);

            luContext.Unlock(key, LockType.Shared);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            // Check Read Copy to Tail resets the modified on locked key
            key += 10;
            luContext.Lock(key, LockType.Exclusive);
            status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            luContext.CompletePending(wait: true);
            AssertLockandModified(luContext, key, xlock: true, slock: false, modified: true);
            luContext.Unlock(key, LockType.Exclusive);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ReadFlagsResetModifiedBit([Values] FlushMode flushMode)
        {
            Populate();

            int input = 0, output = 0, key = numRecords / 2;
            AssertLockandModified(session, key, xlock: false, slock: false, modified: true);

            if (flushMode == FlushMode.ReadOnly)
                this.fht.hlog.ShiftReadOnlyAddress(fht.Log.TailAddress);
            else if (flushMode == FlushMode.OnDisk)
                this.fht.Log.FlushAndEvict(wait: true);

            ReadOptions readOptions = new() { ReadFlags = ReadFlags.CopyReadsToTail | ReadFlags.ResetModifiedBit };

            // Check that reading the record clears the modified bit, even if it went through CopyToTail
            var status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.AreEqual(flushMode == FlushMode.OnDisk, status.IsPending, status.ToString());
            if (status.IsPending)
                session.CompletePending(wait: true);

            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
        }
    }
}

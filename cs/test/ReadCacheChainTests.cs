// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FASTER.test.ReadCacheTests
{
    class ChainTests
    {
        private FasterKV<int, int> fht;
        private IDevice log;
        const int lowChainKey = 40;
        const int midChainKey = lowChainKey + chainLen * (mod / 2);
        const int highChainKey = lowChainKey + chainLen * (mod - 1);
        const int mod = 10;
        const int chainLen = 10;
        const int valueAdd = 1_000_000;

        // -1 so highChainKey is first in the chain.
        const int numKeys = highChainKey + mod - 1;

        // Insert into chain.
        const int spliceInNewKey = highChainKey + mod * 2;
        const int spliceInExistingKey = highChainKey - mod;
        const int immutableSplitKey = numKeys / 2;

        // This is the record after the first readcache record we insert; it lets us limit the range to ReadCacheEvict
        // so we get outsplicing rather than successively overwriting the hash table entry on ReadCacheEvict.
        long readCacheHighEvictionAddress;

        internal class ChainComparer : IFasterEqualityComparer<int>
        {
            int mod;
            internal ChainComparer(int mod) => this.mod = mod;

            public bool Equals(ref int k1, ref int k2) => k1 == k2;

            public long GetHashCode64(ref int k) => k % mod;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 9 };
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/NativeReadCacheTests.log", deleteOnClose: true);
            fht = new FasterKV<int, int>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings },
                comparer: new ChainComparer(mod));
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        void PopulateAndEvict(bool immutable = false)
        {
            using var session = fht.NewSession(new SimpleFunctions<int, int>());

            if (!immutable)
            {
                for (int key = 0; key < numKeys; key++)
                    session.Upsert(key, key + valueAdd);
                session.CompletePending(true);
                fht.Log.FlushAndEvict(true);
                return;
            }

            // Two parts, so we can have some evicted (and bring them into the readcache), and some in immutable (readonly).
            for (int key = 0; key < immutableSplitKey; key++)
                session.Upsert(key, key + valueAdd);
            session.CompletePending(true);
            fht.Log.FlushAndEvict(true);

            for (int key = immutableSplitKey; key < numKeys; key++)
                session.Upsert(key, key + valueAdd);
            session.CompletePending(true);
            fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
        }

        void CreateChain(bool immutable = false)
        {
            using var session = fht.NewSession(new SimpleFunctions<int, int>());

            // Pass1: PENDING reads and populate the cache
            for (var ii = 0; ii < chainLen; ++ii)
            {
                var key = lowChainKey + ii * mod;
                var status = session.Read(key, out _);
                Assert.AreEqual((immutable && key >= immutableSplitKey) ? Status.OK : Status.PENDING, status);
                session.CompletePending(wait: true);
                if (ii == 0)
                    readCacheHighEvictionAddress = fht.readcache.GetTailAddress();
            }

            // Pass2: non-PENDING reads from the cache
            for (var ii = 0; ii < chainLen; ++ii)
            {
                var status = session.Read(lowChainKey + ii * mod, out _);
                Assert.AreNotEqual(Status.PENDING, status);
            }

            // Pass 3: Put in bunch of extra keys into the cache so when we FlushAndEvict we get all the ones of interest.
            for (var key = 0; key < numKeys; ++key)
            {
                if ((key % mod) != 0)
                {
                    var status = session.Read(key, out _);
                    Assert.AreEqual((immutable && key >= immutableSplitKey) ? Status.OK : Status.PENDING, status);
                    session.CompletePending(wait: true);
                }
            }
        }

        unsafe (long logicalAddress, long physicalAddress) GetHashChain(int key, out int recordKey, out bool invalid, out bool isReadCache)
            => GetHashChain(fht, key, out recordKey, out invalid, out isReadCache);

        internal static unsafe (long logicalAddress, long physicalAddress) GetHashChain(FasterKV<int, int> fht, int key, out int recordKey, out bool invalid, out bool isReadCache)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);

            var hash = fht.comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            var entry = default(HashBucketEntry);
            var tagExists = fht.FindTag(hash, tag, ref bucket, ref slot, ref entry);
            Assert.IsTrue(tagExists);

            isReadCache = entry.ReadCache;
            var log = isReadCache ? fht.readcache : fht.hlog;
            var pa = log.GetPhysicalAddress(entry.Address);
            recordKey = log.GetKey(pa);
            invalid = log.GetInfo(pa).Invalid;

            return (entry.Address, pa);
        }

        (long logicalAddress, long physicalAddress) NextInChain(long physicalAddress, out int recordKey, out bool invalid, ref bool isReadCache)
            => NextInChain(fht, physicalAddress, out recordKey, out invalid, ref isReadCache);

        internal static (long logicalAddress, long physicalAddress) NextInChain(FasterKV<int, int> fht, long physicalAddress, out int recordKey, out bool invalid, ref bool isReadCache)
        {
            var log = isReadCache ? fht.readcache : fht.hlog;
            var info = log.GetInfo(physicalAddress);
            var la = info.PreviousAddress;
            isReadCache = new HashBucketEntry { word = la }.ReadCache;
            log = isReadCache ? fht.readcache : fht.hlog;
            var pa = log.GetPhysicalAddress(la);
            recordKey = log.GetKey(pa);
            invalid = log.GetInfo(pa).Invalid;
            return (la, pa);
        }

        (long logicalAddress, long physicalAddress) ScanReadCacheChain(int[] omitted = null, bool evicted = false)
        {
            omitted ??= Array.Empty<int>();

            var (la, pa) = GetHashChain(fht, lowChainKey, out int actualKey, out bool invalid, out bool isReadCache);
            for (var expectedKey = highChainKey; expectedKey >= lowChainKey; expectedKey -= mod)
            {
                if (omitted.Contains(expectedKey))
                {
                    // Either we have not yet evicted Invalid readcache records, in which case we'll see an Invalid record,
                    // or we have, in which case we don't see that record.
                    if (evicted)
                    {
                        expectedKey -= mod;
                        if (expectedKey < lowChainKey)
                        {
                            Assert.IsFalse(isReadCache);
                            break;
                        }
                    }
                    else
                        Assert.IsTrue(invalid);
                }
                Assert.AreEqual(expectedKey, actualKey);
                Assert.IsTrue(isReadCache);
                (la, pa) = NextInChain(pa, out actualKey, out invalid, ref isReadCache);
            }
            Assert.IsFalse(isReadCache);
            return (la, pa);
        }

        (long logicalAddress, long physicalAddress) SkipReadCacheChain(int key)
            => SkipReadCacheChain(fht, key);

        internal static (long logicalAddress, long physicalAddress) SkipReadCacheChain(FasterKV<int, int> fht, int key)
        {
            var (la, pa) = ChainTests.GetHashChain(fht, key, out _, out _, out bool isReadCache);
            while (isReadCache)
                (la, pa) = ChainTests.NextInChain(fht, pa, out _, out _, ref isReadCache);
            return (la, pa);
        }

        void VerifySplicedInKey(int expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = SkipReadCacheChain(expectedKey);
            var storedKey = fht.hlog.GetKey(pa);
            Assert.AreEqual(expectedKey, storedKey);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void ChainVerificationTest()
        {
            PopulateAndEvict();
            CreateChain();

            ScanReadCacheChain();
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void DeleteCacheRecordTest()
        {
            PopulateAndEvict();
            CreateChain();
            using var session = fht.NewSession(new SimpleFunctions<int, int>());

            void doTest(int key)
            {
                var status = session.Delete(key);
                Assert.AreEqual(Status.OK, status);

                status = session.Read(key, out var value);
                Assert.AreEqual(Status.NOTFOUND, status);
            }

            doTest(lowChainKey);
            doTest(highChainKey);
            doTest(midChainKey);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: false);

            fht.ReadCacheEvict(fht.readcache.BeginAddress, readCacheHighEvictionAddress);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: true);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void DeleteAllCacheRecordsTest()
        {
            PopulateAndEvict();
            CreateChain();
            using var session = fht.NewSession(new SimpleFunctions<int, int>());

            void doTest(int key)
            {
                var status = session.Delete(key);
                Assert.AreEqual(Status.OK, status);

                status = session.Read(key, out var value);
                Assert.AreEqual(Status.NOTFOUND, status);
            }

            // Delete all keys in the readcache chain.
            for (var ii = lowChainKey; ii <= highChainKey; ++ii)
                doTest(ii);

            var _ = GetHashChain(lowChainKey, out int actualKey, out bool invalid, out bool isReadCache);
            Assert.IsTrue(isReadCache);
            Assert.IsTrue(invalid);

            fht.ReadCacheEvict(fht.readcache.BeginAddress, readCacheHighEvictionAddress);
            _ = GetHashChain(lowChainKey, out actualKey, out invalid, out isReadCache);
            Assert.IsFalse(isReadCache);
            Assert.IsFalse(invalid);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void UpsertCacheRecordTest()
        {
            DoUpdateTest(useRMW: false);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void RMWCacheRecordTest()
        {
            DoUpdateTest(useRMW: true);
        }

        void DoUpdateTest(bool useRMW)
        {
            PopulateAndEvict();
            CreateChain();
            using var session = fht.NewSession(new SimpleFunctions<int, int>());

            void doTest(int key)
            {
                var status = session.Read(key, out var value);
                Assert.AreEqual(Status.OK, status);

                if (useRMW)
                {
                    // RMW will get the old value from disk, unlike Upsert
                    status = session.RMW(key, value + valueAdd);
                    Assert.AreEqual(Status.PENDING, status);
                    session.CompletePending(wait: true);
                }
                else
                {
                    status = session.Upsert(key, value + valueAdd);
                    Assert.AreEqual(Status.OK, status);
                }

                status = session.Read(key, out value);
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key + valueAdd * 2, value);
            }

            doTest(lowChainKey);
            doTest(highChainKey);
            doTest(midChainKey);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: false);

            fht.ReadCacheEvict(fht.readcache.BeginAddress, readCacheHighEvictionAddress);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: true);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void SpliceInFromCTTTest()
        {
            PopulateAndEvict();
            CreateChain();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            int input = 0, output = 0, key = lowChainKey - mod; // key must be in evicted region for this test
            RecordMetadata recordMetadata = default;

            var status = session.Read(ref key, ref input, ref output, ref recordMetadata, ReadFlags.CopyToTail);
            Assert.AreEqual(Status.PENDING, status);
            session.CompletePending(wait: true);

            VerifySplicedInKey(key);
        }

        public enum RecordRegion { Immutable, OnDisk, NotFound };

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void SpliceInFromUpsertTest([Values] RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == RecordRegion.Immutable);
            CreateChain(recordRegion == RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            int key = -1;

            if (recordRegion == RecordRegion.Immutable || recordRegion == RecordRegion.OnDisk)
            {
                key = spliceInExistingKey;
                var status = session.Upsert(key, key + valueAdd);
                Assert.AreEqual(Status.OK, status);
            }
            else
            {
                key = spliceInNewKey;
                var status = session.Upsert(key, key + valueAdd);
                Assert.AreEqual(Status.OK, status);
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void SpliceInFromRMWTest([Values] RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == RecordRegion.Immutable);
            CreateChain(recordRegion == RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            int key = -1;

            if (recordRegion == RecordRegion.Immutable || recordRegion == RecordRegion.OnDisk)
            {
                key = spliceInExistingKey;
                var status = session.RMW(key, key + valueAdd);
                Assert.AreEqual(recordRegion == RecordRegion.OnDisk ? Status.PENDING : Status.OK, status);
                session.CompletePending(wait: true);
            }
            else
            {
                key = spliceInNewKey;
                var status = session.RMW(key, key + valueAdd);
                // This NOTFOUND key will return PENDING because we have to trace back through the collisions.
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(wait: true);
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void SpliceInFromDeleteTest([Values] RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == RecordRegion.Immutable);
            CreateChain(recordRegion == RecordRegion.Immutable);

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            int key = -1;

            if (recordRegion == RecordRegion.Immutable || recordRegion == RecordRegion.OnDisk)
            {
                key = spliceInExistingKey;
                var status = session.Delete(key);
                Assert.AreEqual(Status.OK, status);
            }
            else
            {
                key = spliceInNewKey;
                var status = session.Delete(key);
                Assert.AreEqual(Status.OK, status);
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void EvictFromReadCacheToLockTableTest()
        {
            PopulateAndEvict();
            CreateChain();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();

            Dictionary<int, LockType> locks = new()
            {
                { lowChainKey, LockType.Exclusive },
                { midChainKey, LockType.Shared },
                { highChainKey, LockType.Exclusive }
            };

            // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
            foreach (var key in locks.Keys.OrderBy(k => k))
                luContext.Lock(key, locks[key]);

            fht.ReadCache.FlushAndEvict(wait: true);

            Assert.IsTrue(fht.LockTable.IsActive);
            Assert.AreEqual(locks.Count, fht.LockTable.dict.Count);

            foreach (var key in locks.Keys)
            {
                var found = fht.LockTable.Get(key, out RecordInfo recordInfo);
                Assert.IsTrue(found);
                var lockType = locks[key];
                Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.IsLockedShared);

                luContext.Unlock(key, lockType);
                Assert.IsFalse(fht.LockTable.Get(key, out recordInfo));
            }

            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);
        }

        [Test]
        [Category(TestUtils.FasterKVTestCategory)]
        [Category(TestUtils.ReadCacheTestCategory)]
        [Category(TestUtils.SmokeTestCategory)]
        public void TransferFromLockTableToReadCacheTest()
        {
            PopulateAndEvict();

            // DO NOT create the chain here; do that below. Here, we create records in the lock table and THEN we create
            // the chain, resulting in transfer of the locked records.
            //CreateChain();

            using var session = fht.NewSession(new SimpleFunctions<int, int>());
            using var luContext = session.GetLockableUnsafeContext();

            Dictionary<int, LockType> locks = new()
            {
                { lowChainKey, LockType.Exclusive },
                { midChainKey, LockType.Shared },
                { highChainKey, LockType.Exclusive }
            };

            // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
            foreach (var key in locks.Keys.OrderBy(k => k))
                luContext.Lock(key, locks[key]);

            fht.ReadCache.FlushAndEvict(wait: true);

            // Verify the locks have been evicted to the lockTable
            Assert.IsTrue(fht.LockTable.IsActive);
            Assert.AreEqual(locks.Count, fht.LockTable.dict.Count);

            foreach (var key in locks.Keys)
            {
                var found = fht.LockTable.Get(key, out RecordInfo recordInfo);
                Assert.IsTrue(found);
                var lockType = locks[key];
                Assert.AreEqual(lockType == LockType.Exclusive, recordInfo.IsLockedExclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, recordInfo.IsLockedShared);
            }

            fht.Log.FlushAndEvict(wait: true);

            // Create the readcache entries, which will transfer the locks from the locktable to the readcache
            foreach (var key in locks.Keys)
            {
                var status = session.Read(key, out _);
                Assert.AreEqual(Status.PENDING, status);
                session.CompletePending(wait: true);

                var lockType = locks[key];
                var (exclusive, shared) = luContext.IsLocked(key);
                Assert.AreEqual(lockType == LockType.Exclusive, exclusive);
                Assert.AreEqual(lockType != LockType.Exclusive, shared);

                luContext.Unlock(key, lockType);
                Assert.IsFalse(fht.LockTable.Get(key, out _));
            }

            Assert.IsFalse(fht.LockTable.IsActive);
            Assert.AreEqual(0, fht.LockTable.dict.Count);
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test.LockTable
{
    internal class SingleBucketComparer : IFasterEqualityComparer<long>
    {
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        public long GetHashCode64(ref long k) => 42L;
    }

    [TestFixture]
    internal class LockTableTests
    {
        OverflowBucketLockTable<long> lockTable;
        IFasterEqualityComparer<long> comparer = new LongFasterEqualityComparer();
        long SingleBucketKey = 1;   // We use a single bucket here for most tests so this lets us use 'ref' easily

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir);
            lockTable = new(true);
        }

        [TearDown]
        public void TearDown()
        {
            lockTable.Dispose();
            lockTable = default;
        }

        void TryLock(long key, LockType lockType, bool ephemeral, int expectedCurrentReadLocks, bool expectedLockResult, bool expectedGotLock)
        {
            var hash = comparer.GetHashCode64(ref key);

            // Check for existing lock
            var found = lockTable.TryGet(ref key, out var existingRecordInfo);
            Assert.AreEqual(expectedCurrentReadLocks > 0, found);
            if (found)
                Assert.AreEqual(expectedCurrentReadLocks, existingRecordInfo.NumLockedShared);

            bool gotLock;
            if (ephemeral)
                Assert.AreEqual(expectedLockResult, lockTable.TryLockEphemeral(ref key, hash, lockType, out gotLock));
            else
            {
                // All manual locks start out tentative; if there is already a lock there, they increment it non-tentatively
                Assert.AreEqual(expectedLockResult, gotLock = lockTable.TryLockManual(ref key, hash, lockType, out bool isTentative));
                Assert.AreEqual(expectedCurrentReadLocks == 0, isTentative);
                if (isTentative)
                    lockTable.ClearTentativeBit(ref key, hash);
            }

            Assert.AreEqual(expectedGotLock, gotLock);
            if (expectedGotLock)
                Assert.IsTrue(lockTable.HasEntries(hash));
        }

        void Unlock(long key, LockType lockType) => lockTable.Unlock(ref key, lockTable.functions.GetHashCode64(ref key), lockType);

        ref InMemKVBucket<long, IHeapContainer<long>, RecordInfo, LockTable<long>.LockTableFunctions> GetBucket(ref long key)
        {
            _ = lockTable.kv.GetBucket(ref key, out var bucketIndex);   // Compiler won't allow a direct 'ref' return for some reason
            return ref lockTable.kv.buckets[bucketIndex];
        }

        static InMemKVChunk<long, IHeapContainer<long>, RecordInfo, LockTable<long>.LockTableFunctions> GetFirstChunk(
                ref InMemKVBucket<long, IHeapContainer<long>, RecordInfo, LockTable<long>.LockTableFunctions> bucket)
        {
            var chunk = bucket.LastOverflowChunk;
            while (chunk is not null && chunk.prev is not null)
                chunk = chunk.prev;
            return chunk;
        }

        public enum RemovalType { Unlock, Transfer };

        internal static bool LockTableHasEntries<TKey>(LockTable<TKey> lockTable)
        {
            foreach (var bucket in lockTable.kv.buckets)
            {
                if (bucket.HasEntries)
                    return true;
            }
            return false;
        }

        internal static int LockTableEntryCount<TKey>(LockTable<TKey> lockTable)
        {
            int count = 0;
            foreach (var bucket in lockTable.kv.buckets)
            {
                var localBucket = bucket; // can't ref iteration variable
                count += LockTableBucketCount(ref localBucket);
            }
            return count;
        }

        internal static int LockTableBucketCount<TKey>(ref InMemKVBucket<TKey, IHeapContainer<TKey>, RecordInfo, LockTable<TKey>.LockTableFunctions> bucket)
        {
            if (bucket.InitialEntry.IsDefault)
                return 0;
            if (!bucket.HasOverflow)
                return 1;

            int count = bucket.LastActiveChunkEntryIndex + 1 /* 0-based */ + 1 /* initialEntry */;
            for (var chunk = bucket.LastOverflowChunk.prev; chunk is not null; chunk = chunk.prev)
            {
                for (var iEntry = 0; iEntry < InMemKV.kChunkSize; ++iEntry)
                {
                    Assert.IsFalse(chunk[iEntry].IsDefault);
                    ++count;
                }
            }
            return count;
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void EphemeralLockTest()
        {
            // No entries
            ref var initialEntry = ref GetBucket(ref SingleBucketKey).InitialEntry;
            Assert.IsTrue(initialEntry.IsDefault);
            long key = 1;
            TryLock(key, LockType.Shared, ephemeral: true, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock:false);
            Assert.IsTrue(initialEntry.IsDefault);
            Assert.IsFalse(lockTable.IsActive);
            Assert.IsFalse(lockTable.TryGet(ref key, out _));

            // Add a non-ephemeral lock
            TryLock(key, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);
            Assert.IsFalse(initialEntry.IsDefault);
            Assert.IsTrue(lockTable.IsActive);
            Assert.AreEqual(1, initialEntry.value.NumLockedShared);
            Assert.IsTrue(lockTable.TryGet(ref key, out _));

            // Now the ephemeral lock with the same key should lock it
            TryLock(key, LockType.Shared, ephemeral: true, expectedCurrentReadLocks: 1, expectedLockResult: true, expectedGotLock: true);
            Assert.IsFalse(initialEntry.IsDefault);
            Assert.IsTrue(lockTable.IsActive);
            Assert.AreEqual(2, initialEntry.value.NumLockedShared);

            // An ephemeral lock with a different key should not add a lock
            key = 2;
            TryLock(key, LockType.Shared, ephemeral: true, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: false);
            Assert.IsFalse(lockTable.TryGet(ref key, out _));
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void SingleEntryTest()
        {
            TryLock(1, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);
            ref var bucket = ref GetBucket(ref SingleBucketKey);
            ref var initialEntry = ref bucket.InitialEntry;
            Assert.IsFalse(initialEntry.IsDefault);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.IsTrue(lockTable.IsActive);
            Unlock(1, LockType.Shared);
            Assert.IsTrue(initialEntry.IsDefault);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.IsFalse(lockTable.IsActive);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreeEntryTest()
        {
            Assert.IsFalse(lockTable.IsActive);

            TryLock(1, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);
            Assert.IsTrue(lockTable.HasEntries(ref SingleBucketKey));
            ref var bucket = ref GetBucket(ref SingleBucketKey);
            ref var initialEntry = ref bucket.InitialEntry;
            Assert.IsFalse(initialEntry.IsDefault);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.IsTrue(lockTable.IsActive);

            // Verify the same key is locked.
            TryLock(1, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 1, expectedLockResult: true, expectedGotLock: true);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.AreEqual(2, initialEntry.value.NumLockedShared);

            Unlock(1, LockType.Shared);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.AreEqual(1, initialEntry.value.NumLockedShared);
            Assert.AreEqual(0, lockTable.kv.FreeListCount);

            TryLock(2, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);
            Assert.IsTrue(bucket.HasOverflow);
            var chunk1 = GetFirstChunk(ref bucket);
            ref var chunkEntry1 = ref chunk1[0];
            ref var chunkEntry2 = ref chunk1[1];
            Assert.IsFalse(chunkEntry1.IsDefault);
            Assert.AreEqual(2, chunkEntry1.heapKey.Get());
            Assert.IsTrue(chunkEntry2.IsDefault);

            // The last entry on the chunk should be default.
            ref var lastEntryOnChunk = ref chunk1[InMemKV.kChunkSize - 1];
            Assert.IsTrue(lastEntryOnChunk.IsDefault);

            TryLock(3, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);
            Assert.IsTrue(bucket.HasOverflow);
            Assert.IsFalse(chunkEntry2.IsDefault);
            Assert.AreEqual(3, chunkEntry2.heapKey.Get());

            // Unlock chunkEntry2; there is no record after it, so it should become empty, but chunkEntry1 is still there.
            Unlock(3, LockType.Shared);
            Assert.IsTrue(bucket.HasOverflow);
            Assert.AreEqual(0, lockTable.kv.FreeListCount);
            Assert.IsFalse(chunkEntry1.IsDefault);
            Assert.IsTrue(chunkEntry2.IsDefault);

            // Unlock chunkEntry1; there is no record after it, so the page should be freed.
            Unlock(2, LockType.Shared);
            Assert.IsFalse(initialEntry.IsDefault);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.AreEqual(1, lockTable.kv.FreeListCount);

            Unlock(1, LockType.Shared);
            Assert.IsTrue(initialEntry.IsDefault);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.AreEqual(1, lockTable.kv.FreeListCount);
            Assert.IsFalse(lockTable.IsActive);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void UnlockMidChunkEntryTest()
        {
            for (long key = 1; key <= 5; ++key)
                TryLock(key, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);
            ref var bucket = ref GetBucket(ref SingleBucketKey);
            ref var initialEntry = ref bucket.InitialEntry;
            Assert.AreEqual(1, initialEntry.heapKey.Get());
            Assert.IsTrue(bucket.HasOverflow);
            var chunk1 = GetFirstChunk(ref bucket);
            ref var chunkEntry1 = ref chunk1[0];
            Assert.AreEqual(2, chunkEntry1.heapKey.Get());
            ref var chunkEntry2 = ref chunk1[1];
            Assert.AreEqual(3, chunkEntry2.heapKey.Get());
            ref var chunkEntry3 = ref chunk1[2];
            Assert.AreEqual(4, chunkEntry3.heapKey.Get());
            ref var chunkEntry4 = ref chunk1[3];
            Assert.AreEqual(5, chunkEntry4.heapKey.Get());

            // Unlock chunkEntry3, which will Compact chunkEntry4 into it.
            Unlock(4, LockType.Shared);
            Assert.AreEqual(5, chunkEntry3.heapKey.Get());
            Assert.IsTrue(chunkEntry4.IsDefault);

            // Unlock chunkEntry1, which will Compact chunkEntry3 into it.
            Unlock(2, LockType.Shared);
            Assert.AreEqual(5, chunkEntry1.heapKey.Get());
            Assert.IsTrue(chunkEntry3.IsDefault);

            // Unlock chunkEntry2, which will not Compact because there is nothing after it.
            Unlock(3, LockType.Shared);
            Assert.IsTrue(chunkEntry2.IsDefault);
            Assert.IsTrue(chunkEntry3.IsDefault);
            Assert.IsTrue(chunkEntry4.IsDefault);

            // Unlock the initial entry, which will Compact chunkEntry1 into it and free the chunk.
            Unlock(1, LockType.Shared);
            Assert.AreEqual(5, initialEntry.heapKey.Get());
            Assert.AreEqual(1, lockTable.kv.FreeListCount);
            Assert.IsTrue(lockTable.IsActive);

            // Remove the final entry.
            Unlock(5, LockType.Shared);
            Assert.IsTrue(initialEntry.IsDefault);
            Assert.IsFalse(bucket.HasOverflow);
            Assert.AreEqual(1, lockTable.kv.FreeListCount);
            Assert.IsFalse(lockTable.IsActive);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void UnlockLastChunkEntryTest()
        {
            // Fill the initial entry and the chunk.
            for (long key = 1; key <= InMemKV.kChunkSize + 1; ++key)
                TryLock(key, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);

            ref var bucket = ref GetBucket(ref SingleBucketKey);
            ref var initialEntry = ref bucket.InitialEntry;
            var chunk1 = GetFirstChunk(ref bucket);
            var lastKey = InMemKV.kChunkSize + 1;
            ref var lastEntry = ref chunk1[InMemKV.kChunkSize - 1];
            Assert.AreEqual(lastKey, lastEntry.heapKey.Get());

            Unlock(lastKey, LockType.Shared);
            Assert.IsTrue(lastEntry.IsDefault);
            Assert.AreEqual(0, lockTable.kv.FreeListCount);
            Assert.IsTrue(lockTable.IsActive);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void CompactFromSecondChunkTest()
        {
            // Fill the initial entry and the chunk, and the first entry of the second chunk.
            for (long key = 1; key <= InMemKV.kChunkSize + 2; ++key)
                TryLock(key, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);

            ref var bucket = ref GetBucket(ref SingleBucketKey);
            ref var initialEntry = ref bucket.InitialEntry;
            var chunk1 = GetFirstChunk(ref bucket);
            Assert.AreSame(chunk1.next, bucket.LastOverflowChunk);
            var lastKey = InMemKV.kChunkSize + 1;
            ref var lastEntry = ref chunk1[InMemKV.kChunkSize - 1];
            Assert.AreEqual(lastKey, lastEntry.heapKey.Get());

            // Unlock the last entry on the first chunk; there should be only one entry in the second
            // chunk, so this will cause it to be Compacted into the first chunk and the second chunk freed.
            Unlock(lastKey, LockType.Shared);
            Assert.IsFalse(lastEntry.IsDefault);
            Assert.AreEqual(1, lockTable.kv.FreeListCount);
            Assert.IsTrue(lockTable.IsActive);
            Assert.AreSame(chunk1, bucket.LastOverflowChunk);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void UnlockOnlyEntryOnSecondChunkTest()
        {
            // Fill the initial entry and the chunk, and the first entry of the second chunk.
            long lastKey = InMemKV.kChunkSize + 2;
            for (long key = 1; key <= lastKey; ++key)
                TryLock(key, LockType.Shared, ephemeral: false, expectedCurrentReadLocks: 0, expectedLockResult: true, expectedGotLock: true);

            ref var bucket = ref GetBucket(ref SingleBucketKey);
            ref var initialEntry = ref bucket.InitialEntry;
            var chunk1 = GetFirstChunk(ref bucket);
            var chunk2 = bucket.LastOverflowChunk;
            Assert.AreNotSame(chunk1, chunk2);
            Assert.AreSame(chunk1.next, chunk2);
            Assert.IsNull(chunk1.prev);
            Assert.AreSame(chunk2.prev, chunk1);
            Assert.IsNull(chunk2.next);
            ref var lastEntryOnFirstChunk = ref chunk1[InMemKV.kChunkSize - 1];

            ref var firstEntryOnSecondChunk = ref chunk2[0];
            Assert.AreEqual(lastKey, firstEntryOnSecondChunk.heapKey.Get());
            Assert.IsTrue(chunk2[1].IsDefault); // Second entry on second chunk should be empty

            // Unlock the first entry on the second chunk; this should free the second chunk.
            Unlock(lastKey, LockType.Shared);
            Assert.AreEqual(1, lockTable.kv.FreeListCount);
            Assert.IsTrue(lockTable.IsActive);
            Assert.AreSame(chunk1, bucket.LastOverflowChunk);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTest1Thread()
        {
            List<Task> tasks = new();
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: 1, maxNumKeys: 5, lowKey: 1, highKey: 5, LockType.Exclusive);
            Task.WaitAll(tasks.ToArray());
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsNoContention([Values(3, 8)] int numThreads)
        {
            List<Task> tasks = new();
            var lastTid = 0;
            for (var ii = 0; ii < numThreads; ++ii)
                AddThreads(tasks, ref lastTid, numThreads: 1, maxNumKeys: 5, lowKey: 1 + 10 * ii, highKey: 5 + 10 * ii, LockType.Exclusive);
            Task.WaitAll(tasks.ToArray());
            Assert.IsTrue(!lockTable.IsActive, "Expected lockTable to be inactive");
            Assert.AreEqual(0, LockTableEntryCount(lockTable), "Expected LockTableEntryCount to be 0");
            Assert.IsFalse(LockTableHasEntries(lockTable), "Expected LockTableHasEntries to be false");
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsFullContention([Values(3, 8)] int numThreads, [Values] LockType lockType)
        {
            List<Task> tasks = new();
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: numThreads, maxNumKeys: 5, lowKey: 1, highKey: 5, lockType);
            Task.WaitAll(tasks.ToArray());
            Assert.IsTrue(!lockTable.IsActive, "Expected lockTable to be inactive");
            Assert.AreEqual(0, LockTableEntryCount(lockTable), "Expected LockTableEntryCount to be 0");
            Assert.IsFalse(LockTableHasEntries(lockTable), "Expected LockTableHasEntries to be false");
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsRandomContention([Values(3, 8)] int numThreads, [Values] LockType lockType)
        {
            List<Task> tasks = new();
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: numThreads, maxNumKeys: 5, lowKey: 1, highKey: 10 * (numThreads / 2), lockType);
            Task.WaitAll(tasks.ToArray());
            Assert.IsTrue(!lockTable.IsActive, "Expected lockTable to be inactive");
            Assert.AreEqual(0, LockTableEntryCount(lockTable), "Expected LockTableEntryCount to be 0");
            Assert.IsFalse(LockTableHasEntries(lockTable), "Expected LockTableHasEntries to be false");
        }

        const int NumTestIterations = 15;
        const int maxSleepMs = 5;

        internal struct ThreadStruct
        {
            internal long key;
            internal long hash;
            internal LockType lockType;

            public override string ToString() => $"key {key}, hash {hash}, {lockType}";
        }

        private void AddThreads(List<Task> tasks, ref int lastTid, int numThreads, int maxNumKeys, int lowKey, int highKey, LockType lockType)
        {
            void runThread(int tid)
            {
                Random rng = new(101 * tid);

                // maxNumKeys < 0 means use random number of keys
                int numKeys = maxNumKeys < 0 ? rng.Next(1, -maxNumKeys) : maxNumKeys;
                ThreadStruct[] threadStructs = new ThreadStruct[numKeys];

                long getNextKey()
                {
                    while (true)
                    {
                        var key = rng.Next(lowKey, highKey + 1);    // +1 because the end # is not included
                        if (!Array.Exists(threadStructs, it => it.key == key ))
                            return key;
                    }
                }

                for (var iteration = 0; iteration < NumTestIterations; ++iteration)
                {
                    // Create key structs
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        var key = getNextKey();
                        threadStructs[ii] = new()   // local var for debugging
                        {
                            key = key,
                            // LockType.None means split randomly between Shared and Exclusive
                            lockType = lockType == LockType.None ? (rng.Next(0, 100) > 50 ? LockType.Shared : LockType.Exclusive) : lockType,
                            hash = lockTable.functions.GetHashCode64(ref key)
                        };
                    }

                    // Sort and lock
                    Array.Sort(threadStructs, (x, y) => x.key.CompareTo(y.key));
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        bool isTentative;
                        while (!lockTable.TryLockManual(ref threadStructs[ii].key, threadStructs[ii].hash, threadStructs[ii].lockType, out isTentative))
                            ;
                        if (isTentative)
                            Assert.IsTrue(lockTable.ClearTentativeBit(ref threadStructs[ii].key, threadStructs[ii].hash));
                    }

                    // Pretend to do work
                    Thread.Sleep(rng.Next(maxSleepMs));

                    // Unlock
                    for (var ii = 0; ii < numKeys; ++ii)
                        Assert.IsTrue(lockTable.Unlock(ref threadStructs[ii].key, threadStructs[ii].hash, threadStructs[ii].lockType));
                    Array.Clear(threadStructs);
                }

            }

            for (int t = 1; t <= numThreads; t++)
            {
                var tid = ++lastTid;
                tasks.Add(Task.Factory.StartNew(() => runThread(tid)));
            }
        }
    }
}

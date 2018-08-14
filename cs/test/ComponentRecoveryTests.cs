// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using FASTER.core;
using System.Threading;

namespace FASTER.test.recovery
{

    [TestClass]
    public class ComponentRecoveryTests
    {
        [TestInitialize]
        public void Setup()
        {
        }

        [TestCleanup]
        public void TearDown()
        {
        }

        [TestMethod]
        public unsafe void MallocFixedPageSizeRecoveryTest()
        {
            int seed = 123;
            var rand1 = new Random(seed);
            IDevice device = new LocalStorageDevice("test_ofb.dat", false, false, true, true);
            var allocator = new MallocFixedPageSize<HashBucket>();

            //do something
            int numBucketsToAdd = 16 * MallocFixedPageSize<HashBucket>.PageSize;
            long[] logicalAddresses = new long[numBucketsToAdd];
            for (int i = 0; i < numBucketsToAdd; i++)
            {
                long logicalAddress = allocator.Allocate();
                logicalAddresses[i] = logicalAddress;
                var bucket = (HashBucket*)allocator.GetPhysicalAddress(logicalAddress);
                for (int j = 0; j < Constants.kOverflowBucketIndex; j++)
                {
                    bucket->bucket_entries[j] = rand1.Next();
                }
            }

            //issue call to checkpoint
            allocator.begin_checkpoint(device, 0, out ulong numBytesWritten);
            //wait until complete
            allocator.IsCheckpointCompleted(true);


            var recoveredAllocator = new MallocFixedPageSize<HashBucket>();
            //issue call to recover
            recoveredAllocator.begin_recovery(device, 0, numBucketsToAdd, numBytesWritten, out ulong numBytesRead);
            //wait until complete
            recoveredAllocator.IsRecoveryCompleted(true);

            Assert.IsTrue(numBytesWritten == numBytesRead);

            var rand2 = new Random(seed);
            for (int i = 0; i < numBucketsToAdd; i++)
            {
                var logicalAddress = logicalAddresses[i];
                var bucket = (HashBucket*)recoveredAllocator.GetPhysicalAddress(logicalAddress);
                for (int j = 0; j < Constants.kOverflowBucketIndex; j++)
                {
                    Assert.IsTrue(bucket->bucket_entries[j] == rand2.Next());
                }
            }
        }

        [TestMethod]
        public unsafe void TestFuzzyIndexRecovery()
        {
            int seed = 123;
            int size = 1 << 16;
            long numAdds = 1 << 18;

            IDevice ht_device = new LocalStorageDevice("ht.dat", false, false, true, true);
            IDevice ofb_device = new LocalStorageDevice("ofb.dat", false, false, true, true);

            var hash_table1 = new FASTERBase();
            hash_table1.Initialize(size, 512);

            //do something
            var bucket = default(HashBucket*);
            var slot = default(int);

            var keyGenerator1 = new Random(seed);
            var valueGenerator = new Random(seed + 1);
            for (int i = 0; i < numAdds; i++)
            {
                long key = keyGenerator1.Next();
                var hash = Utility.GetHashCode(key);
                var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                var entry = default(HashBucketEntry);
                hash_table1.FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);

                hash_table1.UpdateSlot(bucket, slot, entry.word, valueGenerator.Next(), out long found_word);
            }

            //issue checkpoint call
            hash_table1.TakeIndexFuzzyCheckpoint(0, ht_device, out ulong ht_num_bytes_written,
                ofb_device, out ulong ofb_num_bytes_written, out int num_ofb_buckets);

            //wait until complete
            hash_table1.IsIndexFuzzyCheckpointCompleted(true);

            var hash_table2 = new FASTERBase();
            hash_table2.Initialize(size, 512);

            //issue recover call
            hash_table2.RecoverFuzzyIndex(0, ht_device, ht_num_bytes_written, ofb_device, num_ofb_buckets, ofb_num_bytes_written);
            //wait until complete
            hash_table2.IsFuzzyIndexRecoveryComplete(true);

            //verify
            var keyGenerator2 = new Random(seed);

            var bucket1 = default(HashBucket*);
            var bucket2 = default(HashBucket*);
            var slot1 = default(int);
            var slot2 = default(int);

            var entry1 = default(HashBucketEntry);
            var entry2 = default(HashBucketEntry);
            for (int i = 0; i < 2 * numAdds; i++)
            {
                long key = keyGenerator2.Next();
                var hash = Utility.GetHashCode(key);
                var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                var exists1 = hash_table1.FindTag(hash, tag, ref bucket1, ref slot1, ref entry1);
                var exists2 = hash_table2.FindTag(hash, tag, ref bucket2, ref slot2, ref entry2);

                Assert.IsTrue(exists1 == exists2);

                if (exists1)
                {
                    Assert.IsTrue(entry1.word == entry2.word);
                }
            }
        }
    }
}

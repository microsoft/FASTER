// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using FASTER.core;
using System.Threading;
using NUnit.Framework;

namespace FASTER.test.recovery
{

    [TestFixture]
    internal class ComponentRecoveryTests
    {
        [Test]
        public unsafe void MallocFixedPageSizeRecoveryTest()
        {
            int seed = 123;
            var rand1 = new Random(seed);
            var device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\MallocFixedPageSizeRecoveryTest.dat", deleteOnClose: true);
            var allocator = new MallocFixedPageSize<HashBucket>();
            allocator.Acquire();

            //do something
            int numBucketsToAdd = 16 * allocator.GetPageSize();
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
            allocator.BeginCheckpoint(device, 0, out ulong numBytesWritten);
            //wait until complete
            allocator.IsCheckpointCompleted(true);

            allocator.Release();
            allocator.Dispose();

            var recoveredAllocator = new MallocFixedPageSize<HashBucket>();
            recoveredAllocator.Acquire();

            //issue call to recover
            recoveredAllocator.BeginRecovery(device, 0, numBucketsToAdd, numBytesWritten, out ulong numBytesRead);
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

            recoveredAllocator.Release();
            recoveredAllocator.Dispose();
        }

        [Test]
        public unsafe void TestFuzzyIndexRecovery()
        {
            int seed = 123;
            int size = 1 << 16;
            long numAdds = 1 << 18;

            IDevice ht_device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\TestFuzzyIndexRecoveryht.dat", deleteOnClose: true);
            IDevice ofb_device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\TestFuzzyIndexRecoveryofb.dat", deleteOnClose: true);

            var hash_table1 = new FasterBase();
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
                hash_table1.FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, 0);

                hash_table1.UpdateSlot(bucket, slot, entry.word, valueGenerator.Next(), out long found_word);
            }

            //issue checkpoint call
            hash_table1.TakeIndexFuzzyCheckpoint(0, ht_device, out ulong ht_num_bytes_written,
                ofb_device, out ulong ofb_num_bytes_written, out int num_ofb_buckets);

            //wait until complete
            hash_table1.IsIndexFuzzyCheckpointCompleted(true);

            var hash_table2 = new FasterBase();
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

            hash_table1.Free();
            hash_table2.Free();
        }
    }
}

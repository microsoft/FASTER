// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe partial class FasterBase
    {
        internal ICheckpointManager checkpointManager;
        internal bool disposeCheckpointManager;

        // Derived class exposed API
        internal void RecoverFuzzyIndex(IndexCheckpointInfo info)
        {
            var token = info.info.token;
            var ht_version = resizeInfo.version;

            if (state[ht_version].size != info.info.table_size)
                throw new FasterException($"Incompatible hash table size during recovery; allocated {state[ht_version].size} buckets, recovering {info.info.table_size} buckets");

            // Create devices to read from using Async API
            info.main_ht_device = checkpointManager.GetIndexDevice(token);

            BeginMainIndexRecovery(ht_version,
                             info.main_ht_device,
                             info.info.num_ht_bytes);

            var sectorSize = info.main_ht_device.SectorSize;
            var alignedIndexSize = (uint)((info.info.num_ht_bytes + (sectorSize - 1)) & ~(sectorSize - 1));

            overflowBucketsAllocator.Recover(info.main_ht_device, alignedIndexSize, info.info.num_buckets, info.info.num_ofb_bytes);

            // Wait until reading is complete
            IsFuzzyIndexRecoveryComplete(true);

            // close index checkpoint files appropriately
            info.main_ht_device.Dispose();

            // Delete all tentative entries!
            DeleteTentativeEntries();
        }

        internal void RecoverFuzzyIndex(int ht_version, IDevice device, ulong num_ht_bytes, IDevice ofbdevice, int num_buckets, ulong num_ofb_bytes)
        {
            BeginMainIndexRecovery(ht_version, device, num_ht_bytes);
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (uint)((num_ht_bytes + (sectorSize - 1)) & ~(sectorSize - 1));
            overflowBucketsAllocator.Recover(ofbdevice, alignedIndexSize, num_buckets, num_ofb_bytes);
        }

        internal bool IsFuzzyIndexRecoveryComplete(bool waitUntilComplete = false)
        {
            bool completed1 = IsMainIndexRecoveryCompleted(waitUntilComplete);
            bool completed2 = overflowBucketsAllocator.IsRecoveryCompleted(waitUntilComplete);
            return completed1 && completed2;
        }

        //Main Index Recovery Functions
        protected CountdownEvent mainIndexRecoveryEvent;

        private void BeginMainIndexRecovery(
                                int version,
                                IDevice device,
                                ulong num_bytes)
        {
            long totalSize = state[version].size * sizeof(HashBucket);

            int numChunks = 1;
            if (totalSize > uint.MaxValue)
            {
                numChunks = (int)Math.Ceiling((double)totalSize / (long)uint.MaxValue);
                numChunks = (int)Math.Pow(2, Math.Ceiling(Math.Log(numChunks, 2)));
            }

            uint chunkSize = (uint)(totalSize / numChunks);
            mainIndexRecoveryEvent = new CountdownEvent(numChunks);
            HashBucket* start = state[version].tableAligned;
 
            ulong numBytesRead = 0;
            for (int index = 0; index < numChunks; index++)
            {
                long chunkStartBucket = (long)start + (index * chunkSize);
                HashIndexPageAsyncReadResult result = default(HashIndexPageAsyncReadResult);
                result.chunkIndex = index;
                device.ReadAsync(numBytesRead, (IntPtr)chunkStartBucket, chunkSize, AsyncPageReadCallback, result);
                numBytesRead += chunkSize;
            }
            Debug.Assert(numBytesRead == num_bytes);
        }

        private bool IsMainIndexRecoveryCompleted(
                                        bool waitUntilComplete = false)
        {
            bool completed = mainIndexRecoveryEvent.IsSet;
            if (!completed && waitUntilComplete)
            {
                mainIndexRecoveryEvent.Wait();
                return true;
            }
            return completed;
        }

        private unsafe void AsyncPageReadCallback(uint errorCode, uint numBytes, object overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncPageReadCallback error: {0}", errorCode);
            }
            mainIndexRecoveryEvent.Signal();
        }

        internal void DeleteTentativeEntries()
        {
            HashBucketEntry entry = default(HashBucketEntry);

            int version = resizeInfo.version;
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                    {
                        entry.word = b.bucket_entries[bucket_entry];
                        if (entry.Tentative)
                            b.bucket_entries[bucket_entry] = 0;
                    }

                    if (b.bucket_entries[Constants.kOverflowBucketIndex] == 0) break;
                    b = *((HashBucket*)overflowBucketsAllocator.GetPhysicalAddress((b.bucket_entries[Constants.kOverflowBucketIndex])));
                }
            }
        }
    }
}

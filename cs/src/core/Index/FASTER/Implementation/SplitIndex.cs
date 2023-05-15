// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        private void SplitBuckets(long hash)
        {
            long masked_bucket_index = hash & state[1 - resizeInfo.version].size_mask;
            int offset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);
            SplitBuckets(offset);
        }

        private void SplitBuckets(int offset)
        {
            int numChunks = (int)(state[1 - resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk

            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new FasterException("Invalid number of chunks: " + numChunks);
            }
            for (int i = offset; i < offset + numChunks; i++)
            {
                if (0 == Interlocked.CompareExchange(ref splitStatus[i & (numChunks - 1)], 1, 0))
                {
                    long chunkSize = state[1 - resizeInfo.version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = state[1 - resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start0 = state[resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start1 = state[resizeInfo.version].tableAligned + state[1 - resizeInfo.version].size + ptr;

                    SplitChunk(src_start, dest_start0, dest_start1, chunkSize);

                    // split for chunk is done
                    splitStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref numPendingChunksToBeSplit) == 0)
                    {
                        // GC old version of hash table
                        state[1 - resizeInfo.version] = default;
                        overflowBucketsAllocatorResize.Dispose();
                        overflowBucketsAllocatorResize = null;
                        GlobalStateMachineStep(systemState);
                        return;
                    }
                    break;
                }
            }

            while (Interlocked.Read(ref splitStatus[offset & (numChunks - 1)]) == 1)
            {
                Thread.Yield();
            }
        }

        private void SplitChunk(
                    HashBucket* _src_start,
                    HashBucket* _dest_start0,
                    HashBucket* _dest_start1,
                    long chunkSize)
        {
            for (int i = 0; i < chunkSize; i++)
            {
                var src_start = _src_start + i;

                long* left = (long*)(_dest_start0 + i);
                long* right = (long*)(_dest_start1 + i);
                long* left_end = left + Constants.kOverflowBucketIndex;
                long* right_end = right + Constants.kOverflowBucketIndex;

                HashBucketEntry entry = default;
                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (Constants.kInvalidEntry == entry.word)
                        {
                            continue;
                        }

                        var logicalAddress = entry.Address;
                        long physicalAddress = 0;

                        if (entry.ReadCache && entry.AbsoluteAddress >= readcache.HeadAddress)
                            physicalAddress = readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                        else if (logicalAddress >= hlog.HeadAddress)
                            physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                        // It is safe to always use hlog instead of readcache for some calls such
                        // as GetKey and GetInfo
                        if (physicalAddress != 0)
                        {
                            var hash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));
                            if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == 0)
                            {
                                // Insert in left
                                if (left == left_end)
                                {
                                    var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *left = new_bucket_logical;
                                    left = (long*)new_bucket;
                                    left_end = left + Constants.kOverflowBucketIndex;
                                }

                                *left = entry.word;
                                left++;

                                // Insert previous address in right
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 1);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (right == right_end)
                                    {
                                        var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                        *right = new_bucket_logical;
                                        right = (long*)new_bucket;
                                        right_end = right + Constants.kOverflowBucketIndex;
                                    }

                                    *right = entry.word;
                                    right++;
                                }
                            }
                            else
                            {
                                // Insert in right
                                if (right == right_end)
                                {
                                    var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                    *right = new_bucket_logical;
                                    right = (long*)new_bucket;
                                    right_end = right + Constants.kOverflowBucketIndex;
                                }

                                *right = entry.word;
                                right++;

                                // Insert previous address in left
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(physicalAddress).PreviousAddress, 0);
                                if ((entry.Address != Constants.kInvalidAddress) && (entry.Address != Constants.kTempInvalidAddress))
                                {
                                    if (left == left_end)
                                    {
                                        var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                        *left = new_bucket_logical;
                                        left = (long*)new_bucket;
                                        left_end = left + Constants.kOverflowBucketIndex;
                                    }

                                    *left = entry.word;
                                    left++;
                                }
                            }
                        }
                        else
                        {
                            // Insert in both new locations

                            // Insert in left
                            if (left == left_end)
                            {
                                var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *left = new_bucket_logical;
                                left = (long*)new_bucket;
                                left_end = left + Constants.kOverflowBucketIndex;
                            }

                            *left = entry.word;
                            left++;

                            // Insert in right
                            if (right == right_end)
                            {
                                var new_bucket_logical = overflowBucketsAllocator.Allocate();
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(new_bucket_logical);
                                *right = new_bucket_logical;
                                right = (long*)new_bucket;
                                right_end = right + Constants.kOverflowBucketIndex;
                            }

                            *right = entry.word;
                            right++;
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)overflowBucketsAllocatorResize.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }

        private long TraceBackForOtherChainStart(long logicalAddress, int bit)
        {
            while (true)
            {
                HashBucketEntry entry = default;
                entry.Address = logicalAddress;
                if (entry.ReadCache)
                {
                    if (logicalAddress < readcache.HeadAddress)
                        break;
                    var physicalAddress = readcache.GetPhysicalAddress(logicalAddress);
                    var hash = comparer.GetHashCode64(ref readcache.GetKey(physicalAddress));
                    if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = readcache.GetInfo(physicalAddress).PreviousAddress;
                }
                else
                {
                    if (logicalAddress < hlog.HeadAddress)
                        break;
                    var physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    var hash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));
                    if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                    {
                        return logicalAddress;
                    }
                    logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                }
            }
            return logicalAddress;
        }
    }
}

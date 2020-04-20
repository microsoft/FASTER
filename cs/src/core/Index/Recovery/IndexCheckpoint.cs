// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterBase
    {
        // Derived class facing persistence API
        internal IndexCheckpointInfo _indexCheckpoint;

        internal void TakeIndexFuzzyCheckpoint()
        {
            var ht_version = resizeInfo.version;

            TakeMainIndexCheckpoint(ht_version,
                                    _indexCheckpoint.main_ht_device,
                                    out ulong ht_num_bytes_written);

            var sectorSize = _indexCheckpoint.main_ht_device.SectorSize;
            var alignedIndexSize = (uint)((ht_num_bytes_written + (sectorSize - 1)) & ~(sectorSize - 1));
            overflowBucketsAllocator.TakeCheckpoint(_indexCheckpoint.main_ht_device, alignedIndexSize, out ulong ofb_num_bytes_written);
            _indexCheckpoint.info.num_ht_bytes = ht_num_bytes_written;
            _indexCheckpoint.info.num_ofb_bytes = ofb_num_bytes_written;
        }

        internal void TakeIndexFuzzyCheckpoint(int ht_version, IDevice device,
                                            out ulong numBytesWritten, IDevice ofbdevice,
                                           out ulong ofbnumBytesWritten, out int num_ofb_buckets)
        {
            TakeMainIndexCheckpoint(ht_version, device, out numBytesWritten);
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (uint)((numBytesWritten + (sectorSize - 1)) & ~(sectorSize - 1));
            overflowBucketsAllocator.TakeCheckpoint(ofbdevice, alignedIndexSize, out ofbnumBytesWritten);
            num_ofb_buckets = overflowBucketsAllocator.GetMaxValidAddress();
        }

        internal bool IsIndexFuzzyCheckpointCompleted()
        {
            bool completed1 = IsMainIndexCheckpointCompleted();
            bool completed2 = overflowBucketsAllocator.IsCheckpointCompleted();
            return completed1 && completed2;
        }

        internal async ValueTask IsIndexFuzzyCheckpointCompletedAsync(CancellationToken token = default)
        {
            await IsMainIndexCheckpointCompletedAsync(token);
            await overflowBucketsAllocator.IsCheckpointCompletedAsync(token);
        }


        // Implementation of an asynchronous checkpointing scheme 
        // for main hash index of FASTER
        private int mainIndexCheckpointCallbackCount;
        private SemaphoreSlim mainIndexCheckpointSemaphore;

        private void TakeMainIndexCheckpoint(int tableVersion,
                                            IDevice device,
                                            out ulong numBytes)
        {
            BeginMainIndexCheckpoint(tableVersion, device, out numBytes);
        }

        private unsafe void BeginMainIndexCheckpoint(
                                           int version,
                                           IDevice device,
                                           out ulong numBytesWritten)
        {
            int numChunks = 1;
            long totalSize = state[version].size * sizeof(HashBucket);
            Debug.Assert(totalSize < (long)uint.MaxValue); // required since numChunks = 1

            uint chunkSize = (uint)(totalSize / numChunks);
            mainIndexCheckpointCallbackCount = numChunks;
            mainIndexCheckpointSemaphore = new SemaphoreSlim(0);
            HashBucket* start = state[version].tableAligned;
            
            numBytesWritten = 0;
            for (int index = 0; index < numChunks; index++)
            {
                long chunkStartBucket = (long)start + (index * chunkSize);
                HashIndexPageAsyncFlushResult result = default;
                result.chunkIndex = index;
                device.WriteAsync((IntPtr)chunkStartBucket, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                numBytesWritten += chunkSize;
            }
        }

        private bool IsMainIndexCheckpointCompleted()
        {
            return mainIndexCheckpointCallbackCount == 0;
        }

        private async ValueTask IsMainIndexCheckpointCompletedAsync(CancellationToken token = default)
        {
            if (mainIndexCheckpointCallbackCount > 0)
            {
                await mainIndexCheckpointSemaphore.WaitAsync(token);
                mainIndexCheckpointSemaphore.Release();
            }
        }

        private unsafe void AsyncPageFlushCallback(
                                            uint errorCode,
                                            uint numBytes,
                                            NativeOverlapped* overlap)
        {
            //Set the page status to flushed
            _ = (HashIndexPageAsyncFlushResult)Overlapped.Unpack(overlap).AsyncResult;

            try
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Completion Callback error, {0}", ex.Message);
            }
            finally
            {
                if (Interlocked.Decrement(ref mainIndexCheckpointCallbackCount) == 0)
                {
                    mainIndexCheckpointSemaphore.Release();
                }
                Overlapped.Free(overlap);
            }
        }
    }
}

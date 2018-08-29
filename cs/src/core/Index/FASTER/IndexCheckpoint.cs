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


    public unsafe partial class FASTERBase
    {
        // Derived class facing persistence API
        protected IndexCheckpointInfo _indexCheckpoint;

        protected void TakeIndexFuzzyCheckpoint()
        {
            var ht_version = resizeInfo.version;

            TakeMainIndexCheckpoint(ht_version,
                                    _indexCheckpoint.main_ht_device,
                                    out ulong ht_num_bytes_written);
            overflowBucketsAllocator.TakeCheckpoint(
                                    _indexCheckpoint.ofb_device,
                                    out ulong ofb_num_bytes_written);
            _indexCheckpoint.info.num_ht_bytes = ht_num_bytes_written;
            _indexCheckpoint.info.num_ofb_bytes = ofb_num_bytes_written;
        }

        internal void TakeIndexFuzzyCheckpoint(int ht_version, IDevice device,
                                            out ulong numBytesWritten, IDevice ofbdevice,
                                           out ulong ofbnumBytesWritten, out int num_ofb_buckets)
        {
            TakeMainIndexCheckpoint(ht_version, device, out numBytesWritten);
            overflowBucketsAllocator.TakeCheckpoint(ofbdevice, out ofbnumBytesWritten);
            num_ofb_buckets = overflowBucketsAllocator.GetMaxValidAddress();
        }

        internal bool IsIndexFuzzyCheckpointCompleted(bool waitUntilComplete = false)
        {
            bool completed1 = IsMainIndexCheckpointCompleted(waitUntilComplete);
            bool completed2 = overflowBucketsAllocator.IsCheckpointCompleted(waitUntilComplete);
            return completed1 && completed2;
        }


        // Implementation of an asynchronous checkpointing scheme 
        // for main hash index of FASTER
        protected CountdownEvent mainIndexCheckpointEvent;

        private void TakeMainIndexCheckpoint(int tableVersion,
                                            IDevice device,
                                            out ulong numBytes)
        {
            BeginMainIndexCheckpoint(tableVersion, device, out numBytes);
        }

        private void BeginMainIndexCheckpoint(
                                           int version,
                                           IDevice device,
                                           out ulong numBytesWritten)
        {
            long totalSize = state[version].size * sizeof(HashBucket);
            int numChunks = (int)(totalSize >> 23);
            if (numChunks == 0) numChunks = 1;

            uint chunkSize = (uint)(totalSize / numChunks);
            mainIndexCheckpointEvent = new CountdownEvent(1);
            HashBucket* start = state[version].tableAligned;
            int startNumChunks = 1;

            HashIndexPageAsyncFlushResult result = new HashIndexPageAsyncFlushResult
            {
                start = start,
                numChunks = numChunks,
                numIssued = startNumChunks,
                numFinished = 0,
                chunkSize = chunkSize,
                device = device
            };

            numBytesWritten = 0;

            for (int index = 0; index < startNumChunks; index++)
            {
                long chunkStartBucket = (long)start + (index * chunkSize);
                device.WriteAsync((IntPtr)chunkStartBucket, ((ulong)index) * chunkSize, chunkSize, AsyncPageFlushCallback, result);
            }
            numBytesWritten = ((ulong)numChunks) * chunkSize;
        }


        private bool IsMainIndexCheckpointCompleted(bool waitUntilComplete = false)
        {
            bool completed = mainIndexCheckpointEvent.IsSet;
            if (!completed && waitUntilComplete)
            {
                mainIndexCheckpointEvent.Wait();
                return true;
            }
            return completed;
        }

        private void AsyncPageFlushCallback(
                                            uint errorCode,
                                            uint numBytes,
                                            NativeOverlapped* overlap)
        {
            //Set the page status to flushed
            var result = (HashIndexPageAsyncFlushResult)Overlapped.Unpack(overlap).AsyncResult;

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
                if (Interlocked.Increment(ref result.numFinished) == result.numChunks)
                {
                    mainIndexCheckpointEvent.Signal();
                }
                else
                {
                    int nextChunk = Interlocked.Increment(ref result.numIssued) - 1;

                    if (nextChunk < result.numChunks)
                    {
                        long chunkStartBucket = (long)result.start + (nextChunk * result.chunkSize);
                        result.device.WriteAsync(
                            (IntPtr)chunkStartBucket,
                            ((ulong)nextChunk) * result.chunkSize,
                            result.chunkSize,
                            AsyncPageFlushCallback,
                            result);
                    }
                }
                Overlapped.Free(overlap);
            }
        }

    }

}

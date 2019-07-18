// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class StorageDeviceBase : IDevice
    {

        /// <summary>
        /// 
        /// </summary>
        public uint SectorSize { get; }

        /// <summary>
        /// 
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Returns the maximum capacity of the storage device, in number of bytes. 
        /// If returned -1, the storage device has no capacity limit. 
        /// </summary>
        public long Capacity { get; }

        public int StartSegment { get { return startSegment; } }

        public int EndSegment { get { return endSegment; } }

        public long SegmentSize { get { return segmentSize; } }

        /// <summary>
        /// Segment size
        /// </summary>
        protected long segmentSize;

        private int segmentSizeBits;
        private ulong segmentSizeMask;

        // A device may have internal in-memory data structure that requires epoch protection under concurrent access.
        protected LightEpoch epoch;

        private int startSegment, endSegment;

        /// <summary>
        /// Initializes a new StorageDeviceBase
        /// </summary>
        /// <param name="filename">Name of the file to use</param>
        /// <param name="sectorSize">The smallest unit of write of the underlying storage device (e.g. 512 bytes for a disk) </param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommondate, or CAPAPCITY_UNSPECIFIED if there is no such limit </param>
        public StorageDeviceBase(string filename, uint sectorSize, long capacity)
        {
            FileName = filename;        
            SectorSize = sectorSize;

            segmentSize = -1;
            segmentSizeBits = 64;
            segmentSizeMask = ~0UL;

            Capacity = capacity;
        }

        /// <summary>
        /// Initialize device
        /// </summary>
        /// <param name="segmentSize"></param>
        public virtual void Initialize(long segmentSize, LightEpoch epoch = null)
        {
            // TODO(Tianyu): Alternatively, we can adjust capacity based on the segment size: given a phsyical upper limit of capacity,
            // we only make use of (Capacity / segmentSize * segmentSize) many bytes. 
            Debug.Assert(Capacity == -1 || Capacity % segmentSize == 0, "capacity must be a multiple of segment sizes");
            this.segmentSize = segmentSize;
            this.epoch = epoch;
            if (!Utility.IsPowerOfTwo(segmentSize))
            {
                if (segmentSize != -1)
                    throw new Exception("Invalid segment size: " + segmentSize);
                segmentSizeBits = 64;
                segmentSizeMask = ~0UL;
            }
            else
            {
                segmentSizeBits = Utility.GetLogBase2((ulong)segmentSize);
                segmentSizeMask = (ulong)segmentSize - 1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            int segment = (int)(segmentSizeBits < 64 ? alignedDestinationAddress >> segmentSizeBits : 0);

            // If the device has bounded space, and we are writing a new segment, need to check whether an existing segment needs to be evicted. 
            if (Capacity != Devices.CAPACITY_UNSPECIFIED && Utility.MonotonicUpdate(ref endSegment, segment, out int oldEnd))
            {
                // Attempt to update the stored range until there are enough space on the tier to accomodate the current logTail
                int newStartSegment = endSegment - (int)(Capacity >> segmentSizeBits);
                // Assuming that we still have enough physical capacity to write another segment, even if delete does not immediately free up space.
                TruncateUntilSegmentAsync(newStartSegment, r => { }, null);
            }
            WriteAsync(
                alignedSourceAddress,
                segment,
                alignedDestinationAddress & segmentSizeMask,
                numBytesToWrite, callback, asyncResult);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            var segment = segmentSizeBits < 64 ? alignedSourceAddress >> segmentSizeBits : 0;

            ReadAsync(
                (int)segment,
                alignedSourceAddress & segmentSizeMask,
                alignedDestinationAddress,
                aligned_read_length, callback, asyncResult);
        }

        public abstract void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result);

        public virtual void RemoveSegment(int segment)
        {
            ManualResetEventSlim completionEvent = new ManualResetEventSlim(false);
            RemoveSegmentAsync(segment, r => completionEvent.Set(), null);
            completionEvent.Wait();
        }

        public virtual void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
        {
            // Reset begin range to at least toAddress
            if (!Utility.MonotonicUpdate(ref startSegment, toSegment, out int oldStart))
            {
                // If no-op, invoke callback and return immediately
                callback(result);
                return;
            }
            CountdownEvent countdown = new CountdownEvent(toSegment - oldStart);
            // This action needs to be epoch-protected because readers may be issuing reads to the deleted segment, unaware of the delete.
            // Because of earlier compare-and-swap, the caller has exclusive access to the range [oldStartSegment, newStartSegment), and there will
            // be no double deletes.
            epoch.BumpCurrentEpoch(() =>
            {
                for (int i = oldStart; i < toSegment; i++)
                {
                    RemoveSegmentAsync(i, r => {
                        if (countdown.Signal())
                        {
                            callback(r);
                            countdown.Dispose();
                        }
                    }, result);
                }
            });
        }

        public virtual void TruncateUntilSegment(int toSegment)
        {
            using (ManualResetEventSlim completionEvent = new ManualResetEventSlim(false))
            {
                TruncateUntilSegmentAsync(toSegment, r => completionEvent.Set(), null);
                completionEvent.Wait();
            }
        }

        public virtual void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
        {
            // Truncate only up to segment boundary if address is not aligned
            TruncateUntilSegmentAsync((int)toAddress >> segmentSizeBits, callback, result);
        }

        public virtual void TruncateUntilAddress(long toAddress)
        {
            using (ManualResetEventSlim completionEvent = new ManualResetEventSlim(false))
            {
                TruncateUntilAddressAsync(toAddress, r => completionEvent.Set(), null);
                completionEvent.Wait();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public abstract void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public abstract void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// 
        /// </summary>
        public abstract void Close();
    }
}

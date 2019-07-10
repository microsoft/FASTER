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

        /// <summary>
        /// Segment size
        /// </summary>
        protected long segmentSize;

        private int segmentSizeBits;
        private ulong segmentSizeMask;

        // A device may have internal in-memory data structure that requires epoch protection under concurrent access.
        protected LightEpoch epoch;

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
            var segment = segmentSizeBits < 64 ? alignedDestinationAddress >> segmentSizeBits : 0;
            WriteAsync(
                alignedSourceAddress,
                (int)segment,
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="toAddress"></param>
        public void DeleteAddressRange(long fromAddress, long toAddress)
        {
            var fromSegment = segmentSizeBits < 64 ? fromAddress >> segmentSizeBits : 0;
            var toSegment = segmentSizeBits < 64 ? toAddress >> segmentSizeBits : 0;
            DeleteSegmentRange((int)fromSegment, (int)toSegment);
        }

        private bool AlignedAtSegmentBoundary(long address)
        {
            return ((long)segmentSizeMask & address) == 0;
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
        /// <param name="fromSegment"></param>
        /// <param name="toSegment"></param>
        public virtual unsafe void DeleteSegmentRange(int fromSegment, int toSegment)
        {
            ManualResetEventSlim completionEvent = new ManualResetEventSlim(false);
            DeleteSegmentRangeAsync(fromSegment, toSegment, r =>
            {
                completionEvent.Set();
            }, null);
            completionEvent.Wait();
        }

        public abstract void DeleteSegmentRangeAsync(int fromSegment, int toSegment, AsyncCallback callback, IAsyncResult asyncResult);


        protected void UseSynchronousDeleteSegmentRangeForAsync(int fromSegment, int toSegment, AsyncCallback callback, IAsyncResult asyncResult)
        {
            DeleteSegmentRange(fromSegment, toSegment);
            // TODO(Tianyu): There is apparently no setters on IAsyncResult. Should I just pass this or do I need to set some states?
            // e.g. set CompletedSynchronously to true
            callback(asyncResult);
        }

        /// <summary>
        /// 
        /// </summary>
        public abstract void Close();
    }
}

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
        /// Segment size
        /// </summary>
        protected long segmentSize;

        private int segmentSizeBits;
        private ulong segmentSizeMask;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="sectorSize"></param>
        public StorageDeviceBase(string filename, uint sectorSize)
        {
            FileName = filename;        
            SectorSize = sectorSize;

            segmentSize = -1;
            segmentSizeBits = 64;
            segmentSizeMask = ~0UL;
        }

        /// <summary>
        /// Initialize device
        /// </summary>
        /// <param name="segmentSize"></param>
        public void Initialize(long segmentSize)
        {
            this.segmentSize = segmentSize;
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
        public abstract void DeleteSegmentRange(int fromSegment, int toSegment);

        /// <summary>
        /// 
        /// </summary>
        public abstract void Close();
    }
}

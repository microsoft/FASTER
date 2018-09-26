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
    public abstract class StorageDeviceBase : IDevice
    {
        public uint SectorSize { get; }
        public string FileName { get; }
        public long SegmentSize { get; }

        private readonly int segmentSizeBits;
        private readonly ulong segmentSizeMask;

        public StorageDeviceBase(
            string filename,  long segmentSize, uint sectorSize)
        {
            FileName = filename;
            SegmentSize = segmentSize;

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

            SectorSize = sectorSize;
        }

        public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            WriteAsync(
                alignedSourceAddress,
                (int)(alignedDestinationAddress >> segmentSizeBits),
                alignedDestinationAddress & segmentSizeMask,
                numBytesToWrite, callback, asyncResult);
        }

        public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            ReadAsync(
                (int)(alignedSourceAddress >> segmentSizeBits),
                alignedSourceAddress & segmentSizeMask,
                alignedDestinationAddress,
                aligned_read_length, callback, asyncResult);
        }

        public void DeleteAddressRange(long fromAddress, long toAddress)
        {
            DeleteSegmentRange(
                (int)(fromAddress >> segmentSizeBits),
                (int)(toAddress >> segmentSizeBits));
        }

        public abstract void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);
        public abstract void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult);
        public abstract void DeleteSegmentRange(int fromSegment, int toSegment);

        public abstract void Close();
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    public class SegmentedNullDevice : ISegmentedDevice
    {
        private readonly int _segmentSizeBits = 30;

        public SegmentedNullDevice(int sector_size = -1)
        {
        }

        public void DeleteSegmentRange(int fromSegment, int toSegment)
        {
        }

        public uint GetSectorSize()
        {
            return 512;
        }

        public long GetSegmentSize()
        {
            return 1L << _segmentSizeBits;
        }

        public unsafe void ReadAsync(int segmentId, ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            alignedSourceAddress = ((ulong)segmentId << _segmentSizeBits) | alignedSourceAddress;

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);
            ov_native->OffsetLow = unchecked((int)(alignedSourceAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedSourceAddress >> 32) & 0xFFFFFFFF));

            callback(0, aligned_read_length, ov_native);
        }

        public unsafe void WriteAsync(IntPtr alignedSourceAddress, int segmentId, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            alignedDestinationAddress = ((ulong)segmentId << _segmentSizeBits) | alignedDestinationAddress;

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);

            ov_native->OffsetLow = unchecked((int)(alignedDestinationAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedDestinationAddress >> 32) & 0xFFFFFFFF));

            callback(0, numBytesToWrite, ov_native);
        }
    }
}

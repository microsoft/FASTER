// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// 
    /// </summary>
    public class NullDevice : StorageDeviceBase
    {
        /// <summary>
        /// 
        /// </summary>
        public NullDevice() : base("null", 1L << 30, 512)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public override unsafe void ReadAsync(int segmentId, ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            alignedSourceAddress = ((ulong)segmentId << 30) | alignedSourceAddress;

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);
            ov_native->OffsetLow = unchecked((int)(alignedSourceAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedSourceAddress >> 32) & 0xFFFFFFFF));

            callback(0, aligned_read_length, ov_native);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public override unsafe void WriteAsync(IntPtr alignedSourceAddress, int segmentId, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            alignedDestinationAddress = ((ulong)segmentId << 30) | alignedDestinationAddress;

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);

            ov_native->OffsetLow = unchecked((int)(alignedDestinationAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedDestinationAddress >> 32) & 0xFFFFFFFF));

            callback(0, numBytesToWrite, ov_native);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fromSegment"></param>
        /// <param name="toSegment"></param>
        public override void DeleteSegmentRange(int fromSegment, int toSegment)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Close()
        {
        }
    }
}

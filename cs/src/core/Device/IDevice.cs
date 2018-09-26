// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    public interface IDevice
    {
        uint SectorSize { get; }
        long SegmentSize { get; }
        string FileName { get; }

        /* Segmented addressing API */
        void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);
        void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult);
        void DeleteSegmentRange(int fromSegment, int toSegment);

        /* Direct addressing API */
        void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);
        void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult);
        void DeleteAddressRange(long fromAddress, long toAddress);

        /* Close */
        void Close();
    }
}

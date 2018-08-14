// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    public interface ISegmentedDevice
    {
        uint GetSectorSize();
        long GetSegmentSize();

        void WriteAsync(IntPtr alignedSourceAddress, int segmentId, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);
        void ReadAsync(int segmentId, ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// Delete range of segments from fromSegment (inclusive) to toSegment (exclusive)
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="toAddress"></param>
        void DeleteSegmentRange(int fromSegment, int toSegment);
    }
}

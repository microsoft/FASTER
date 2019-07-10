// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Interface for devices
    /// </summary>
    public interface IDevice
    {
        /// <summary>
        /// Size of sector
        /// </summary>
        uint SectorSize { get; }

        /// <summary>
        /// Name of device
        /// </summary>
        string FileName { get; }

        /// <summary>
        /// Returns the maximum capacity of the storage device, in number of bytes. 
        /// If returned CAPACITY_UNSPECIFIED, the storage device has no specfied capacity limit. 
        /// </summary>
        long Capacity { get; }

        /// <summary>
        /// Initialize device. This function is used to pass optional information that may only be known after
        /// FASTER initialization (whose constructor takes in IDevice upfront). Implementation are free to ignore
        /// information if it does not need the supplied information.
        /// 
        /// This is a bit of a hack. 
        /// </summary>
        /// <param name="segmentSize"></param>
        /// <param name="epoch">
        /// The instance of the epoch protection framework to use, if needed
        /// </param>
        void Initialize(long segmentSize, LightEpoch epoch = null);

        
        /* Segmented addressing API */
        /// <summary>
        /// Write
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// Read
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult);

        void DeleteSegmentRangeAsync(int fromSegment, int toSegment, AsyncCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// Delete segment range
        /// </summary>
        /// <param name="fromSegment"></param>
        /// <param name="toSegment"></param>
        void DeleteSegmentRange(int fromSegment, int toSegment);

        /* Direct addressing API */

        /// <summary>
        /// Write
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// Read
        /// </summary>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="alignedDestinationAddress"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// Delete address range
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="toAddress"></param>
        void DeleteAddressRange(long fromAddress, long toAddress);

        /* Close */

        /// <summary>
        /// Close
        /// </summary>
        void Close();
    }
}

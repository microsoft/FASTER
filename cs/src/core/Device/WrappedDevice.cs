// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class WrappedDevice : IDevice
    {
        private readonly ISegmentedDevice _device;
        private readonly long _segmentSize;
        private readonly int _segmentSizeBits;
        private readonly long _segmentSizeMask;

        public WrappedDevice(ISegmentedDevice device)
        {
            _device = device;
            _segmentSize = device.GetSegmentSize();

            if (!Utility.IsPowerOfTwo(_segmentSize))
                throw new Exception("Invalid segment size");

            _segmentSizeBits = Utility.GetLogBase2((ulong)_segmentSize);
            _segmentSizeMask = _segmentSize - 1;
        }

        public ISegmentedDevice GetUnderlyingDevice()
        {
            return _device;
        }

        public uint GetSectorSize()
        {
            return _device.GetSectorSize();
        }

        public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            _device.WriteAsync(
                alignedSourceAddress, 
                (int)(alignedDestinationAddress >> _segmentSizeBits), 
                (ulong)((long)alignedDestinationAddress & _segmentSizeMask), 
                numBytesToWrite, callback, asyncResult);
        }

        public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            _device.ReadAsync(
                (int)(alignedSourceAddress >> _segmentSizeBits), 
                (ulong)((long)alignedSourceAddress & _segmentSizeMask), alignedDestinationAddress, 
                aligned_read_length, callback, asyncResult);
        }

        public void DeleteAddressRange(long fromAddress, long toAddress)
        {
            _device.DeleteSegmentRange(
                (int)(fromAddress >> _segmentSizeBits), 
                (int)(toAddress >> _segmentSizeBits));
        }
    }
}

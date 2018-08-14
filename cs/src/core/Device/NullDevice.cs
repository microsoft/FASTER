// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    public class NullDevice : IDevice
    {
        public NullDevice(int sector_size = -1)
        {
        }

        public void DeleteAddressRange(long fromAddress, long toAddress)
        {
        }

        public uint GetSectorSize()
        {
            return 512;
        }

        public unsafe void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);
            ov_native->OffsetLow = unchecked((int)(alignedSourceAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedSourceAddress >> 32) & 0xFFFFFFFF));

            callback(0, aligned_read_length, ov_native);
        }

        public unsafe void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);

            ov_native->OffsetLow = unchecked((int)(alignedDestinationAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedDestinationAddress >> 32) & 0xFFFFFFFF));

            callback(0, numBytesToWrite, ov_native);
        }
    }
}

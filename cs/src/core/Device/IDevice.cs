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
    public interface IDevice
    {
        uint GetSectorSize();
        void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult);
        void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult);

        /// <summary>
        /// Delete range of addresses from fromAddress (inclusive) to toAddress (exclusive)
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="toAddress"></param>
        void DeleteAddressRange(long fromAddress, long toAddress);
    }
}

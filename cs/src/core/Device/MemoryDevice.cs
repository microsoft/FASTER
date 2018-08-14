// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public unsafe class MemoryDevice<T> : IDevice
    {
        // Epoch information
        public LightEpoch epoch;

        // Record size and pinning
        private readonly bool IsPinned;
        private readonly int PrivateRecordSize;
        private static bool ForceUnpinnedAllocation = false;

        private readonly IDevice backingDevice;
        private readonly int sectorSize;

        // Constants for each page in the log
        public const int PageSizeBits = 22;
        public const int PageSize = 1 << PageSizeBits;
        public const int PageSizeMask = PageSize - 1;
        public readonly int AlignedPageSizeBytes;

        // Constants for circular buffer representing tail of log
        private const int BufferSizeBits = 14;
        public const int BufferSize = 1 << BufferSizeBits;
        public const int BufferSizeMask = BufferSize - 1;

        // Circular buffer definition
        private T[][] values = new T[BufferSize][];
        private GCHandle[] handles = new GCHandle[BufferSize];
        private IntPtr[] pointers = new IntPtr[BufferSize];

        public MemoryDevice()
        {
            this.backingDevice = new NullDevice();
            sectorSize = (int)backingDevice.GetSectorSize();
            epoch = LightEpoch.Instance;

            if (ForceUnpinnedAllocation)
            {
                IsPinned = false;
            }
            else
            {
                IsPinned = true;
                try
                {
                    var tmp = new T[1];
                    var h = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                    var p = h.AddrOfPinnedObject();
                    PrivateRecordSize = Marshal.SizeOf(tmp[0]);
                    AlignedPageSizeBytes = (((PrivateRecordSize * PageSize) + (sectorSize - 1)) & ~(sectorSize - 1));
                }
                catch (Exception)
                {
                    IsPinned = false;
                }
            }
        }
        
        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        protected void Free()
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (IsPinned && (handles[i].IsAllocated)) handles[i].Free();
                values[i] = null;
            }
            handles = null;
            pointers = null;
            values = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & PageSizeMask);

            // Global page address
            long page = (logicalAddress >> PageSizeBits);

            // Index of page within the circular buffer
            int pageIndex = (int)(page % BufferSize);

            return ((long)pointers[pageIndex]) + (long)offset * PrivateRecordSize;
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void AllocatePage(int index)
        {
            if (IsPinned)
            {
                var adjustedSize = PageSize + (int)Math.Ceiling(2 * sectorSize / PrivateRecordSize * 1.0);
                T[] tmp = new T[adjustedSize];
#if !(CALLOC)
                Array.Clear(tmp, 0, adjustedSize);
#endif
                handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                long p = (long)handles[index].AddrOfPinnedObject();
                pointers[index] = (IntPtr)((p + (sectorSize - 1)) & ~(sectorSize - 1));
                values[index] = tmp;
            }
            else
            {
                T[] tmp = new T[PageSize];
#if !(CALLOC)
                Array.Clear(tmp, 0, tmp.Length);
#endif
                values[index] = tmp;
            }
        }
        
        public uint GetSectorSize()
        {
            return backingDevice.GetSectorSize();
        }

        public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            long logicalAddress = (long)alignedDestinationAddress;
            long page = logicalAddress >> PageSizeBits;
            int pageIndex = (int) (page % BufferSize);
            Debug.Assert(page == pageIndex);

            if (values[pageIndex] == null)
            {
                // Allocate a new page
                AllocatePage(pageIndex);
            }
            else
            {
                //Clear an old used page
                Array.Clear(values[pageIndex], 0, values[pageIndex].Length);
            }

            long physicalAddress = GetPhysicalAddress(logicalAddress);
            Utility.Copy((byte*)alignedSourceAddress, (byte*)physicalAddress, (int)numBytesToWrite);

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);

            ov_native->OffsetLow = unchecked((int)(alignedDestinationAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedDestinationAddress >> 32) & 0xFFFFFFFF));

            callback(0, numBytesToWrite, ov_native);
        }

        public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            long logicalAddress = (long)alignedSourceAddress;
            long page = logicalAddress >> PageSizeBits;
            int pageIndex = (int)(page % BufferSize);
            Debug.Assert(page == pageIndex);

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ov_native = ov.UnsafePack(callback, IntPtr.Zero);
            ov_native->OffsetLow = unchecked((int)(alignedSourceAddress & 0xFFFFFFFF));
            ov_native->OffsetHigh = unchecked((int)((alignedSourceAddress >> 32) & 0xFFFFFFFF));

            if (values[pageIndex] == null)
            {
                callback(2, 0, ov_native);
            }
            else
            {
                long physicalAddress = GetPhysicalAddress(logicalAddress);
                Utility.Copy((byte*)physicalAddress, (byte*)alignedDestinationAddress, (int)aligned_read_length);
                callback(0, aligned_read_length, ov_native);
            }
        }

        public void DeleteAddressRange(long fromAddress, long toAddress)
        {
        }
    }
}

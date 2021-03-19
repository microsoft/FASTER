// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    internal sealed class DeltaLogIterator : ScanIteratorBase, IDisposable
    {
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly BlittableFrame frame;
        private bool disposed = false;
        const int headerSize = 12;

        /// <summary>
        /// Constructor
        /// </summary>
        public DeltaLogIterator(IDevice deltaLogDevice, int logPageSizeBits)
            : base(Constants.kFirstValidAddress, deltaLogDevice.GetFileSize(0), ScanBufferingMode.SinglePageBuffering, default, logPageSizeBits)
        {
            this.allocator = new BlittableAllocator<Empty, byte>(new
                LogSettings { LogDevice = deltaLogDevice, MemorySizeBits = logPageSizeBits + 2, PageSizeBits = logPageSizeBits }, null);
            deltaLogDevice.Initialize(-1);
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, 1 << logPageSizeBits, allocator.GetDeviceSectorSize());
        }

        /// <summary>
        /// Dispose the iterator
        /// </summary>
        public override void Dispose()
        {
            if (!disposed)
            {
                base.Dispose();

                // Dispose/unpin the frame from memory
                frame?.Dispose();
                disposed = true;
            }
        }

        internal override void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed, long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => allocator.AsyncReadPagesFromDeviceToFrame(readPageStart, numPages, untilAddress, AsyncReadPagesCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice, cts);

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                var result = (PageAsyncReadResult<Empty>)context;

                if (errorCode != 0)
                {
                    Trace.TraceError("AsyncReadPagesCallback error: {0}", errorCode);
                    result.cts?.Cancel();
                }

                if (result.freeBuffer1 != null)
                {
                    if (errorCode == 0)
                        allocator.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                    result.freeBuffer1.Return();
                    result.freeBuffer1 = null;
                }

                if (errorCode == 0)
                    result.handle?.Signal();

                Interlocked.MemoryBarrier();
            }
            catch when (disposed) { }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long Align(long length)
        {
            return (length + 511) & ~511;
        }

        public void Reset()
        {
            currentAddress = nextAddress = 0;
        }

        public unsafe bool GetNext(out long physicalAddress, out int entryLength)
        {
            while (true)
            {
                physicalAddress = 0;
                entryLength = 0;
                currentAddress = nextAddress;

                var _currentPage = currentAddress >> allocator.LogPageSizeBits;
                var _currentFrame = _currentPage % frameSize;
                var _currentOffset = currentAddress & allocator.PageSizeMask;
                var _headAddress = long.MaxValue;

                if (disposed)
                    return false;

                if (currentAddress >= endAddress)
                    return false;

                var _endAddress = endAddress;

                if (BufferAndLoad(currentAddress, _currentPage, _currentFrame, _headAddress, _endAddress))
                    continue;
                physicalAddress = frame.GetPhysicalAddress(_currentFrame, _currentOffset);

                // Get and check entry length
                entryLength = Utility.GetBlockLength((byte*)physicalAddress);
                if (entryLength == 0)
                {
                    // We are likely at end of page, skip to next
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;

                    Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _);

                    if (0 != Utility.GetBlockChecksum((byte*)physicalAddress))
                    {
                        return false;
                    }
                    else
                        continue;
                }

                int recordSize = (int)(Align(_currentOffset + headerSize + entryLength) - _currentOffset);
                if (entryLength < 0 || (_currentOffset + recordSize > allocator.PageSize))
                {
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                    {
                        return false;
                        throw new FasterException("Invalid length of record found: " + entryLength + " at address " + currentAddress + ", skipping page");
                    }
                    else
                        continue;
                }

                // Verify checksum if needed
                if (!Utility.VerifyBlockChecksum((byte*)physicalAddress, entryLength))
                {
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                    {
                        return false;
                    }
                    else
                        continue;
                }
                physicalAddress += headerSize;

                if ((currentAddress & allocator.PageSizeMask) + recordSize == allocator.PageSize)
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                else
                    currentAddress += recordSize;

                if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out long oldCurrentAddress))
                {
                    currentAddress = oldCurrentAddress;
                    return true;
                }
            }
        }
    }
}

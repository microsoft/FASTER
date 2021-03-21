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
    internal sealed class DeltaLog : ScanIteratorBase, IDisposable
    {
        const int headerSize = 12;
        readonly IDevice deltaLogDevice;
        readonly BlittableFrame frame;
        bool disposed = false;
        readonly int LogPageSizeBits;
        readonly int PageSize;
        readonly int PageSizeMask;
        readonly int AlignedPageSizeBytes;
        readonly int sectorSize;

        /// <summary>
        /// Constructor
        /// </summary>
        public DeltaLog(IDevice deltaLogDevice, int logPageSizeBits, long tailAddress)
            : base(Constants.kFirstValidAddress, tailAddress >= 0 ? tailAddress : deltaLogDevice.GetFileSize(0), ScanBufferingMode.SinglePageBuffering, default, logPageSizeBits)
        {
            LogPageSizeBits = logPageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;
            this.deltaLogDevice = deltaLogDevice;
            deltaLogDevice.Initialize(-1);
            sectorSize = (int)deltaLogDevice.SectorSize;
            if (frameSize > 0 && endAddress > 0)
                frame = new BlittableFrame(frameSize, 1 << logPageSizeBits, sectorSize);
            AlignedPageSizeBytes = (PageSize + (sectorSize - 1)) & ~(sectorSize - 1);
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
        {
            IDevice usedDevice = deltaLogDevice;
            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.frame[pageIndex] == null)
                {
                    frame.Allocate(pageIndex);
                }
                else
                {
                    frame.Clear(pageIndex);
                }
                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context,
                    handle = completed,
                    frame = frame
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                uint readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = (AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask));

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                usedDevice.ReadAsync(offsetInFile, (IntPtr)frame.pointers[pageIndex], readLength, AsyncReadPagesCallback, asyncResult);
            }
        }

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
                Debug.Assert(result.freeBuffer1 == null);

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

        public unsafe void Apply<Key, Value>(AllocatorBase<Key, Value> hlog, long startLogicalAddress, long endLogicalAddress, ref int version)
        {
            nextAddress = beginAddress;
            while (GetNext(out long physicalAddress, out int entryLength))
            {
                long endAddress = physicalAddress + entryLength;
                while (physicalAddress < endAddress)
                {
                    long address = *(long*)physicalAddress;
                    physicalAddress += sizeof(long);
                    int size = *(int*)physicalAddress;
                    physicalAddress += sizeof(int);
                    if (address >= startLogicalAddress && address < endLogicalAddress)
                    {
                        var destination = hlog.GetPhysicalAddress(address);
                        Buffer.MemoryCopy((void*)physicalAddress, (void*)destination, size, size);
                        version = hlog.GetInfo(destination).Version;
                    }
                    physicalAddress += size;
                }
                var alignedEntryLength = (entryLength + (sectorSize - 1)) & ~(sectorSize - 1);
            }
        }


        public unsafe bool GetNext(out long physicalAddress, out int entryLength)
        {
            while (true)
            {
                physicalAddress = 0;
                entryLength = 0;
                currentAddress = nextAddress;

                var _currentPage = currentAddress >> LogPageSizeBits;
                var _currentFrame = _currentPage % frameSize;
                var _currentOffset = currentAddress & PageSizeMask;
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
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        return false;
                    else
                        continue;
                }

                int recordSize = (int)(Align(_currentOffset + headerSize + entryLength) - _currentOffset);
                if (entryLength < 0 || (_currentOffset + recordSize > PageSize))
                {
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        return false;
                    else
                        continue;
                }

                // Verify checksum
                if (!Utility.VerifyBlockChecksum((byte*)physicalAddress, entryLength))
                {
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        return false;
                    else
                        continue;
                }
                physicalAddress += headerSize;

                if ((currentAddress & PageSizeMask) + recordSize == PageSize)
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
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

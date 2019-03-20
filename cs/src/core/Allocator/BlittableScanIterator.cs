// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Diagnostics;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public class BlittableScanIterator<Key, Value> : IFasterScanIterator<Key, Value>
        where Key : new()
        where Value : new()
    {
        private readonly int frameSize;
        private readonly BlittableAllocator<Key, Value> hlog;
        private readonly long beginAddress, endAddress;
        private readonly BlittableFrame frame;
        private readonly CountdownEvent[] loaded;

        private bool first = true;
        private long currentAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        public unsafe BlittableScanIterator(BlittableAllocator<Key, Value> hlog, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
        {
            this.hlog = hlog;
            this.beginAddress = beginAddress;
            this.endAddress = endAddress;
            currentAddress = beginAddress;

            if (scanBufferingMode == ScanBufferingMode.SinglePageBuffering)
                frameSize = 1;
            else
                frameSize = 2;

            frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
            loaded = new CountdownEvent[frameSize];

            var frameNumber = (currentAddress >> hlog.LogPageSizeBits) % frameSize;
            hlog.AsyncReadPagesFromDeviceToFrame
                (currentAddress >> hlog.LogPageSizeBits,
                1, AsyncReadPagesCallback, Empty.Default,
                frame, out loaded[frameNumber]);
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool GetNext(out Key key, out Value value)
        {
            key = default(Key);
            value = default(Value);

            while (true)
            {
                // Check for boundary conditions
                if (currentAddress >= endAddress)
                {
                    return false;
                }

                if (currentAddress < hlog.BeginAddress)
                {
                    throw new Exception("Iterator address is less than log BeginAddress " + hlog.BeginAddress);
                }

                var currentPage = currentAddress >> hlog.LogPageSizeBits;
                var currentFrame = currentPage % frameSize;
                var offset = currentAddress & hlog.PageSizeMask;

                if (currentAddress < hlog.HeadAddress)
                    BufferAndLoad(currentAddress, currentPage, currentFrame);

                var recordSize = hlog.GetRecordSize(hlog.GetPhysicalAddress(currentAddress));
                // Check if record fits on page, if not skip to next page
                if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                {
                    currentAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                    continue;
                }


                if (currentAddress >= hlog.HeadAddress)
                {
                    // Read record from cached page memory
                    var _physicalAddress = hlog.GetPhysicalAddress(currentAddress);

                    if (hlog.GetInfo(_physicalAddress).Invalid)
                    {
                        currentAddress += hlog.GetRecordSize(_physicalAddress);
                        continue;
                    }

                    key = hlog.GetKey(_physicalAddress);
                    value = hlog.GetValue(_physicalAddress);
                    currentAddress += hlog.GetRecordSize(_physicalAddress);
                    return true;
                }

                var physicalAddress = frame.GetPhysicalAddress(currentFrame, offset);

                if (hlog.GetInfo(physicalAddress).Invalid)
                {
                    currentAddress += hlog.GetRecordSize(physicalAddress);
                    continue;
                }

                key = hlog.GetKey(physicalAddress);
                value = hlog.GetValue(physicalAddress);
                currentAddress += hlog.GetRecordSize(physicalAddress);
                return true;
            }
        }

        /// <summary>
        /// Dispose the iterator
        /// </summary>
        public void Dispose()
        {
            frame.Dispose();
        }

        private unsafe void BufferAndLoad(long currentAddress, long currentPage, long currentFrame)
        {
            if (first || (currentAddress & hlog.PageSizeMask) == 0)
            {
                // Prefetch pages based on buffering mode
                if (frameSize == 1)
                {
                    if (!first)
                    {
                        hlog.AsyncReadPagesFromDeviceToFrame(currentAddress >> hlog.LogPageSizeBits, 1, AsyncReadPagesCallback, Empty.Default, frame, out loaded[currentFrame]);
                    }
                }
                else
                {
                    var endPage = endAddress >> hlog.LogPageSizeBits;
                    if ((endPage > currentPage) &&
                        ((endPage > currentPage + 1) || ((endAddress & hlog.PageSizeMask) != 0)))
                    {
                        hlog.AsyncReadPagesFromDeviceToFrame(1 + (currentAddress >> hlog.LogPageSizeBits), 1, AsyncReadPagesCallback, Empty.Default, frame, out loaded[(currentPage + 1) % frameSize]);
                    }
                }
                first = false;
            }
            loaded[currentFrame].Wait();
        }

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            var result = (PageAsyncReadResult<Empty>)Overlapped.Unpack(overlap).AsyncResult;

            if (result.freeBuffer1 != null)
            {
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (result.handle != null)
            {
                result.handle.Signal();
            }

            Interlocked.MemoryBarrier();
            Overlapped.Free(overlap);
        }
    }
}



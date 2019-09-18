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
    public class FasterLogScanIterator : IDisposable
    {
        private readonly int frameSize;
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly long endAddress;
        private readonly BlittableFrame frame;
        private readonly CountdownEvent[] loaded;
        private readonly long[] loadedPage;
        private readonly LightEpoch epoch;

        private long currentAddress, nextAddress;
        

        /// <summary>
        /// Current address
        /// </summary>
        public long CurrentAddress => currentAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch"></param>
        internal unsafe FasterLogScanIterator(BlittableAllocator<Empty, byte> hlog, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, LightEpoch epoch)
        {
            this.allocator = hlog;
            this.epoch = epoch;

            if (beginAddress == 0)
                beginAddress = hlog.GetFirstValidLogicalAddress(0);

            this.endAddress = endAddress;
            currentAddress = -1;
            nextAddress = beginAddress;

            if (scanBufferingMode == ScanBufferingMode.SinglePageBuffering)
                frameSize = 1;
            else if (scanBufferingMode == ScanBufferingMode.DoublePageBuffering)
                frameSize = 2;
            else if (scanBufferingMode == ScanBufferingMode.NoBuffering)
            {
                frameSize = 0;
                return;
            }

            frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
            loaded = new CountdownEvent[frameSize];
            loadedPage = new long[frameSize];
            for (int i = 0; i < frameSize; i++)
                loadedPage[i] = -1;

        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public unsafe bool GetNext(out Span<byte> entry)
        {
            currentAddress = nextAddress;
            while (true)
            {
                // Check for boundary conditions
                if (currentAddress < allocator.BeginAddress)
                {
                    Debug.WriteLine("Iterator address is less than log BeginAddress " + allocator.BeginAddress + ", adjusting iterator address");
                    currentAddress = allocator.BeginAddress;
                }

                if ((currentAddress >= endAddress) || (currentAddress >= allocator.ReadOnlyAddress))
                {
                    entry = default(Span<byte>);
                    return false;
                }


                if (frameSize == 0 && currentAddress < allocator.HeadAddress)
                {
                    throw new Exception("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                var currentPage = currentAddress >> allocator.LogPageSizeBits;
                var offset = currentAddress & allocator.PageSizeMask;

                var headAddress = allocator.HeadAddress;
                var physicalAddress = default(long);

                if (currentAddress < headAddress)
                {
                    BufferAndLoad(currentAddress, currentPage, currentPage % frameSize);
                    physicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
                }
                else
                {
                    epoch.Resume();
                    headAddress = allocator.HeadAddress;
                    if (currentAddress < headAddress) // rare case
                    {
                        epoch.Suspend();
                        continue;
                    }

                    physicalAddress = allocator.GetPhysicalAddress(currentAddress);
                }

                // Check if record fits on page, if not skip to next page
                int length = *(int*)physicalAddress;
                int recordSize = 4;
                if (length > 0)
                    recordSize += length;
                if ((currentAddress & allocator.PageSizeMask) + recordSize > allocator.PageSize)
                {
                    if (currentAddress >= headAddress)
                        epoch.Suspend();
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    continue;
                }

                if (length == 0)
                {
                    if (currentAddress >= headAddress)
                        epoch.Suspend();
                    currentAddress += recordSize;
                    continue;
                }

                entry = new Span<byte>((void*)(physicalAddress + 4), length);
                if (currentAddress >= headAddress)
                {
                    // Have to copy out bytes within epoch protection in 
                    // this case because this is a shared buffer
                    var _entry = new byte[length];
                    entry.CopyTo(_entry);
                    entry = _entry;
                    epoch.Suspend();
                }
                nextAddress = currentAddress + recordSize;
                return true;
            }
        }

        /// <summary>
        /// Dispose the iterator
        /// </summary>
        public void Dispose()
        {
            frame?.Dispose();
        }

        private unsafe void BufferAndLoad(long currentAddress, long currentPage, long currentFrame)
        {
            if (loadedPage[currentFrame] != currentPage)
            {
                if (loadedPage[currentFrame] != -1)
                    loaded[currentFrame].Wait(); // Ensure we have completed ongoing load
                allocator.AsyncReadPagesFromDeviceToFrame(currentAddress >> allocator.LogPageSizeBits, 1, endAddress, AsyncReadPagesCallback, Empty.Default, frame, out loaded[currentFrame]);
                loadedPage[currentFrame] = currentAddress >> allocator.LogPageSizeBits;
            }

            if (frameSize == 2)
            {
                currentPage++;
                currentFrame = (currentFrame + 1) % frameSize;

                if (loadedPage[currentFrame] != currentPage)
                {
                    if (loadedPage[currentFrame] != -1)
                        loaded[currentFrame].Wait(); // Ensure we have completed ongoing load
                    allocator.AsyncReadPagesFromDeviceToFrame(1 + (currentAddress >> allocator.LogPageSizeBits), 1, endAddress, AsyncReadPagesCallback, Empty.Default, frame, out loaded[currentFrame]);
                    loadedPage[currentFrame] = 1 + (currentAddress >> allocator.LogPageSizeBits);
                }
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
                allocator.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
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



// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public class FasterLogScanIterator : IDisposable
    {
        private readonly int frameSize;
        private readonly FasterLog fasterLog;
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly long endAddress;
        private readonly BlittableFrame frame;
        private readonly CountdownEvent[] loaded;
        private readonly long[] loadedPage;
        private readonly LightEpoch epoch;
        private readonly GetMemory getMemory;
        private readonly int headerSize;
        private long currentAddress, nextAddress;
        
        /// <summary>
        /// Current address
        /// </summary>
        public long CurrentAddress => currentAddress;

        /// <summary>
        /// Next address
        /// </summary>
        public long NextAddress => nextAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fasterLog"></param>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch"></param>
        /// <param name="headerSize"></param>
        /// <param name="getMemory"></param>
        internal unsafe FasterLogScanIterator(FasterLog fasterLog, BlittableAllocator<Empty, byte> hlog, long beginAddress, long endAddress, GetMemory getMemory, ScanBufferingMode scanBufferingMode, LightEpoch epoch, int headerSize)
        {
            this.fasterLog = fasterLog;
            this.allocator = hlog;
            this.getMemory = getMemory;
            this.epoch = epoch;
            this.headerSize = headerSize;

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
        /// Wait for iteration to be ready to continue
        /// </summary>
        /// <returns></returns>
        public async ValueTask WaitAsync()
        {
            while (true)
            {
                var commitTask = fasterLog.CommitTask;
                if (nextAddress >= fasterLog.CommittedUntilAddress)
                    await commitTask;
                else
                    break;
            }
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(out byte[] entry, out int entryLength)
        {
            if (GetNextInternal(out long physicalAddress, out entryLength, out bool epochTaken))
            {
                if (getMemory != null)
                {
                    // Use user delegate to allocate memory
                    entry = getMemory(entryLength);
                    if (entry.Length < entryLength)
                        throw new Exception("Byte array provided has invalid length");
                }
                else
                {
                    // We allocate a byte array from heap
                    entry = new byte[entryLength];
                }

                fixed (byte* bp = entry)
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);

                if (epochTaken)
                    epoch.Suspend();

                return true;
            }
            entry = default;
            return false;
        }

        /// <summary>
        /// GetNext supporting memory pools
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="entry"></param>
        /// <param name="entryLength"></param>
        /// <returns></returns>
        public unsafe bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength)
        {
            if (GetNextInternal(out long physicalAddress, out entryLength, out bool epochTaken))
            {
                entry = pool.Rent(entryLength);

                fixed (byte* bp = &entry.Memory.Span.GetPinnableReference())
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);

                if (epochTaken)
                    epoch.Suspend();

                return true;
            }
            entry = default;
            entryLength = default;
            return false;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Align(int length)
        {
            return (length + 3) & ~3;
        }

        /// <summary>
        /// Retrieve physical address of next iterator value
        /// (under epoch protection if it is from main page buffer)
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <param name="entryLength"></param>
        /// <param name="epochTaken"></param>
        /// <returns></returns>
        private unsafe bool GetNextInternal(out long physicalAddress, out int entryLength, out bool epochTaken)
        {
            physicalAddress = 0;
            entryLength = 0;
            epochTaken = false;

            currentAddress = nextAddress;
            while (true)
            {
                // Check for boundary conditions
                if (currentAddress < allocator.BeginAddress)
                {
                    Debug.WriteLine("Iterator address is less than log BeginAddress " + allocator.BeginAddress + ", adjusting iterator address");
                    currentAddress = allocator.BeginAddress;
                }

                if ((currentAddress >= endAddress) || (currentAddress >= fasterLog.CommittedUntilAddress))
                {
                    nextAddress = currentAddress;
                    return false;
                }

                if (frameSize == 0 && currentAddress < allocator.HeadAddress)
                {
                    throw new Exception("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                var currentPage = currentAddress >> allocator.LogPageSizeBits;
                var offset = currentAddress & allocator.PageSizeMask;

                var headAddress = allocator.HeadAddress;

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

                // Get and check entry length
                entryLength = fasterLog.GetLength((byte*)physicalAddress);
                if (entryLength == 0)
                {
                    if (currentAddress >= headAddress)
                        epoch.Suspend();

                    nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    if (0 != fasterLog.GetChecksum((byte*)physicalAddress))
                    {
                        var curPage = currentAddress >> allocator.LogPageSizeBits;
                        throw new Exception("Invalid checksum found during scan, skipping page " + curPage);
                    }
                    else
                    {
                        // We are likely at end of page, skip to next
                        currentAddress = nextAddress;
                        continue;
                    }
                }

                int recordSize = headerSize + Align(entryLength);
                if ((currentAddress & allocator.PageSizeMask) + recordSize > allocator.PageSize)
                {
                    if (currentAddress >= headAddress)
                        epoch.Suspend();
                    nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    throw new Exception("Invalid length of record found: " + entryLength + ", skipping page");
                }

                // Verify checksum if needed
                if (currentAddress < headAddress)
                {
                    if (!fasterLog.VerifyChecksum((byte*)physicalAddress, entryLength))
                    {
                        var curPage = currentAddress >> allocator.LogPageSizeBits;
                        nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                        throw new Exception("Invalid checksum found during scan, skipping page " + curPage);
                    }
                }

                if ((currentAddress & allocator.PageSizeMask) + recordSize == allocator.PageSize)
                    nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                else
                    nextAddress = currentAddress + recordSize;

                epochTaken = currentAddress >= headAddress;
                return true;
            }
        }

    }
}



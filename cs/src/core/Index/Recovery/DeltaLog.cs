// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = DeltaLog.HeaderSize)]
    internal struct DeltalogHeader
    {
        [FieldOffset(0)]
        public ulong Checksum;
        [FieldOffset(8)]
        public int Length;
        [FieldOffset(12)]
        public int Type;
    }

    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    internal sealed class DeltaLog : ScanIteratorBase, IDisposable
    {
        public const int HeaderSize = 16;
        readonly IDevice deltaLogDevice;
        readonly int LogPageSizeBits;
        readonly int PageSize;
        readonly int PageSizeMask;
        readonly int AlignedPageSizeBytes;
        readonly int sectorSize;
        BlittableFrame frame;
        bool disposed = false;

        // Fields to support writes
        SectorAlignedBufferPool memory;
        long tailAddress;
        long flushedUntilAddress;

        SemaphoreSlim completedSemaphore;
        int issuedFlush;
        SectorAlignedMemory buffer;

        /// <summary>
        /// Tail address
        /// </summary>
        public long TailAddress => tailAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        public DeltaLog(IDevice deltaLogDevice, int logPageSizeBits, long tailAddress)
            : base(0, tailAddress >= 0 ? tailAddress : deltaLogDevice.GetFileSize(0), ScanBufferingMode.SinglePageBuffering, default, logPageSizeBits, false)
        {
            LogPageSizeBits = logPageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;
            this.deltaLogDevice = deltaLogDevice;
            this.tailAddress = this.flushedUntilAddress = tailAddress;
            deltaLogDevice.Initialize(-1);
            sectorSize = (int)deltaLogDevice.SectorSize;
            AlignedPageSizeBytes = (int)Align(PageSize);
        }

        /// <inheritdoc />
        public override void InitializeForReads()
        {
            base.InitializeForReads();
            if (frameSize > 0 && endAddress > 0)
                frame = new BlittableFrame(frameSize, 1 << LogPageSizeBits, sectorSize);
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
                // Wait for ongoing page flushes
                completedSemaphore?.Wait();
                // Dispose flush buffer
                buffer?.Dispose();
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
                    readLength = (uint)(Align(readLength));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                usedDevice.ReadAsync(offsetInFile, (IntPtr)frame.pointers[pageIndex], readLength, AsyncReadPagesCallback, asyncResult);
            }
        }

        public unsafe static ref DeltalogHeader GetHeader(long physicalAddress) => ref Unsafe.AsRef<DeltalogHeader>((void*)physicalAddress);

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
            return (length + sectorSize - 1) & ~(sectorSize - 1);
        }

        public unsafe bool GetNext(out long physicalAddress, out int entryLength, out int type)
        {
            while (true)
            {
                physicalAddress = 0;
                entryLength = 0;
                currentAddress = nextAddress;
                type = 0;

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
                entryLength = GetHeader(physicalAddress).Length;
                type = GetHeader(physicalAddress).Type;

                if (entryLength == 0)
                {
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        return false;
                    else
                        continue;
                }

                int recordSize = (int)(Align(_currentOffset + HeaderSize + entryLength) - _currentOffset);
                if (entryLength < 0 || (_currentOffset + recordSize > PageSize))
                {
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        return false;
                    else
                        continue;
                }

                // Verify checksum
                if (!VerifyBlockChecksum((byte*)physicalAddress, entryLength))
                {
                    currentAddress = (1 + (currentAddress >> LogPageSizeBits)) << LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        return false;
                    else
                        continue;
                }
                physicalAddress += HeaderSize;

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static bool VerifyBlockChecksum(byte* ptr, int length)
        {
            var cs = Utility.XorBytes(ptr + 8, length + HeaderSize - 8);
            if (cs != GetHeader((long)ptr).Checksum)
            {
                return false;
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static void SetBlockHeader(int length, int type, byte* dest)
        {
            ref var header = ref GetHeader((long)dest);
            header.Length = length;
            header.Type = type;
            header.Checksum = Utility.XorBytes(dest + 8, length + HeaderSize - 8);
        }

        public void InitializeForWrites(SectorAlignedBufferPool memory, long tailAddress = -1)
        {
            this.memory = memory;
            if (tailAddress >= 0)
                this.tailAddress = tailAddress;
            completedSemaphore = new SemaphoreSlim(0);
            issuedFlush = 1;
            buffer = memory.Get(PageSize);
        }

        /// <summary>
        /// Returns allocated region on delta log to write to
        /// </summary>
        /// <param name="maxEntryLength">Max usable size of allocated region</param>
        /// <param name="physicalAddress">Address for caller to write to</param>
        public unsafe void Allocate(out int maxEntryLength, out long physicalAddress)
        {
            long pageEndAddress = (1 + (tailAddress >> LogPageSizeBits)) << LogPageSizeBits;
            long dataStartAddress = tailAddress + HeaderSize;
            maxEntryLength = (int)(pageEndAddress - dataStartAddress);
            int offset = (int)(dataStartAddress & PageSizeMask);
            physicalAddress = (long)buffer.aligned_pointer + offset;
        }

        /// <summary>
        /// Seal allocated region for given size, write header, move tail address
        /// </summary>
        /// <param name="entryLength">Entry length</param>
        /// <param name="type">Optional record type</param>
        public unsafe void Seal(int entryLength, int type = 0)
        {
            if (entryLength > 0)
            {
                int offset = (int)(tailAddress & PageSizeMask);
                SetBlockHeader(entryLength, type, buffer.aligned_pointer + offset);

                long oldTailAddress = tailAddress;
                tailAddress += HeaderSize + entryLength;
                tailAddress = Align(tailAddress);

                long pageEndAddress = (1 + (tailAddress >> LogPageSizeBits)) << LogPageSizeBits;
                if (tailAddress + HeaderSize >= pageEndAddress)
                    tailAddress = (1 + (tailAddress >> LogPageSizeBits)) << LogPageSizeBits;

                if ((oldTailAddress >> LogPageSizeBits) < (tailAddress >> LogPageSizeBits))
                    FlushPage();
            }
            else
            {
                // Unable to use entry, skip to next page
                tailAddress = (1 + (tailAddress >> LogPageSizeBits)) << LogPageSizeBits;
                FlushPage();
            }
        }

        private unsafe void FlushPage()
        {
            long pageStartAddress = tailAddress & ~PageSizeMask;
            int offset = (int)(tailAddress & PageSizeMask);
            if (offset == 0)
                pageStartAddress = (tailAddress - 1) & ~PageSizeMask;
            if (flushedUntilAddress > pageStartAddress)
                pageStartAddress = flushedUntilAddress;
            int startOffset = (int)(pageStartAddress & PageSizeMask);

            var asyncResult = new PageAsyncFlushResult<Empty> { count = 1, freeBuffer1 = buffer };
            var alignedBlockSize = Align(tailAddress - pageStartAddress);
            Interlocked.Increment(ref issuedFlush);
            deltaLogDevice.WriteAsync((IntPtr)buffer.aligned_pointer + startOffset,
                        (ulong)pageStartAddress,
                        (uint)alignedBlockSize, AsyncFlushPageToDeviceCallback, asyncResult);
            flushedUntilAddress = tailAddress;
            buffer = memory.Get(PageSize);
        }

        public unsafe SemaphoreSlim CompleteWrites()
        {
            // Flush last page if needed
            long pageStartAddress = tailAddress & ~PageSizeMask;
            if (tailAddress > pageStartAddress)
                FlushPage();
            if (Interlocked.Decrement(ref issuedFlush) == 0)
                completedSemaphore.Release(int.MaxValue);
            return completedSemaphore;
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        private void AsyncFlushPageToDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("AsyncFlushPageToDeviceCallback error: {0}", errorCode);
                }

                PageAsyncFlushResult<Empty> result = (PageAsyncFlushResult<Empty>)context;
                if (Interlocked.Decrement(ref result.count) == 0)
                {
                    result.Free();
                }
                if (Interlocked.Decrement(ref issuedFlush) == 0)
                    completedSemaphore.Release(int.MaxValue);
            }
            catch when (disposed) { }
        }
    }
}

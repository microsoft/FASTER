// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.IO;
using System.Diagnostics;

namespace FASTER.core
{
    internal interface IAllocator : IDisposable
    {
        long Allocate(int numSlots);
        long GetPhysicalAddress(long logicalAddress);
        void CheckForAllocateComplete(ref long address);
    }

    internal enum FlushStatus : int { Flushed, InProgress };

    internal enum CloseStatus : int { Closed, Open };

    internal struct FullPageStatus
	{
        public long LastFlushedUntilAddress;
        public FlushCloseStatus PageFlushCloseStatus;
	}

    [StructLayout(LayoutKind.Explicit)]
    internal struct FlushCloseStatus
    {
        [FieldOffset(0)]
        public FlushStatus PageFlushStatus;
        [FieldOffset(4)]
        public CloseStatus PageCloseStatus;
        [FieldOffset(0)]
        public long value;
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct PageOffset
    {
        [FieldOffset(0)]
        public int Offset;
        [FieldOffset(4)]
        public int Page;
        [FieldOffset(0)]
        public long PageAndOffset;
    }

    public unsafe partial class PersistentMemoryMalloc : IAllocator
    {
        // Epoch information
        public LightEpoch epoch;

        // Read buffer pool
        NativeSectorAlignedBufferPool readBufferPool;

        private readonly IDevice device;
        private readonly IDevice objectLogDevice;
        private readonly int sectorSize;

        // Page size
        private const int LogPageSizeBits = 25;
        private const int PageSize = 1 << LogPageSizeBits;
        private const int PageSizeMask = PageSize - 1;
        private readonly int AlignedPageSizeBytes;

        // Segment size
        private const int LogSegmentSizeBits = 30;
        private const long SegmentSize = 1 << LogSegmentSizeBits;
        private const long SegmentSizeMask = SegmentSize - 1;
        private const int SegmentBufferSize = 1 +
            (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

        // Total HLOG size
        private const int LogTotalSizeBits = 34;
        private const long LogTotalSizeBytes = 1L << LogTotalSizeBits;
        private const int BufferSize = (int)(LogTotalSizeBytes / (1L << LogPageSizeBits));

        // HeadOffset lag (from tail)
        private const int HeadOffsetLagNumPages = 4;
        private const int HeadOffsetLagSize = BufferSize - HeadOffsetLagNumPages;
        private const long HeadOffsetLagAddress = (long)HeadOffsetLagSize << LogPageSizeBits;

        // ReadOnlyOffset lag (from tail)
        public const double LogMutableFraction = 0.9;
        public const long ReadOnlyLagAddress = (long)(LogMutableFraction * BufferSize) << LogPageSizeBits;

        // Circular buffer definition
        private byte[][] values = new byte[BufferSize][];
        private GCHandle[] handles = new GCHandle[BufferSize];
        private long[] pointers = new long[BufferSize];
        private GCHandle ptrHandle;
        private long* nativePointers;

        // Array that indicates the status of each buffer page
        private FullPageStatus[] PageStatusIndicator = new FullPageStatus[BufferSize];

        NativeSectorAlignedBufferPool ioBufferPool;

        // Index in circular buffer, of the current tail page
        private volatile int TailPageIndex;

        // Global address of the current tail (next element to be allocated from the circular buffer)
        private PageOffset TailPageOffset;


        /// <summary>
        /// Read-only address
        /// </summary>
        public long ReadOnlyAddress;

        /// <summary>
        /// Safe read-only address
        /// </summary>
        public long SafeReadOnlyAddress;

        /// <summary>
        /// Head address
        /// </summary>
        public long HeadAddress;

        /// <summary>
        ///  Safe head address
        /// </summary>
        public long SafeHeadAddress;

        /// <summary>
        /// Flushed until address
        /// </summary>
        public long FlushedUntilAddress;

        /// <summary>
        /// Begin address
        /// </summary>
        public long BeginAddress;

        private IPageHandlers pageHandlers;

        /// <summary>
        /// Create instance of PMM
        /// </summary>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="pageHandlers"></param>
        public PersistentMemoryMalloc(IDevice device, IDevice objectLogDevice, IPageHandlers pageHandlers) : this(device, objectLogDevice, 0, pageHandlers)
        {
            Allocate(Constants.kFirstValidAddress); // null pointer
            ReadOnlyAddress = GetTailAddress();
            SafeReadOnlyAddress = ReadOnlyAddress;
            HeadAddress = ReadOnlyAddress;
            SafeHeadAddress = ReadOnlyAddress;
            BeginAddress = ReadOnlyAddress;
            this.pageHandlers = pageHandlers;
        }

        /// <summary>
        /// Create instance of PMM
        /// </summary>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="startAddress"></param>
        /// <param name="pageHandlers"></param>
        internal PersistentMemoryMalloc(IDevice device, IDevice objectLogDevice, long startAddress, IPageHandlers pageHandlers)
        {
            if (BufferSize < 16)
            {
                throw new Exception("HLOG buffer must be at least 16 pages");
            }

            this.device = device;

            this.objectLogDevice = objectLogDevice;

            if (pageHandlers.HasObjects())
            {
                if (objectLogDevice == null)
                    throw new Exception("Objects in key/value, but object log not provided during creation of FASTER instance");
            }

            sectorSize = (int)device.SectorSize;
            epoch = LightEpoch.Instance;
            ioBufferPool = new NativeSectorAlignedBufferPool(1, sectorSize);
            AlignedPageSizeBytes = ((PageSize + (sectorSize - 1)) & ~(sectorSize - 1));
            
            ptrHandle = GCHandle.Alloc(pointers, GCHandleType.Pinned);
            nativePointers = (long*)ptrHandle.AddrOfPinnedObject();

            Initialize(startAddress);
        }
        
        /// <summary>
        /// Get sector size
        /// </summary>
        /// <returns></returns>
        public int GetSectorSize()
        {
            return sectorSize;
        }

        internal void Initialize(long startAddress)
        {
            readBufferPool = new NativeSectorAlignedBufferPool(1, sectorSize);
            long tailPage = startAddress >> LogPageSizeBits;
            int tailPageIndex = (int)(tailPage % BufferSize);

            AllocatePage(tailPageIndex);

            SafeReadOnlyAddress = startAddress;
            ReadOnlyAddress = startAddress;
            SafeHeadAddress = startAddress;
            HeadAddress = startAddress;
            FlushedUntilAddress = startAddress;
            BeginAddress = startAddress;

            TailPageOffset.Page = (int)(startAddress >> LogPageSizeBits);
            TailPageOffset.Offset = (int)(startAddress & PageSizeMask);

            TailPageIndex = -1;

            //Handle the case when startAddress + pageSize overflows
            //onto the next pageIndex in our buffer pages array
            if (0 != (startAddress & PageSizeMask))
            {
                // Update write cache to point to current level.
                TailPageIndex = tailPageIndex;
                Interlocked.MemoryBarrier();

                // Allocate for next page
                int newPageIndex = (tailPageIndex + 1) % BufferSize;
                AllocatePage(newPageIndex);
            }
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public void Dispose()
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
                values[i] = null;
                PageStatusIndicator[i].PageFlushCloseStatus = new FlushCloseStatus { PageFlushStatus = FlushStatus.Flushed, PageCloseStatus = CloseStatus.Closed };
            }
            handles = null;
            pointers = null;
            values = null;
            TailPageOffset.Page = 0;
            TailPageOffset.Offset = 0;
            SafeReadOnlyAddress = 0;
            ReadOnlyAddress = 0;
            SafeHeadAddress = 0;
            HeadAddress = 0;
            BeginAddress = 1;
        }

        public long GetTailAddress()
        {
            var local = TailPageOffset;
            return ((long)local.Page << LogPageSizeBits) | (uint)local.Offset;
        }

        // Simple Accessor Functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPage(long logicalAddress)
        {
            return (logicalAddress >> LogPageSizeBits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPageIndexForPage(long page)
        {
            return (int)(page % BufferSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPageIndexForAddress(long address)
        {
            return (int)((address >> LogPageSizeBits) % BufferSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetCapacityNumPages()
        {
            return BufferSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetStartLogicalAddress(long page)
        {
            return page << LogPageSizeBits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPageSize()
        {
            return PageSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetOffsetInPage(long address)
        {
            return address & PageSizeMask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHeadOffsetLagInPages()
        {
            return HeadOffsetLagSize;
        }

        /// <summary>
        /// Used to obtain the physical address corresponding to a logical address
        /// </summary>
        /// <param name="logicalAddress"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & PageSizeMask);

            // Global page address
            long page = (logicalAddress >> LogPageSizeBits);

            // Index of page within the circular buffer
            int pageIndex = (int)(page % BufferSize);

            return *(nativePointers+pageIndex) + offset;
        }

        /// <summary>
        /// Key function used to allocate memory for a specified number of items
        /// </summary>
        /// <param name="numSlots"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Allocate(int numSlots = 1)
        {
            PageOffset localTailPageOffset = default(PageOffset);

            // Determine insertion index.
            // ReSharper disable once CSharpWarnings::CS0420
#pragma warning disable 420
            localTailPageOffset.PageAndOffset = Interlocked.Add(ref TailPageOffset.PageAndOffset, numSlots);
#pragma warning restore 420

            int page = localTailPageOffset.Page;
            int offset = localTailPageOffset.Offset - numSlots;

#region HANDLE PAGE OVERFLOW
            /* To prove correctness of the following modifications 
             * done to TailPageOffset and the allocation itself, 
             * we should use the fact that only one thread will have any 
             * of the following cases since it is a counter and we spin-wait
             * until the tail is folded onto next page accordingly.
             */
            if (localTailPageOffset.Offset >= PageSize)
            {
                if (offset >= PageSize)
                {
                    //The tail offset value was more than page size before atomic add
                    //We consider that a failed attempt and retry again
                    var spin = new SpinWait();
                    do
                    {
                        //Just to give some more time to the thread
                        // that is handling this overflow
                        while (TailPageOffset.Offset >= PageSize)
                        {
                            spin.SpinOnce();
                        }

                        // ReSharper disable once CSharpWarnings::CS0420
#pragma warning disable 420
                        localTailPageOffset.PageAndOffset = Interlocked.Add(ref TailPageOffset.PageAndOffset, numSlots);
#pragma warning restore 420

                        page = localTailPageOffset.Page;
                        offset = localTailPageOffset.Offset - numSlots;
                    } while (offset >= PageSize);
                }


                if (localTailPageOffset.Offset == PageSize)
                {
                    //Folding over at page boundary
                    localTailPageOffset.Page++;
                    localTailPageOffset.Offset = 0;
                    TailPageOffset = localTailPageOffset;
                }
                else if (localTailPageOffset.Offset >= PageSize)
                {
                    //Overflows not allowed. We allot same space in next page.
                    localTailPageOffset.Page++;
                    localTailPageOffset.Offset = numSlots;
                    TailPageOffset = localTailPageOffset;

                    page = localTailPageOffset.Page;
                    offset = 0;
                }
            }
#endregion

            long address = (((long)page) << LogPageSizeBits) | ((long)offset);

            // Check if TailPageIndex is appropriate and allocated!
            int pageIndex = page % BufferSize;

            if (TailPageIndex == pageIndex)
            {
                return (address);
            }

            //Invert the address if either the previous page is not flushed or if it is null
            if ((PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageFlushStatus != FlushStatus.Flushed) ||
                (PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus != CloseStatus.Closed) ||
                (values[pageIndex] == null))
            {
                address = -address;
            }

            // Update the read-only so that we can get more space for the tail
            if (offset == 0)
            {
                if (address >= 0)
                {
                    TailPageIndex = pageIndex;
                    Interlocked.MemoryBarrier();
                }

                long newPage = page + 1;
                int newPageIndex = (int)((page + 1) % BufferSize);

                long tailAddress = (address < 0 ? -address : address);
                PageAlignedShiftReadOnlyAddress(tailAddress);
                PageAlignedShiftHeadAddress(tailAddress);

                if (values[newPageIndex] == null) 
                {
                    AllocatePage(newPageIndex);
                }
            }

            return (address);
        }

        /// <summary>
        /// If allocator cannot allocate new memory as the head has not shifted or the previous page 
        /// is not yet closed, it allocates but returns the negative address. 
        /// This function is invoked to check if the address previously allocated has become valid to be used
        /// </summary>
        /// <param name="address"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckForAllocateComplete(ref long address)
        {
            if (address >= 0)
            {
                throw new Exception("Address already allocated!");
            }

            PageOffset p = default(PageOffset);
            p.Page = (int)((-address) >> LogPageSizeBits);
            p.Offset = (int)((-address) & PageSizeMask);

            //Check write cache
            int pageIndex = p.Page % BufferSize;
            if (TailPageIndex == pageIndex)
            {
                address = -address;
                return;
            }

            //Check if we can move the head offset
            long currentTailAddress = GetTailAddress();
            PageAlignedShiftHeadAddress(currentTailAddress);

            //Check if I can allocate pageIndex at all
            if ((PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageFlushStatus != FlushStatus.Flushed) ||
                (PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus != CloseStatus.Closed) ||
                (values[pageIndex] == null))
            {
                return;
            }

            //correct values and set write cache
            address = -address;
            if (p.Offset == 0)
            {
                TailPageIndex = pageIndex;
            }
            return;
        }
        
        /// <summary>
        /// Used by applications to make the current state of the database immutable quickly
        /// </summary>
        /// <param name="tailAddress"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShiftReadOnlyToTail(out long tailAddress)
        {
            tailAddress = GetTailAddress();
            long localTailAddress = tailAddress;
            long currentReadOnlyOffset = ReadOnlyAddress;
            if (MonotonicUpdate(ref ReadOnlyAddress, tailAddress, out long oldReadOnlyOffset))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(localTailAddress, false));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShiftBeginAddress(long oldBeginAddress, long newBeginAddress)
        {
            epoch.BumpCurrentEpoch(() =>
            {
                device.DeleteAddressRange(oldBeginAddress, newBeginAddress);
                objectLogDevice.DeleteSegmentRange((int)(oldBeginAddress >> LogSegmentSizeBits), (int)(newBeginAddress >> LogSegmentSizeBits));
            });
        }

        /// <summary>
        /// Checks if until address has been flushed!
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public bool CheckFlushedUntil(long address)
        {
            return FlushedUntilAddress >= address;
        }

        public void KillFuzzyRegion()
        {
            while (SafeReadOnlyAddress != ReadOnlyAddress)
            {
                Interlocked.CompareExchange(ref SafeReadOnlyAddress,
                                            ReadOnlyAddress,
                                            SafeReadOnlyAddress);
            }
        }

        /// <summary>
        /// Seal: make sure there are no longer any threads writing to the page
        /// Flush: send page to secondary store
        /// </summary>
        /// <param name="newSafeReadOnlyAddress"></param>
        /// <param name="waitForPendingFlushComplete"></param>
        public void OnPagesMarkedReadOnly(long newSafeReadOnlyAddress, bool waitForPendingFlushComplete = false)
        {
            if(MonotonicUpdate(ref SafeReadOnlyAddress, newSafeReadOnlyAddress, out long oldSafeReadOnlyAddress))
            {
                Debug.WriteLine("SafeReadOnly shifted from {0:X} to {1:X}", oldSafeReadOnlyAddress, newSafeReadOnlyAddress);
                long startPage = oldSafeReadOnlyAddress >> LogPageSizeBits;

                long endPage = (newSafeReadOnlyAddress >> LogPageSizeBits);
                int numPages = (int)(endPage - startPage);
                if (numPages > 10)
                {
                    new Thread(
                        () => AsyncFlushPages(startPage, newSafeReadOnlyAddress)).Start();
                }
                else
                {
                    AsyncFlushPages(startPage, newSafeReadOnlyAddress);
                }
            }
        }

        /// <summary>
        /// Action to be performed for when all threads have 
        /// agreed that a page range is closed.
        /// </summary>
        /// <param name="newSafeHeadAddress"></param>
        public void OnPagesClosed(long newSafeHeadAddress)
        {
            if (MonotonicUpdate(ref SafeHeadAddress, newSafeHeadAddress, out long oldSafeHeadAddress))
            {
                Debug.WriteLine("SafeHeadOffset shifted from {0:X} to {1:X}", oldSafeHeadAddress, newSafeHeadAddress);

                for (long closePageAddress = oldSafeHeadAddress; closePageAddress < newSafeHeadAddress; closePageAddress += PageSize)
                {
                    int closePage = (int)((closePageAddress >> LogPageSizeBits) % BufferSize);

                    if (values[closePage] == null)
                    {
                        AllocatePage(closePage);
                    }                    

                    while (true)
                    {
                        var oldStatus = PageStatusIndicator[closePage].PageFlushCloseStatus;
                        if (oldStatus.PageFlushStatus == FlushStatus.Flushed)
                        {
                            ClearPage(closePage, (closePageAddress >> LogPageSizeBits) == 0);

                            var thisCloseSegment = closePageAddress >> LogSegmentSizeBits;
                            var nextClosePage = (closePageAddress >> LogPageSizeBits) + 1;
                            var nextCloseSegment = nextClosePage >> (LogSegmentSizeBits - LogPageSizeBits);

                            if (thisCloseSegment != nextCloseSegment)
                            {
                                // Last page in current segment
                                segmentOffsets[thisCloseSegment % SegmentBufferSize] = 0;
                            }
                        }
                        else
                        {
                            throw new Exception("Impossible");
                        }
                        var newStatus = oldStatus;
                        newStatus.PageCloseStatus = CloseStatus.Closed;
                        if (oldStatus.value == Interlocked.CompareExchange(ref PageStatusIndicator[closePage].PageFlushCloseStatus.value, newStatus.value, oldStatus.value))
                        {
                            break;
                        }
                    }

                    //Necessary to propagate this change to other threads
                    Interlocked.MemoryBarrier();
                }
            }
        }
        
        private void ClearPage(int page, bool pageZero)
        {
            if (pageHandlers.HasObjects())
            {
                long ptr = pointers[page];
                int numBytes = PageSize;
                long endptr = ptr + numBytes;

                if (pageZero) ptr += Constants.kFirstValidAddress;
                pageHandlers.ClearPage(ptr, endptr);
            }
            Array.Clear(values[page], 0, values[page].Length);
        }


        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        private void AllocatePage(int index)
        {
            var adjustedSize = PageSize + 2 * sectorSize;
            byte[] tmp = new byte[adjustedSize];
            Array.Clear(tmp, 0, adjustedSize);

            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
            pointers[index] = (p + (sectorSize - 1)) & ~(sectorSize - 1);
            values[index] = tmp;

            PageStatusIndicator[index].PageFlushCloseStatus.PageFlushStatus = FlushStatus.Flushed;
            PageStatusIndicator[index].PageFlushCloseStatus.PageCloseStatus = CloseStatus.Closed;
            Interlocked.MemoryBarrier();
        }

        /// <summary>
        /// Called every time a new tail page is allocated. Here the read-only is 
        /// shifted only to page boundaries unlike ShiftReadOnlyToTail where shifting
        /// can happen to any fine-grained address.
        /// </summary>
        /// <param name="currentTailAddress"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PageAlignedShiftReadOnlyAddress(long currentTailAddress)
        {
            long currentReadOnlyAddress = ReadOnlyAddress;
            long pageAlignedTailAddress = currentTailAddress & ~PageSizeMask;
            long desiredReadOnlyAddress = (pageAlignedTailAddress - ReadOnlyLagAddress);
            if (MonotonicUpdate(ref ReadOnlyAddress, desiredReadOnlyAddress, out long oldReadOnlyAddress))
            {
                Debug.WriteLine("Allocate: Moving read-only offset from {0:X} to {1:X}", oldReadOnlyAddress, desiredReadOnlyAddress);
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(desiredReadOnlyAddress));
            }
        }

        /// <summary>
        /// Called whenever a new tail page is allocated or when the user is checking for a failed memory allocation
        /// Tries to shift head address based on the head offset lag size.
        /// </summary>
        /// <param name="currentTailAddress"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PageAlignedShiftHeadAddress(long currentTailAddress)
        {
            //obtain local values of variables that can change
            long currentHeadAddress = HeadAddress;
            long currentFlushedUntilAddress = FlushedUntilAddress;
            long pageAlignedTailAddress = currentTailAddress & ~PageSizeMask;
            long desiredHeadAddress = (pageAlignedTailAddress - HeadOffsetLagAddress);

            long newHeadAddress = desiredHeadAddress;
            if(currentFlushedUntilAddress < newHeadAddress)
            {
                newHeadAddress = currentFlushedUntilAddress;
            }
            newHeadAddress = newHeadAddress & ~PageSizeMask;

            if (MonotonicUpdate(ref HeadAddress, newHeadAddress, out long oldHeadAddress))
            {
                Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
            }
        }

        /// <summary>
        /// Every async flush callback tries to update the flushed until address to the latest value possible
        /// Is there a better way to do this with enabling fine-grained addresses (not necessarily at page boundaries)?
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ShiftFlushedUntilAddress()
        {
            long currentFlushedUntilAddress = FlushedUntilAddress;
            long page = GetPage(currentFlushedUntilAddress);

            bool update = false;
            long pageLastFlushedAddress = PageStatusIndicator[(int)(page % BufferSize)].LastFlushedUntilAddress;
            while (pageLastFlushedAddress >= currentFlushedUntilAddress)
            {
                currentFlushedUntilAddress = pageLastFlushedAddress;
                update = true;
                page++;
                pageLastFlushedAddress = PageStatusIndicator[(int)(page % BufferSize)].LastFlushedUntilAddress;
            }

            if (update)
            {
                MonotonicUpdate(ref FlushedUntilAddress, currentFlushedUntilAddress, out long oldFlushedUntilAddress);
            }
        }



        /// <summary>
        /// Used by several functions to update the variable to newValue. Ignores if newValue is smaller or 
        /// than the current value.
        /// </summary>
        /// <param name="variable"></param>
        /// <param name="newValue"></param>
        /// <param name="oldValue"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MonotonicUpdate(ref long variable, long newValue, out long oldValue)
        {
            oldValue = variable;
            while (oldValue < newValue)
            {
                var foundValue = Interlocked.CompareExchange(ref variable, newValue, oldValue);
                if (foundValue == oldValue)
                {
                    return true;
                }
                oldValue = foundValue;
            }
            return false;
        }

        public void RecoveryReset(long tailAddress, long headAddress)
        {
            long tailPage = GetPage(tailAddress);
            long offsetInPage = GetOffsetInPage(tailAddress);
            TailPageOffset.Page = (int)tailPage;
            TailPageOffset.Offset = (int)offsetInPage;
            TailPageIndex = GetPageIndexForPage(TailPageOffset.Page);

            // issue read request to all pages until head lag
            HeadAddress = headAddress;
            SafeHeadAddress = headAddress;
            FlushedUntilAddress = headAddress;
            ReadOnlyAddress = tailAddress;
            SafeReadOnlyAddress = tailAddress;

            for (var addr = headAddress; addr < tailAddress; addr += PageSize)
            {
                var pageIndex = GetPageIndexForAddress(addr);
                PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus = CloseStatus.Open;
            }
        }
    }
}

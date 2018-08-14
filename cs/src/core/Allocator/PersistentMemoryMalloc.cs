// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CALLOC
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

	public enum FlushStatus : int { Flushed, InProgress };

	public enum CloseStatus : int { Closed, Open };

    
    public struct FullPageStatus
	{
        public long LastFlushedUntilAddress;
        public FlushCloseStatus PageFlushCloseStatus;
	}

    [StructLayout(LayoutKind.Explicit)]
    public struct FlushCloseStatus
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

    public unsafe partial class PersistentMemoryMalloc<T> : IAllocator
    {
        // Epoch information
        public LightEpoch epoch;

        // Read buffer pool
        NativeSectorAlignedBufferPool readBufferPool;

        // Record size and pinning
        private readonly bool IsPinned;
        private const int PrivateRecordSize = 1;
        private static bool ForceUnpinnedAllocation = false;

        private readonly IDevice device;
        private readonly ISegmentedDevice objlogDevice;
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
        private const long LogTotalSizeBytes = 1L << 34; // 29
        private const int BufferSize = (int)(LogTotalSizeBytes / (1L << LogPageSizeBits));

        // HeadOffset lag (from tail)
        private const int HeadOffsetLagNumPages = 4;
        private const int HeadOffsetLagSize = BufferSize - HeadOffsetLagNumPages;
        private const long HeadOffsetLagAddress = (long)HeadOffsetLagSize << LogPageSizeBits;

        // ReadOnlyOffset lag (from tail)
        public const double LogMutableFraction = 0.9;
        public const long ReadOnlyLagAddress = (long)(LogMutableFraction * BufferSize) << LogPageSizeBits;

        // Circular buffer definition
        private T[][] values = new T[BufferSize][];
        private GCHandle[] handles = new GCHandle[BufferSize];
        private IntPtr[] pointers = new IntPtr[BufferSize];
        private GCHandle ptrHandle;
        private long* nativePointers;

        // Array that indicates the status of each buffer page
        private FullPageStatus[] PageStatusIndicator = new FullPageStatus[BufferSize];

        NativeSectorAlignedBufferPool ioBufferPool;

        // Index in circular buffer, of the current tail page
        private volatile int TailPageIndex;

        // Global address of the current tail (next element to be allocated from the circular buffer)
        private PageOffset TailPageOffset;

        public long ReadOnlyAddress;

        public long SafeReadOnlyAddress;

        public long HeadAddress;

        public long SafeHeadAddress;

        public long FlushedUntilAddress;

        public long BeginAddress;

        /// <summary>
        /// The smallest record size that can be allotted
        /// </summary>
        public int RecordSize
        {
            get
            {
                return PrivateRecordSize;
            }
        }

        public PersistentMemoryMalloc(IDevice device) : this(device, 0)
        {
            Allocate(Constants.kFirstValidAddress); // null pointer
            ReadOnlyAddress = GetTailAddress();
            SafeReadOnlyAddress = ReadOnlyAddress;
            HeadAddress = ReadOnlyAddress;
            SafeHeadAddress = ReadOnlyAddress;
            BeginAddress = ReadOnlyAddress;
        }

        public PersistentMemoryMalloc(IDevice device, long startAddress)
        {
            // Console.WriteLine("Total memory (GB) = " + totalSize/1000000000);
            // Console.WriteLine("BufferSize = " + BufferSize);
            // Console.WriteLine("ReadOnlyLag = " + (ReadOnlyLagAddress >> PageSizeBits));

            if (BufferSize < 16)
            {
                throw new Exception("HLOG buffer must be at least 16 pages");
            }

            this.device = device;

            objlogDevice = CreateObjectLogDevice(device);

            sectorSize = (int)device.GetSectorSize();
            epoch = LightEpoch.Instance;
            ioBufferPool = new NativeSectorAlignedBufferPool(1, sectorSize);

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
                    //PrivateRecordSize = Marshal.SizeOf(tmp[0]);
                    AlignedPageSizeBytes = (((PrivateRecordSize * PageSize) + (sectorSize - 1)) & ~(sectorSize - 1));
                }
                catch (Exception)
                {
                    IsPinned = false;
                }
            }

            ptrHandle = GCHandle.Alloc(pointers, GCHandleType.Pinned);
            nativePointers = (long*)ptrHandle.AddrOfPinnedObject();

            Initialize(startAddress);
        }
        
        public int GetSectorSize()
        {
            return sectorSize;
        }

        public void Initialize(long startAddress)
        {
            readBufferPool = new NativeSectorAlignedBufferPool(PrivateRecordSize, sectorSize);
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
        public void Free()
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (IsPinned && (handles[i].IsAllocated)) handles[i].Free();
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Get(long index)
        {
            if (this.IsPinned)
                throw new Exception("Physical pointer returned by allocator: de-reference pointer to get records instead of calling Get");

            return values
                [index >> LogPageSizeBits]
                [index & PageSizeMask];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long index, ref T value)
        {
            if (this.IsPinned)
                throw new Exception("Physical pointer returned by allocator: de-reference pointer to set records instead of calling Set (otherwise, set ForceUnpinnedAllocation to true)");

            values
                [index >> LogPageSizeBits]
                [index & PageSizeMask]
                = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long index, T value)
        {
            Set(index, ref value);
        }

#if USEFREELIST
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FreeAtEpoch(long pointer, int removed_epoch)
        {
            if (freeList == null) freeList = new Queue<FreeItem>();
            freeList.Enqueue(new FreeItem { removed_item = pointer, removal_epoch = removed_epoch });
        }

#if DEBUG
        public long TotalFreeCount()
        {
            long result = 0;
            var x = allQueues.ToArray();
            foreach (var q in x)
            {
                result += q.Count;
            }
            return result;
        }

        public long TotalUsedPointers()
        {
            return TailAddress - TotalFreeCount();
        }

#endif
#endif
        //Simple Accessor Functions
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

            return (*(nativePointers+pageIndex)) + offset*PrivateRecordSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddressInternal(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & PageSizeMask);

            // Global page address
            long page = (logicalAddress >> LogPageSizeBits);

            // Index of page within the circular buffer
            int pageIndex = (int)(page % BufferSize);

            return (*(nativePointers + pageIndex)) + offset * PrivateRecordSize;
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

            /*
            if (pageIndex == 0 && page != 0)
            {
                Debugger.Break();
            }*/
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
                objlogDevice.DeleteSegmentRange((int)(oldBeginAddress >> LogSegmentSizeBits), (int)(newBeginAddress >> LogSegmentSizeBits));
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
        /// <param name="untilAddress"></param>
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
        /// Action to be performed for when all threads have agreed that a page range is closed.
        /// </summary>
        /// <param name="oldSafeHeadOffset"></param>
        /// <param name="newSafeHeadAddress"></param>
        /// <param name="replaceWithCleanPage"></param>
        public void OnPagesClosed(long newSafeHeadAddress,  bool replaceWithCleanPage = false)
        {
            if (MonotonicUpdate(ref SafeHeadAddress, newSafeHeadAddress, out long oldSafeHeadAddress))
            {
                Debug.WriteLine("SafeHeadOffset shifted from {0:X} to {1:X}", oldSafeHeadAddress, newSafeHeadAddress);

                for (long closePageAddress = oldSafeHeadAddress; closePageAddress < newSafeHeadAddress; closePageAddress += PageSize)
                {
                    int closePage = (int)((closePageAddress >> LogPageSizeBits) % BufferSize);

                    if (replaceWithCleanPage)
                    {
                        if (values[closePage] == null)
                        {
                            // Allocate a new page
                            AllocatePage(closePage);
                        }
                        else
                        {
                            //Clear an old used page
                            // BUG: we cannot clear because the
                            // page may not be flushed.
                            // Array.Clear(values[closePage], 0, values[closePage].Length);
                        }
                    }
                    else
                    {
                        values[closePage] = null;
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
            if (Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize())
            {
                long ptr = (long)pointers[page];
                int numBytes = PageSize * PrivateRecordSize;
                long endptr = ptr + numBytes;

                if (pageZero) ptr += Constants.kFirstValidAddress;

                List<long> addr = new List<long>();
                while (ptr < endptr)
                {
                    if (!Layout.GetInfo(ptr)->Invalid)
                    {
                        if (Key.HasObjectsToSerialize())
                        {
                            Key* key = Layout.GetKey(ptr);
                            Key.Free(key);
                        }
                        if (Value.HasObjectsToSerialize())
                        {
                            Value* value = Layout.GetValue(ptr);
                            Value.Free(value);
                        }
                    }
                    ptr += Layout.GetPhysicalSize(ptr);
                }
            }
            Array.Clear(values[page], 0, values[page].Length);
        }


        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        private void AllocatePage(int index, bool clear = false)
        {
            if (IsPinned)
            {
                var adjustedSize = PageSize + (int)Math.Ceiling(2 * sectorSize / PrivateRecordSize * 1.0);
                T[] tmp = new T[adjustedSize];
                if (clear)
                {
                    Array.Clear(tmp, 0, adjustedSize);
                }
                else
                {
#if !(CALLOC)
                    Array.Clear(tmp, 0, adjustedSize);
#endif
                }

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
                if (oldReadOnlyAddress == 0)
                    Console.WriteLine("Going read-only");
                /*
                for (int i = (int)(oldReadOnlyAddress >> LogPageSizeBits); i < (int)(desiredReadOnlyAddress >> LogPageSizeBits); i++)
                {
                    //Set status to in-progress
                    PageStatusIndicator[i % BufferSize].PageFlushCloseStatus
                        = new FlushCloseStatus { PageFlushStatus = FlushStatus.InProgress, PageCloseStatus = CloseStatus.Open };
                    PageStatusIndicator[i % BufferSize].LastFlushedUntilAddress = -1;
                }
                */
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
                if (oldHeadAddress == 0)
                    Console.WriteLine("Going external memory");

                Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress, true));
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

            if(update)
            {
                bool success = MonotonicUpdate(ref FlushedUntilAddress, currentFlushedUntilAddress, out long oldFlushedUntilAddress);
                if (success)
                {
                }
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
    }
}

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
    internal enum PMMFlushStatus : int { Flushed, InProgress };

    internal enum PMMCloseStatus : int { Closed, Open };

    internal struct FullPageStatus
    {
        public long LastFlushedUntilAddress;
        public FlushCloseStatus PageFlushCloseStatus;
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct FlushCloseStatus
    {
        [FieldOffset(0)]
        public PMMFlushStatus PageFlushStatus;
        [FieldOffset(4)]
        public PMMCloseStatus PageCloseStatus;
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

    /// <summary>
    /// Base class for hybrid log memory allocator
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public unsafe abstract class AllocatorBase<Key, Value> : IDisposable
        where Key : new()
        where Value : new()
    {
        /// <summary>
        /// Epoch information
        /// </summary>
        protected LightEpoch epoch;

        /// <summary>
        /// Comparer
        /// </summary>
        protected readonly IFasterEqualityComparer<Key> comparer;

        #region Protected size definitions
        /// <summary>
        /// Buffer size
        /// </summary>
        protected readonly int BufferSize;
        /// <summary>
        /// Log page size
        /// </summary>
        protected readonly int LogPageSizeBits;

        /// <summary>
        /// Page size
        /// </summary>
        protected readonly int PageSize;
        /// <summary>
        /// Page size mask
        /// </summary>
        protected readonly int PageSizeMask;
        /// <summary>
        /// Buffer size mask
        /// </summary>
        protected readonly int BufferSizeMask;
        /// <summary>
        /// Aligned page size in bytes
        /// </summary>
        protected readonly int AlignedPageSizeBytes;

        /// <summary>
        /// Total hybrid log size (bits)
        /// </summary>
        protected readonly int LogTotalSizeBits;
        /// <summary>
        /// Total hybrid log size (bytes)
        /// </summary>
        protected readonly long LogTotalSizeBytes;

        /// <summary>
        /// Segment size in bits
        /// </summary>
        protected readonly int LogSegmentSizeBits;
        /// <summary>
        /// Segment size
        /// </summary>
        protected readonly long SegmentSize;
        /// <summary>
        /// Segment buffer size
        /// </summary>
        protected readonly int SegmentBufferSize;

        /// <summary>
        /// HeadOffset lag (from tail)
        /// </summary>
        protected const int HeadOffsetLagNumPages = 4;
        /// <summary>
        /// HeadOffset lag size
        /// </summary>
        protected readonly int HeadOffsetLagSize;
        /// <summary>
        /// HeadOFfset lag address
        /// </summary>
        protected readonly long HeadOffsetLagAddress;

        /// <summary>
        /// Log mutable fraction
        /// </summary>
        protected readonly double LogMutableFraction;
        /// <summary>
        /// ReadOnlyOffset lag (from tail)
        /// </summary>
        protected readonly long ReadOnlyLagAddress;

        #endregion

        #region Public addresses
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

        #endregion

        #region Protected device info
        /// <summary>
        /// Device
        /// </summary>
        protected readonly IDevice device;
        /// <summary>
        /// Sector size
        /// </summary>
        protected readonly int sectorSize;
        #endregion

        #region Private page metadata
        /// <summary>
        /// Index in circular buffer, of the current tail page
        /// </summary>
        private volatile int TailPageIndex;

        // Array that indicates the status of each buffer page
        internal readonly FullPageStatus[] PageStatusIndicator;

        /// <summary>
        /// Global address of the current tail (next element to be allocated from the circular buffer) 
        /// </summary>
        private PageOffset TailPageOffset;

        /// <summary>
        /// Number of pending reads
        /// </summary>
        private static int numPendingReads = 0;
        #endregion

        /// <summary>
        /// Read buffer pool
        /// </summary>
        protected SectorAlignedBufferPool readBufferPool;

        #region Abstract methods
        /// <summary>
        /// Initialize
        /// </summary>
        public abstract void Initialize();
        /// <summary>
        /// Get start logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public abstract long GetStartLogicalAddress(long page);
        /// <summary>
        /// Get physical address
        /// </summary>
        /// <param name="newLogicalAddress"></param>
        /// <returns></returns>
        public abstract long GetPhysicalAddress(long newLogicalAddress);
        /// <summary>
        /// Get address info
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract ref RecordInfo GetInfo(long physicalAddress);

        /// <summary>
        /// Get info from byte pointer
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        public abstract ref RecordInfo GetInfoFromBytePointer(byte* ptr);

        /// <summary>
        /// Get key
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract ref Key GetKey(long physicalAddress);
        /// <summary>
        /// Get value
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract ref Value GetValue(long physicalAddress);
        /// <summary>
        /// Get address info for key
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract AddressInfo* GetKeyAddressInfo(long physicalAddress);
        /// <summary>
        /// Get address info for value
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract AddressInfo* GetValueAddressInfo(long physicalAddress);

        /// <summary>
        /// Get record size
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract int GetRecordSize(long physicalAddress);
        /// <summary>
        /// Get average record size
        /// </summary>
        /// <returns></returns>
        public abstract int GetAverageRecordSize();
        /// <summary>
        /// Get initial record size
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        public abstract int GetInitialRecordSize<Input>(ref Key key, ref Input input);
        /// <summary>
        /// Get record size
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public abstract int GetRecordSize(ref Key key, ref Value value);

        /// <summary>
        /// Allocate page
        /// </summary>
        /// <param name="index"></param>
        protected abstract void AllocatePage(int index);
        /// <summary>
        /// Whether page is allocated
        /// </summary>
        /// <param name="pageIndex"></param>
        /// <returns></returns>
        protected abstract bool IsAllocated(int pageIndex);
        /// <summary>
        /// Populate page
        /// </summary>
        /// <param name="src"></param>
        /// <param name="required_bytes"></param>
        /// <param name="destinationPage"></param>
        internal abstract void PopulatePage(byte* src, int required_bytes, long destinationPage);
        /// <summary>
        /// Write async to device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="startPage"></param>
        /// <param name="flushPage"></param>
        /// <param name="pageSize"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        protected abstract void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize, IOCompletionCallback callback, PageAsyncFlushResult<TContext> result, IDevice device, IDevice objectLogDevice);
        /// <summary>
        /// Read objects to memory (async)
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        protected abstract void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, IOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default(SectorAlignedMemory));
        /// <summary>
        /// Read page (async)
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="destinationPageIndex"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        /// <param name="device"></param>
        /// <param name="objlogDevice"></param>
        protected abstract void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length, IOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice);
        /// <summary>
        /// Clear page
        /// </summary>
        /// <param name="page"></param>
        /// <param name="pageZero"></param>
        protected abstract void ClearPage(int page, bool pageZero);
        /// <summary>
        /// Write page (async)
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="flushPage"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        protected abstract void WriteAsync<TContext>(long flushPage, IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult);
        /// <summary>
        /// Retrieve full record
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected abstract bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx);
        /// <summary>
        /// Whether key has objects
        /// </summary>
        /// <returns></returns>
        public abstract bool KeyHasObjects();

        /// <summary>
        /// Whether value has objects
        /// </summary>
        /// <returns></returns>
        public abstract bool ValueHasObjects();

        /// <summary>
        /// Get segment offsets
        /// </summary>
        /// <returns></returns>
        public abstract long[] GetSegmentOffsets();
        #endregion

        /// <summary>
        /// Instantiate base allocator
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="comparer"></param>
        public AllocatorBase(LogSettings settings, IFasterEqualityComparer<Key> comparer)
        {
            this.comparer = comparer;
            settings.LogDevice.Initialize(1L << settings.SegmentSizeBits);
            settings.ObjectLogDevice?.Initialize(1L << settings.SegmentSizeBits);

            // Page size
            LogPageSizeBits = settings.PageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;

            // Total HLOG size
            LogTotalSizeBits = settings.MemorySizeBits;
            LogTotalSizeBytes = 1L << LogTotalSizeBits;
            BufferSize = (int)(LogTotalSizeBytes / (1L << LogPageSizeBits));
            BufferSizeMask = BufferSize - 1;

            // HeadOffset lag (from tail)
            HeadOffsetLagSize = BufferSize - HeadOffsetLagNumPages;
            HeadOffsetLagAddress = (long)HeadOffsetLagSize << LogPageSizeBits;

            // ReadOnlyOffset lag (from tail)
            LogMutableFraction = settings.MutableFraction;
            ReadOnlyLagAddress = (long)(LogMutableFraction * BufferSize) << LogPageSizeBits;

            // Segment size
            LogSegmentSizeBits = settings.SegmentSizeBits;
            SegmentSize = 1 << LogSegmentSizeBits;
            SegmentBufferSize = 1 + (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

            if (BufferSize < 16)
            {
                throw new Exception("HLOG buffer must be at least 16 pages");
            }

            PageStatusIndicator = new FullPageStatus[BufferSize];

            device = settings.LogDevice;
            sectorSize = (int)device.SectorSize;
            AlignedPageSizeBytes = ((PageSize + (sectorSize - 1)) & ~(sectorSize - 1));
        }

        /// <summary>
        /// Initialize allocator
        /// </summary>
        /// <param name="firstValidAddress"></param>
        protected void Initialize(long firstValidAddress)
        {
            readBufferPool = SectorAlignedBufferPool.GetPool(1, sectorSize);

            long tailPage = firstValidAddress >> LogPageSizeBits;
            int tailPageIndex = (int)(tailPage % BufferSize);
            Debug.Assert(tailPageIndex == 0);
            AllocatePage(tailPageIndex);

            // Allocate next page as well
            if (firstValidAddress > 0)
                AllocatePage(tailPageIndex + 1);

            SafeReadOnlyAddress = firstValidAddress;
            ReadOnlyAddress = firstValidAddress;
            SafeHeadAddress = firstValidAddress;
            HeadAddress = firstValidAddress;
            FlushedUntilAddress = firstValidAddress;
            BeginAddress = firstValidAddress;

            TailPageOffset.Page = (int)(firstValidAddress >> LogPageSizeBits);
            TailPageOffset.Offset = (int)(firstValidAddress & PageSizeMask);

            TailPageIndex = 0;
        }

        /// <summary>
        /// Dispose allocator
        /// </summary>
        public virtual void Dispose()
        {
            for (int i=0; i<PageStatusIndicator.Length; i++)
                PageStatusIndicator[i].PageFlushCloseStatus = new FlushCloseStatus { PageFlushStatus = PMMFlushStatus.Flushed, PageCloseStatus = PMMCloseStatus.Closed };

            TailPageOffset.Page = 0;
            TailPageOffset.Offset = 0;
            SafeReadOnlyAddress = 0;
            ReadOnlyAddress = 0;
            SafeHeadAddress = 0;
            HeadAddress = 0;
            BeginAddress = 1;
        }

        /// <summary>
        /// Segment size
        /// </summary>
        /// <returns></returns>
        public long GetSegmentSize()
        {
            return SegmentSize;
        }

        /// <summary>
        /// Get tail address
        /// </summary>
        /// <returns></returns>
        public long GetTailAddress()
        {
            var local = TailPageOffset;
            return ((long)local.Page << LogPageSizeBits) | (uint)local.Offset;
        }

        /// <summary>
        /// Get page
        /// </summary>
        /// <param name="logicalAddress"></param>
        /// <returns></returns>
        public long GetPage(long logicalAddress)
        {
            return (logicalAddress >> LogPageSizeBits);
        }

        /// <summary>
        /// Get page index for page
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public int GetPageIndexForPage(long page)
        {
            return (int)(page % BufferSize);
        }

        /// <summary>
        /// Get page index for address
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public int GetPageIndexForAddress(long address)
        {
            return (int)((address >> LogPageSizeBits) % BufferSize);
        }

        /// <summary>
        /// Get capacity (number of pages)
        /// </summary>
        /// <returns></returns>
        public int GetCapacityNumPages()
        {
            return BufferSize;
        }


        /// <summary>
        /// Get page size
        /// </summary>
        /// <returns></returns>
        public long GetPageSize()
        {
            return PageSize;
        }

        /// <summary>
        /// Get offset in page
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public long GetOffsetInPage(long address)
        {
            return address & PageSizeMask;
        }

        /// <summary>
        /// Get offset lag in pages
        /// </summary>
        /// <returns></returns>
        public long GetHeadOffsetLagInPages()
        {
            return HeadOffsetLagSize;
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
            if ((PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageFlushStatus != PMMFlushStatus.Flushed) ||
                (PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus != PMMCloseStatus.Closed) ||
                (!IsAllocated(pageIndex)))
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

                if ((!IsAllocated(newPageIndex)))
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
            if ((PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageFlushStatus != PMMFlushStatus.Flushed) ||
                (PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus != PMMCloseStatus.Closed) ||
                (!IsAllocated(pageIndex)))
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

        /// <summary>
        /// Used by applications to move read-only forward
        /// </summary>
        /// <param name="newReadOnlyAddress"></param>
        public bool ShiftReadOnlyAddress(long newReadOnlyAddress)
        {
            if (MonotonicUpdate(ref ReadOnlyAddress, newReadOnlyAddress, out long oldReadOnlyOffset))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(newReadOnlyAddress, false));
                return true;
            }
            return false;
        }

        /// <summary>
        /// Shift begin address
        /// </summary>
        /// <param name="newBeginAddress"></param>
        public void ShiftBeginAddress(long newBeginAddress)
        {
            // First update the begin address
            MonotonicUpdate(ref BeginAddress, newBeginAddress, out long oldBeginAddress);
            // Then the head address
            var h = MonotonicUpdate(ref HeadAddress, newBeginAddress, out long old);
            // Finally the read-only address
            var r = MonotonicUpdate(ref ReadOnlyAddress, newBeginAddress, out old);

            // Clean up until begin address
            epoch.BumpCurrentEpoch(() =>
            {
                if (r)
                {
                    MonotonicUpdate(ref SafeReadOnlyAddress, newBeginAddress, out long _old);
                    MonotonicUpdate(ref FlushedUntilAddress, newBeginAddress, out _old);
                }
                if (h) OnPagesClosed(newBeginAddress);

                DeleteAddressRange(oldBeginAddress, newBeginAddress);
            });
        }

        /// <summary>
        /// Delete address range
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="toAddress"></param>
        protected virtual void DeleteAddressRange(long fromAddress, long toAddress)
        {
            device.DeleteAddressRange(fromAddress, toAddress);
        }

        /// <summary>
        /// Seal: make sure there are no longer any threads writing to the page
        /// Flush: send page to secondary store
        /// </summary>
        /// <param name="newSafeReadOnlyAddress"></param>
        /// <param name="waitForPendingFlushComplete"></param>
        public void OnPagesMarkedReadOnly(long newSafeReadOnlyAddress, bool waitForPendingFlushComplete = false)
        {
            if (MonotonicUpdate(ref SafeReadOnlyAddress, newSafeReadOnlyAddress, out long oldSafeReadOnlyAddress))
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

                for (long closePageAddress = oldSafeHeadAddress & ~PageSizeMask; closePageAddress < newSafeHeadAddress; closePageAddress += PageSize)
                {
                    if (newSafeHeadAddress < closePageAddress + PageSize)
                    {
                        // Partial page - do not close
                        return;
                    }

                    int closePage = (int)((closePageAddress >> LogPageSizeBits) % BufferSize);

                    if (!IsAllocated(closePage))
                    {
                        AllocatePage(closePage);
                    }

                    while (true)
                    {
                        var oldStatus = PageStatusIndicator[closePage].PageFlushCloseStatus;
                        if (oldStatus.PageFlushStatus == PMMFlushStatus.Flushed)
                        {
                            ClearPage(closePage, (closePageAddress >> LogPageSizeBits) == 0);
                        }
                        else
                        {
                            throw new Exception("Impossible");
                        }

                        var newStatus = oldStatus;
                        newStatus.PageCloseStatus = PMMCloseStatus.Closed;
                        if (oldStatus.value == Interlocked.CompareExchange(ref PageStatusIndicator[closePage].PageFlushCloseStatus.value, newStatus.value, oldStatus.value))
                        {
                            break;
                        }
                    }

                    // Necessary to propagate this change to other threads
                    Interlocked.MemoryBarrier();
                }
            }
        }

        /// <summary>
        /// Called every time a new tail page is allocated. Here the read-only is 
        /// shifted only to page boundaries unlike ShiftReadOnlyToTail where shifting
        /// can happen to any fine-grained address.
        /// </summary>
        /// <param name="currentTailAddress"></param>
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
        private void PageAlignedShiftHeadAddress(long currentTailAddress)
        {
            //obtain local values of variables that can change
            long currentHeadAddress = HeadAddress;
            long currentFlushedUntilAddress = FlushedUntilAddress;
            long pageAlignedTailAddress = currentTailAddress & ~PageSizeMask;
            long desiredHeadAddress = (pageAlignedTailAddress - HeadOffsetLagAddress);

            long newHeadAddress = desiredHeadAddress;
            if (currentFlushedUntilAddress < newHeadAddress)
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
        /// Tries to shift head address to specified value
        /// </summary>
        /// <param name="desiredHeadAddress"></param>
        public long ShiftHeadAddress(long desiredHeadAddress)
        {
            //obtain local values of variables that can change
            long currentFlushedUntilAddress = FlushedUntilAddress;

            long newHeadAddress = desiredHeadAddress;
            if (currentFlushedUntilAddress < newHeadAddress)
            {
                newHeadAddress = currentFlushedUntilAddress;
            }

            if (MonotonicUpdate(ref HeadAddress, newHeadAddress, out long oldHeadAddress))
            {
                Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
                return newHeadAddress;
            }
            return oldHeadAddress;
        }

        /// <summary>
        /// Called whenever a new tail page is allocated or when the user is checking for a failed memory allocation
        /// Tries to shift head address based on the head offset lag size.
        /// </summary>
        /// <param name="desiredHeadAddress"></param>
        private void PageAlignedShiftHeadAddressToValue(long desiredHeadAddress)
        {
            //obtain local values of variables that can change
            long currentHeadAddress = HeadAddress;
            long currentFlushedUntilAddress = FlushedUntilAddress;

            desiredHeadAddress = desiredHeadAddress & ~PageSizeMask;

            long newHeadAddress = desiredHeadAddress;
            if (currentFlushedUntilAddress < newHeadAddress)
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
        protected void ShiftFlushedUntilAddress()
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

        /// <summary>
        /// Reset for recovery
        /// </summary>
        /// <param name="tailAddress"></param>
        /// <param name="headAddress"></param>
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
                PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus = PMMCloseStatus.Open;
            }
        }

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        internal void AsyncReadRecordToMemory(long fromLogical, int numBytes, IOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default(SectorAlignedMemory))
        {
            ulong fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            ulong alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            uint alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = readBufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numBytes;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext<Key, Value>>);
            asyncResult.context = context;
            asyncResult.context.record = record;
            device.ReadAsync(alignedFileOffset,
                        (IntPtr)asyncResult.context.record.aligned_pointer,
                        alignedReadLength,
                        callback,
                        asyncResult);
        }

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="readPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="devicePageOffset"></param>
        /// <param name="logDevice"></param>
        /// <param name="objectLogDevice"></param>
        public void AsyncReadPagesFromDevice<TContext>(
                                long readPageStart,
                                int numPages,
                                IOCompletionCallback callback,
                                TContext context,
                                long devicePageOffset = 0,
                                IDevice logDevice = null, IDevice objectLogDevice = null)
        {
            AsyncReadPagesFromDevice(readPageStart, numPages, callback, context,
                out CountdownEvent completed, devicePageOffset, logDevice, objectLogDevice);
        }

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="readPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="completed"></param>
        /// <param name="devicePageOffset"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        private void AsyncReadPagesFromDevice<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        IOCompletionCallback callback,
                                        TContext context,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null, IDevice objectLogDevice = null)
        {
            var usedDevice = device;
            IDevice usedObjlogDevice = objectLogDevice;

            if (device == null)
            {
                usedDevice = this.device;
            }

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % BufferSize);
                if (!IsAllocated(pageIndex))
                {
                    // Allocate a new page
                    AllocatePage(pageIndex);
                }
                else
                {
                    ClearPage(pageIndex, readPage == 0);
                }
                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context,
                    handle = completed,
                    count = 1
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                ReadAsync(offsetInFile, pageIndex, (uint)PageSize, callback, asyncResult, usedDevice, usedObjlogDevice);
            }
        }

        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="untilAddress"></param>
        public void AsyncFlushPages(long startPage, long untilAddress)
        {
            long endPage = (untilAddress >> LogPageSizeBits);
            int numPages = (int)(endPage - startPage);
            long offsetInEndPage = GetOffsetInPage(untilAddress);
            if (offsetInEndPage > 0)
            {
                numPages++;
            }


            /* Request asynchronous writes to the device. If waitForPendingFlushComplete
             * is set, then a CountDownEvent is set in the callback handle.
             */
            for (long flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                long pageStartAddress = flushPage << LogPageSizeBits;
                long pageEndAddress = (flushPage + 1) << LogPageSizeBits;

                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    page = flushPage,
                    count = 1
                };
                if (pageEndAddress > untilAddress)
                {
                    asyncResult.partial = true;
                    asyncResult.untilAddress = untilAddress;
                }
                else
                {
                    asyncResult.partial = false;
                    asyncResult.untilAddress = pageEndAddress;

                    // Set status to in-progress
                    PageStatusIndicator[flushPage % BufferSize].PageFlushCloseStatus
                        = new FlushCloseStatus { PageFlushStatus = PMMFlushStatus.InProgress, PageCloseStatus = PMMCloseStatus.Open };
                }

                PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress = -1;

                WriteAsync(flushPage, AsyncFlushPageCallback, asyncResult);
            }
        }

        /// <summary>
        /// Flush pages asynchronously
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="flushPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void AsyncFlushPages<TContext>(
                                        long flushPageStart,
                                        int numPages,
                                        IOCompletionCallback callback,
                                        TContext context)
        {
            for (long flushPage = flushPageStart; flushPage < (flushPageStart + numPages); flushPage++)
            {
                int pageIndex = GetPageIndexForPage(flushPage);
                var asyncResult = new PageAsyncFlushResult<TContext>()
                {
                    page = flushPage,
                    context = context,
                    count = 1,
                    partial = false,
                    untilAddress = (flushPage + 1) << LogPageSizeBits
                };

                WriteAsync(flushPage, callback, asyncResult);
            }
        }

        /// <summary>
        /// Flush pages from startPage (inclusive) to endPage (exclusive)
        /// to specified log device and obj device
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="endPage"></param>
        /// <param name="endLogicalAddress"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="completed"></param>
        public void AsyncFlushPagesToDevice(long startPage, long endPage, long endLogicalAddress, IDevice device, IDevice objectLogDevice, out CountdownEvent completed)
        {
            int totalNumPages = (int)(endPage - startPage);
            completed = new CountdownEvent(totalNumPages);

            for (long flushPage = startPage; flushPage < endPage; flushPage++)
            {
                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    handle = completed,
                    count = 1
                };

                var pageSize = PageSize;

                if (flushPage == endPage - 1)
                    pageSize = (int)(endLogicalAddress - (flushPage << LogPageSizeBits));

                // Intended destination is flushPage
                WriteAsyncToDevice(startPage, flushPage, pageSize, AsyncFlushPageToDeviceCallback, asyncResult, device, objectLogDevice);
            }
        }

        /// <summary>
        /// Async get from disk
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        public void AsyncGetFromDisk(long fromLogical,
                              int numBytes,
                              AsyncIOContext<Key, Value> context,
                              SectorAlignedMemory result = default(SectorAlignedMemory))
        {
            while (numPendingReads > 120)
            {
                Thread.SpinWait(100);

                // Do not protect if we are not already protected
                // E.g., we are in an IO thread
                if (epoch.IsProtected())
                    epoch.ProtectAndDrain();
            }
            Interlocked.Increment(ref numPendingReads);

            if (result.buffer == null)
                AsyncReadRecordToMemory(fromLogical, numBytes, AsyncGetFromDiskCallback, context, result);
            else
                AsyncReadRecordObjectsToMemory(fromLogical, numBytes, AsyncGetFromDiskCallback, context, result);
        }

        private void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            var result = (AsyncGetFromDiskResult<AsyncIOContext<Key, Value>>)Overlapped.Unpack(overlap).AsyncResult;
            Interlocked.Decrement(ref numPendingReads);

            var ctx = result.context;

            var record = ctx.record.GetValidPointer();
            int requiredBytes = GetRecordSize((long)record);
            if (ctx.record.available_bytes >= requiredBytes)
            {
                // We have the complete record.
                if (RetrievedFullRecord(record, ref ctx))
                {
                    if (comparer.Equals(ref ctx.request_key, ref ctx.key))
                    {
                        // The keys are same, so I/O is complete
                        // ctx.record = result.record;
                        ctx.callbackQueue.Add(ctx);
                    }
                    else
                    {
                        var oldAddress = ctx.logicalAddress;

                        //keys are not same. I/O is not complete
                        ctx.logicalAddress = GetInfoFromBytePointer(record).PreviousAddress;
                        if (ctx.logicalAddress != Constants.kInvalidAddress)
                        {
                            ctx.record.Return();
                            ctx.record = ctx.objBuffer = default(SectorAlignedMemory);
                            AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, ctx);
                        }
                        else
                        {
                            ctx.callbackQueue.Add(ctx);
                        }
                    }
                }
            }
            else
            {
                ctx.record.Return();
                AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, ctx);
            }

            Overlapped.Free(overlap);
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="overlap"></param>
        private void AsyncFlushPageCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            // Set the page status to flushed
            PageAsyncFlushResult<Empty> result = (PageAsyncFlushResult<Empty>)Overlapped.Unpack(overlap).AsyncResult;

            if (Interlocked.Decrement(ref result.count) == 0)
            {
                PageStatusIndicator[result.page % BufferSize].LastFlushedUntilAddress = result.untilAddress;

                if (!result.partial)
                {
                    while (true)
                    {
                        var oldStatus = PageStatusIndicator[result.page % BufferSize].PageFlushCloseStatus;
                        if (oldStatus.PageCloseStatus == PMMCloseStatus.Closed)
                        {
                            ClearPage((int)(result.page % BufferSize), result.page == 0);
                        }
                        var newStatus = oldStatus;
                        newStatus.PageFlushStatus = PMMFlushStatus.Flushed;
                        if (oldStatus.value == Interlocked.CompareExchange(ref PageStatusIndicator[result.page % BufferSize].PageFlushCloseStatus.value, newStatus.value, oldStatus.value))
                        {
                            break;
                        }
                    }
                }
                ShiftFlushedUntilAddress();
                result.Free();
            }

            Overlapped.Free(overlap);
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="overlap"></param>
        private void AsyncFlushPageToDeviceCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            PageAsyncFlushResult<Empty> result = (PageAsyncFlushResult<Empty>)Overlapped.Unpack(overlap).AsyncResult;

            if (Interlocked.Decrement(ref result.count) == 0)
            {
                result.Free();
            }
            Overlapped.Free(overlap);
        }

        /// <summary>
        /// Shallow copy
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        public virtual void ShallowCopy(ref Key src, ref Key dst)
        {
            dst = src;
        }

        /// <summary>
        /// Shallow copy
        /// </summary>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        public virtual void ShallowCopy(ref Value src, ref Value dst)
        {
            dst = src;
        }
    }
}

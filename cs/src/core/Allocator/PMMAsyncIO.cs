// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading;

namespace FASTER.core
{
    public unsafe struct AsyncIOContext
    {
        public long id;

        public IntPtr key;

        public long logicalAddress;

        public SectorAlignedMemory record;

        public SectorAlignedMemory objBuffer;

        public BlockingCollection<AsyncIOContext> callbackQueue;
    }
    
    /// <summary>
    /// Async IO related functions of PMM
    /// </summary>
    public unsafe partial class PersistentMemoryMalloc : IAllocator
    {
        // Size of object chunks beign written to storage
        const int kObjectBlockSize = 100 * (1 << 20);
        // Tail offsets per segment, in object log
        public long[] segmentOffsets = new long[SegmentBufferSize];

        #region Async file operations

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numRecords"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AsyncReadRecordToMemory(long fromLogical, int numRecords, IOCompletionCallback callback, AsyncIOContext context, SectorAlignedMemory result = default(SectorAlignedMemory))
        {
            ulong fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            ulong alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            uint alignedReadLength = (uint)((long)fileOffset + numRecords - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = readBufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numRecords;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext>);
            asyncResult.context = context;
            if (result.buffer == null)
            {
                asyncResult.context.record = record;
                device.ReadAsync(alignedFileOffset,
                            (IntPtr)asyncResult.context.record.aligned_pointer,
                            alignedReadLength,
                            callback,
                            asyncResult);
            }
            else
            {
                asyncResult.context.record = result;
                asyncResult.context.objBuffer = record;
                objectLogDevice.ReadAsync(
                    (int)(context.logicalAddress >> LogSegmentSizeBits),
                    alignedFileOffset,
                    (IntPtr)asyncResult.context.objBuffer.aligned_pointer,
                    alignedReadLength,
                    callback,
                    asyncResult);
            }
        }

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

        public void AsyncReadPagesFromDevice<TContext>(
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
                usedObjlogDevice = this.objectLogDevice;
            }

            if (Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize())
            {
                if (usedObjlogDevice == null)
                    throw new Exception("Object log device not provided");
            }

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % BufferSize);
                if (values[pageIndex] == null)
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

                ReadAsync(offsetInFile, (IntPtr)pointers[pageIndex], PageSize, callback, asyncResult, usedDevice, usedObjlogDevice);
            }
        }

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

                WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                                  (ulong)(AlignedPageSizeBytes * flushPage),
                                  PageSize, callback, asyncResult, device, objectLogDevice);
            }
        }


        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="untilAddress"></param>
        private void AsyncFlushPages(long startPage, long untilAddress)
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
                        = new FlushCloseStatus { PageFlushStatus = FlushStatus.InProgress, PageCloseStatus = CloseStatus.Open };
                }

                PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress = -1;

                WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                                    (ulong)(AlignedPageSizeBytes * flushPage),
                                    PageSize,
                                    AsyncFlushPageCallback,
                                    asyncResult, device, objectLogDevice);
            }
        }

        /// <summary>
        /// Flush pages from startPage (inclusive) to endPage (exclusive)
        /// to specified log device and obj device
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="endPage"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="completed"></param>
        public void AsyncFlushPagesToDevice(long startPage, long endPage, IDevice device, IDevice objectLogDevice, out CountdownEvent completed)
        {
            int totalNumPages = (int)(endPage - startPage);
            completed = new CountdownEvent(totalNumPages);

            // We are writing to separate device, so use fresh segment offsets
            var _segmentOffsets = new long[SegmentBufferSize];

            for (long flushPage = startPage; flushPage < endPage; flushPage++)
            {
                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    handle = completed,
                    count = 1
                };

                long pageStartAddress = flushPage << LogPageSizeBits;
                long pageEndAddress = (flushPage + 1) << LogPageSizeBits;

                // Intended destination is flushPage
                WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                            (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                            PageSize,
                            AsyncFlushPageToDeviceCallback,
                            asyncResult, device, objectLogDevice, flushPage, _segmentOffsets);
            }
        }

        private void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                                IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                                IDevice device, IDevice objlogDevice, long intendedDestinationPage = -1, long[] localSegmentOffsets = null)
        {
            

            if (!Key.HasObjectsToSerialize() && !Value.HasObjectsToSerialize())
            {
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress,
                    numBytesToWrite, callback, asyncResult);
                return;
            }

            // Check if user did not override with special segment offsets
            if (localSegmentOffsets == null) localSegmentOffsets = segmentOffsets;

            // need to write both page and object cache
            asyncResult.count++;
            
            var buffer = ioBufferPool.Get(PageSize);
            Buffer.MemoryCopy((void*)alignedSourceAddress, buffer.aligned_pointer, numBytesToWrite, numBytesToWrite);

            long ptr = (long)buffer.aligned_pointer;
            List<long> addr = new List<long>();
            asyncResult.freeBuffer1 = buffer;

            // Correct for page 0 of HLOG
            if (intendedDestinationPage < 0)
            {
                // By default, when we are not writing to a separate device, the intended 
                // destination page (logical) is the same as actual
                intendedDestinationPage = (long)(alignedDestinationAddress >> LogPageSizeBits);
            }

            if (intendedDestinationPage == 0)
                ptr += Constants.kFirstValidAddress;

            var untilptr = (long)buffer.aligned_pointer + numBytesToWrite;


            while (ptr < untilptr)
            {
                MemoryStream ms = new MemoryStream();
                pageHandlers.Serialize(ref ptr, untilptr, ms, kObjectBlockSize, out List<long> addresses);
                var _s = ms.ToArray();
                ms.Close();

                var _objBuffer = ioBufferPool.Get(_s.Length);

                asyncResult.done = new AutoResetEvent(false);

                var _alignedLength = (_s.Length + (sectorSize - 1)) & ~(sectorSize - 1);

                var _objAddr = Interlocked.Add(ref localSegmentOffsets[(alignedDestinationAddress >> LogSegmentSizeBits) % SegmentBufferSize], _alignedLength) - _alignedLength;
                fixed (void* src = _s)
                    Buffer.MemoryCopy(src, _objBuffer.aligned_pointer, _s.Length, _s.Length);

                foreach (var address in addresses)
                    *((long*)address) += _objAddr;

                if (ptr < untilptr)
                {
                    objlogDevice.WriteAsync(
                        (IntPtr)_objBuffer.aligned_pointer,
                        (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                        (ulong)_objAddr, (uint)_alignedLength, AsyncFlushPartialObjectLogCallback<TContext>, asyncResult);

                    // Wait for write to complete before resuming next write
                    asyncResult.done.WaitOne();
                    _objBuffer.Return();
                }
                else
                {
                    objlogDevice.WriteAsync(
                        (IntPtr)_objBuffer.aligned_pointer,
                        (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                        (ulong)_objAddr, (uint)_alignedLength, callback, asyncResult);
                }
            }

            // Finally write the hlog page
            device.WriteAsync((IntPtr)buffer.aligned_pointer, alignedDestinationAddress,
                numBytesToWrite, callback, asyncResult);
        }

        private void ReadAsync<TContext>(
            ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length,
            IOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            if (!Key.HasObjectsToSerialize() && !Value.HasObjectsToSerialize())
            {
                device.ReadAsync(alignedSourceAddress, alignedDestinationAddress,
                    aligned_read_length, callback, asyncResult);
                return;
            }

            asyncResult.callback = callback;
            asyncResult.count++;
            asyncResult.objlogDevice = objlogDevice;

            device.ReadAsync(alignedSourceAddress, alignedDestinationAddress,
                    aligned_read_length, AsyncReadPageWithObjectsCallback<TContext>, asyncResult);
        }
        #endregion


        #region Async callbacks



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
                        if (oldStatus.PageCloseStatus == CloseStatus.Closed)
                        {
                            ClearPage((int)(result.page % BufferSize), result.page == 0);
                        }
                        var newStatus = oldStatus;
                        newStatus.PageFlushStatus = FlushStatus.Flushed;
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
        private void AsyncFlushPartialObjectLogCallback<TContext>(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            // Set the page status to flushed
            PageAsyncFlushResult<TContext> result = (PageAsyncFlushResult<TContext>)Overlapped.Unpack(overlap).AsyncResult;
            result.done.Set();

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

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            PageAsyncReadResult<TContext> result = (PageAsyncReadResult<TContext>)Overlapped.Unpack(overlap).AsyncResult;

            long ptr = pointers[result.page % BufferSize];
            
            // Correct for page 0 of HLOG
            if (result.page == 0)
                ptr += Constants.kFirstValidAddress;

            // Check if we are resuming
            if (result.resumeptr > ptr)
                ptr = result.resumeptr;

            // Deserialize all objects until untilptr
            if (ptr < result.untilptr)
            {
                MemoryStream ms = new MemoryStream(result.freeBuffer1.buffer);
                ms.Seek(result.freeBuffer1.offset + result.freeBuffer1.valid_offset, SeekOrigin.Begin);

                pageHandlers.Deserialize(ptr, result.untilptr, ms);

                ms.Dispose();
                result.freeBuffer1.Return();
                result.freeBuffer1.buffer = null;
                result.resumeptr = ptr;
            }

            // If we have processed entire page, return
            if (ptr >= (long)pointers[result.page % BufferSize] + PageSize)
            {

                result.Free();

                // Call the "real" page read callback
                result.callback(errorCode, numBytes, overlap);
                return;
            }

            // We will be re-issuing I/O, so free current overlap
            Overlapped.Free(overlap);

            long minObjAddress = long.MaxValue;
            long maxObjAddress = long.MinValue;
            while (ptr < pointers[result.page % BufferSize] + PageSize)
            {
                if (!Layout.GetInfo(ptr)->Invalid)
                {

                    if (Key.HasObjectsToSerialize())
                    {
                        Key* key = Layout.GetKey(ptr);
                        var addr = ((AddressInfo*)key)->Address;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > kObjectBlockSize))
                        {
                            // First address after kObjectSize would be aligned at sector boundary
                            Debug.Assert(maxObjAddress % sectorSize == 0);
                            break;
                        }

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += ((AddressInfo*)key)->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;
                    }


                    if (Value.HasObjectsToSerialize())
                    {
                        Value* value = Layout.GetValue(ptr);
                        var addr = ((AddressInfo*)value)->Address;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > kObjectBlockSize))
                        {
                            // First address after kObjectSize would be aligned at sector boundary
                            Debug.Assert(maxObjAddress % sectorSize == 0);
                            break;
                        }

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += ((AddressInfo*)value)->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;
                    }
                }
                ptr += Layout.GetPhysicalSize(ptr);
            }

            // We will be able to process all records until (but not including) ptr
            result.untilptr = ptr;

            // Object log fragment should be aligned by construction
            Debug.Assert(minObjAddress % sectorSize == 0);

            var to_read_long = maxObjAddress - minObjAddress;
            if (to_read_long > int.MaxValue)
                throw new Exception("Unable to read object page, total size greater than 2GB: " + to_read_long);

            var to_read = (int)to_read_long;

            // Handle the case where no objects are to be written
            if (minObjAddress == long.MaxValue && maxObjAddress == long.MinValue)
            {
                minObjAddress = 0;
                maxObjAddress = 0;
                to_read = 0;
            }

            var objBuffer = ioBufferPool.Get(to_read);
            result.freeBuffer1 = objBuffer;
            var alignedLength = (to_read + (sectorSize - 1)) & ~(sectorSize - 1);

            // Request objects from objlog
            result.objlogDevice.ReadAsync(
                (int)(result.page >> (LogSegmentSizeBits-LogPageSizeBits)),
                (ulong)minObjAddress, 
                (IntPtr)objBuffer.aligned_pointer, (uint)alignedLength, AsyncReadPageWithObjectsCallback<TContext>, result);

        }
        #endregion
    }
}

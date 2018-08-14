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
    public unsafe partial class FasterKV : FASTERBase, IFASTER
    {

        private void AsyncGetFromDisk(long fromLogical, 
                                      int numRecords, 
                                      IOCompletionCallback callback, 
                                      AsyncIOContext context, 
                                      SectorAlignedMemory result = default(SectorAlignedMemory))
        {
            //Debugger.Break();
            while (numPendingReads > 120)
            {
                Thread.SpinWait(100);

                // Do not protect if we are not already protected
                // E.g., we are in an IO thread
                if (epoch.IsProtected())
                    epoch.ProtectAndDrain();
            }
            Interlocked.Increment(ref numPendingReads);
            hlog.AsyncReadRecordToMemory(fromLogical, numRecords, callback, context, result);
        }

        private bool RetrievedObjects(byte* record, AsyncIOContext ctx)
        {
            if (!Key.HasObjectsToSerialize() && !Value.HasObjectsToSerialize())
                return true;

            if (ctx.objBuffer.buffer == null)
            {
                // Issue IO for objects
                long startAddress = -1;
                int numBytes = 0;
                if (Key.HasObjectsToSerialize())
                {
                    var x = (AddressInfo*)Layout.GetKey((long)record);
                    if (x->IsDiskAddress)
                    {
                        numBytes += x->Size;
                        startAddress = x->Address;
                    }
                }

                if (Value.HasObjectsToSerialize())
                {
                    var x = (AddressInfo*)Layout.GetValue((long)record);
                    if (x->IsDiskAddress)
                    {
                        numBytes += x->Size;
                        if (startAddress == -1)
                            startAddress = x->Address;
                    }
                }

                AsyncGetFromDisk(startAddress, numBytes, 
                    AsyncGetFromDiskCallback, ctx, ctx.record);
                return false;
            }

            // Parse the key and value objects
            MemoryStream ms = new MemoryStream(ctx.objBuffer.buffer);
            ms.Seek(ctx.objBuffer.offset + ctx.objBuffer.valid_offset, SeekOrigin.Begin);
            Key.Deserialize(Layout.GetKey((long)record), ms);
            Value.Deserialize(Layout.GetValue((long)record), ms);
            ctx.objBuffer.Return();
            return true;
        }


        protected void AsyncGetFromDiskCallback(
                    uint errorCode,
                    uint numBytes,
                    NativeOverlapped* overlap)
        {
            //Debugger.Break();
            var result = (AsyncGetFromDiskResult<AsyncIOContext>)Overlapped.Unpack(overlap).AsyncResult;
            try
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Completion Callback error, {0}", ex.Message);
            }
            finally
            {
                Interlocked.Decrement(ref numPendingReads);

                var ctx = result.context;
                var record = ctx.record.GetValidPointer();
                if (Layout.HasTotalRecord(record, ctx.record.available_bytes, out int requiredBytes))
                {
                    //We have the complete record.
                    if (RetrievedObjects(record, ctx))
                    {
                        if (Key.Equals(ctx.key, Layout.GetKey((long)record)))
                        {
                            //The keys are same, so I/O is complete
                            // ctx.record = result.record;
                            ctx.callbackQueue.Add(ctx);
                        }
                        else
                        {
                            var oldAddress = ctx.logicalAddress;

                            //keys are not same. I/O is not complete
                            ctx.logicalAddress = ((RecordInfo*)record)->PreviousAddress;
                            if (ctx.logicalAddress != Constants.kInvalidAddress)
                            {

                                // Delete key, value, record
                                if (Key.HasObjectsToSerialize())
                                {
                                    var physicalAddress = (long)ctx.record.GetValidPointer();
                                    Key.Free(Layout.GetKey(physicalAddress));
                                }
                                if (Value.HasObjectsToSerialize())
                                {
                                    var physicalAddress = (long)ctx.record.GetValidPointer();
                                    Value.Free(Layout.GetValue(physicalAddress));
                                }
                                ctx.record.Return();
                                AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, AsyncGetFromDiskCallback, ctx);
                            }
                            else
                            {
                                //Console.WriteLine("Lookup Address = " + oldAddress);
                                //Console.WriteLine("RecordInfo: " + RecordInfo.ToString((RecordInfo*)record));
                                // Console.WriteLine("Record not found. Looking for: " + ctx.key->value + " found: " + Layout.GetKey((long)record)->value + " req bytes: " + requiredBytes);
                                ctx.callbackQueue.Add(ctx);
                            }
                        }
                    }
                }
                else
                {
                    ctx.record.Return();
                    AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, AsyncGetFromDiskCallback, ctx);
                }

                Overlapped.Free(overlap);
            }
        }



    }


    public unsafe partial class PersistentMemoryMalloc<T> : IAllocator
    {
        #region Async file operations

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="fromLogical"></param>
        /// <param name="numRecords"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AsyncReadRecordToMemory(long fromLogical, int numRecords, IOCompletionCallback callback, AsyncIOContext context, SectorAlignedMemory result = default(SectorAlignedMemory))
        {
            ulong fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + PrivateRecordSize * (fromLogical & PageSizeMask));
            ulong alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            uint alignedReadLength = (uint)((long)fileOffset + (numRecords * PrivateRecordSize) - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = readBufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numRecords * RecordSize;

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
                objlogDevice.ReadAsync(
                    (int)(context.logicalAddress >> LogSegmentSizeBits),
                    alignedFileOffset,
                    (IntPtr)asyncResult.context.objBuffer.aligned_pointer,
                    alignedReadLength,
                    callback,
                    asyncResult);
            }
        }

        private ISegmentedDevice CreateObjectLogDevice(IDevice device)
        {
            if ((device as NullDevice) != null)
            {
                return new SegmentedNullDevice();
            }
            else if ((device as LocalStorageDevice) != null)
            {
                string filename = (device as LocalStorageDevice).GetFileName();
                filename = filename.Substring(0, filename.Length - 4) + ".objlog";
                return new SegmentedLocalStorageDevice(filename, -1, false, false, true);
            }
            else if ((device as SegmentedLocalStorageDevice) != null)
            {
                string filename = (device as SegmentedLocalStorageDevice).GetFileName();
                filename = filename.Substring(0, filename.Length - 4) + ".objlog";
                return new SegmentedLocalStorageDevice(filename, -1, false, false, true);
            }
            else if ((device as WrappedDevice) != null)
            {
                var ud = (device as WrappedDevice).GetUnderlyingDevice() as SegmentedLocalStorageDevice;
                if (ud != null)
                {
                    string filename = ud.GetFileName();
                    filename = filename.Substring(0, filename.Length - 4) + ".objlog";
                    return new SegmentedLocalStorageDevice(filename, -1, false, false, true);
                }
            }

            Console.WriteLine("Unable to create object log device");
            return null;
        }

        public void AsyncReadPagesFromDevice(int numPages, long destinationStartPage, IDevice device, out CountdownEvent completed)
        {
            var asyncResult = new PageAsyncFlushResult();

            ISegmentedDevice objlogDevice = null;
            if (Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize())
            {
                objlogDevice = CreateObjectLogDevice(device);
                throw new Exception("Reading pages with object log not yet supported");
            }
            completed = new CountdownEvent(numPages);
            asyncResult.handle = completed;
            asyncResult.objlogDevice = objlogDevice;

            for (long flushPage = destinationStartPage; flushPage < destinationStartPage + numPages; flushPage++)
            {
                long pageStartAddress = flushPage << LogPageSizeBits;
                long pageEndAddress = (flushPage + 1) << LogPageSizeBits;

                device.ReadAsync(
                    (ulong)(AlignedPageSizeBytes * (flushPage - destinationStartPage)),
                    pointers[flushPage % BufferSize],
                    (uint)(PageSize * PrivateRecordSize),
                    AsyncReadPageFromDeviceCallback,
                    asyncResult);
            }
        }


        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="numPages"></param>
        /// <param name="waitForPendingFlushComplete"></param>
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

                var asyncResult = new PageAsyncFlushResult();
                asyncResult.page = flushPage;
                asyncResult.count = 1;
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

                WriteAsync(pointers[flushPage % BufferSize],
                                    (ulong)(AlignedPageSizeBytes * flushPage),
                                    (uint)(PageSize * PrivateRecordSize),
                                    AsyncFlushPageCallback,
                                    asyncResult, device, objlogDevice);
            }
        }

        /// <summary>
        /// Flush pages from startPage (inclusive) to endPage (exclusive)
        /// to specified log device and obj device
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="endPage"></param>
        /// <param name="device"></param>
        public void AsyncFlushPagesToDevice(long startPage, long endPage, IDevice device, out CountdownEvent completed)
        {
            var asyncResult = new PageAsyncFlushResult();

            int numPages = (int)(endPage - startPage);

            ISegmentedDevice objlogDevice = null;
            if (Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize())
            {
                numPages = numPages * 2;
                objlogDevice = CreateObjectLogDevice(device);
            }

            completed = new CountdownEvent(numPages);
            asyncResult.handle = completed;
            for (long flushPage = startPage; flushPage < endPage; flushPage++)
            {
                long pageStartAddress = flushPage << LogPageSizeBits;
                long pageEndAddress = (flushPage + 1) << LogPageSizeBits;

                WriteAsync(pointers[flushPage % BufferSize],
                            (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                            (uint)(PageSize * PrivateRecordSize),
                            AsyncFlushPageToDeviceCallback,
                            asyncResult, device, objlogDevice);
            }
        }


        long[] segmentOffsets = new long[SegmentBufferSize];

        private void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                                IOCompletionCallback callback, PageAsyncFlushResult asyncResult,
                                IDevice device, ISegmentedDevice objlogDevice)
        {
            // Debugger.Break();

            if (!Key.HasObjectsToSerialize() && !Value.HasObjectsToSerialize())
            {
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress,
                    numBytesToWrite, callback, asyncResult);
                return;
            }

            // need to write both page and object cache
            asyncResult.count++;
            //if (alignedDestinationAddress == 0)
            //    Debugger.Break();
            MemoryStream ms = new MemoryStream();
            var buffer = ioBufferPool.Get(PageSize);
            Buffer.MemoryCopy((void*)alignedSourceAddress, buffer.aligned_pointer, numBytesToWrite, numBytesToWrite);

            long ptr = (long)buffer.aligned_pointer;
            List<long> addr = new List<long>();
            asyncResult.freeBuffer1 = buffer;

            // Correct for page 0 of HLOG
            if (alignedDestinationAddress >> LogPageSizeBits == 0)
                ptr += Constants.kFirstValidAddress;

            while (ptr < (long)buffer.aligned_pointer + numBytesToWrite)
            {
                if (!Layout.GetInfo(ptr)->Invalid)
                {
                    long pos = ms.Position;

                    Key* key = Layout.GetKey(ptr);
                    Key.Serialize(key, ms);
                    ((AddressInfo*)key)->IsDiskAddress = true;
                    ((AddressInfo*)key)->Address = pos;
                    ((AddressInfo*)key)->Size = (int)(ms.Position - pos);
                    addr.Add((long)key);

                    pos = ms.Position;
                    Value* value = Layout.GetValue(ptr);
                    Value.Serialize(value, ms);
                    ((AddressInfo*)value)->IsDiskAddress = true;
                    ((AddressInfo*)value)->Address = pos;
                    ((AddressInfo*)value)->Size = (int)(ms.Position - pos);
                    addr.Add((long)value);
                }
                ptr += Layout.GetPhysicalSize(ptr);
            }


            var s = ms.ToArray();
            var objBuffer = ioBufferPool.Get(s.Length);
            asyncResult.freeBuffer2 = objBuffer;

            var alignedLength = (s.Length + (sectorSize - 1)) & ~(sectorSize - 1);

            var objAddr = Interlocked.Add(ref segmentOffsets[(alignedDestinationAddress >> LogSegmentSizeBits) % SegmentBufferSize], alignedLength) - alignedLength;
            fixed (void* src = s)
                Buffer.MemoryCopy(src, objBuffer.aligned_pointer, s.Length, s.Length);

            foreach (var address in addr)
            {
                *((long*)address) += objAddr;
            }

            objlogDevice.WriteAsync(
                (IntPtr)objBuffer.aligned_pointer,
                (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                (ulong)objAddr, (uint)alignedLength, callback, asyncResult);

            device.WriteAsync((IntPtr)buffer.aligned_pointer, alignedDestinationAddress,
                numBytesToWrite, callback, asyncResult);
        }
        #endregion


        #region Async callbacks

        /// <summary>
        /// IOCompletion callback for page read
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="overlap"></param>
        private void AsyncReadPageFromDeviceCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            PageAsyncFlushResult result = (PageAsyncFlushResult)Overlapped.Unpack(overlap).AsyncResult;
            if (result.handle != null)
                result.handle.Signal();
            Overlapped.Free(overlap);

            if (Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize())
            {
                throw new NotImplementedException("Recovery of object log not yet supported");
            }
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
                System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            //Set the page status to flushed
            PageAsyncFlushResult result = (PageAsyncFlushResult)Overlapped.Unpack(overlap).AsyncResult;

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

                Interlocked.MemoryBarrier();

                if (result.freeBuffer1.buffer != null)
                    result.freeBuffer1.Return();
                if (result.freeBuffer2.buffer != null)
                    result.freeBuffer2.Return();

                if (result.handle != null)
                {
                    result.handle.Signal();
                }
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
                System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            PageAsyncFlushResult result = (PageAsyncFlushResult)Overlapped.Unpack(overlap).AsyncResult;

            if (result.freeBuffer1.buffer != null)
                result.freeBuffer1.Return();
            if (result.freeBuffer2.buffer != null)
                result.freeBuffer2.Return();

            if (result.handle != null)
                result.handle.Signal();
            Overlapped.Free(overlap);
        }
        #endregion
    }
}

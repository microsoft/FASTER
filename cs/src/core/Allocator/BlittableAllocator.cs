﻿// Copyright (c) Microsoft Corporation. All rights reserved.
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

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    public unsafe sealed class BlittableAllocator<Key, Value> : AllocatorBase<Key, Value>
        where Key : IKey<Key>, new()
        where Value : IValue<Value>, new()
    {
        // Circular buffer definition
        private byte[][] values;
        private GCHandle[] handles;
        private long[] pointers;
        private readonly GCHandle ptrHandle;
        private readonly long* nativePointers;

        // Segment size
        private readonly int LogSegmentSizeBits;
        private readonly long SegmentSize;
        private readonly long SegmentSizeMask;
        private readonly int SegmentBufferSize;

        // Object log related variables
        private readonly IDevice objectLogDevice;
        // Size of object chunks beign written to storage
        private const int kObjectBlockSize = 100 * (1 << 20);
        // Tail offsets per segment, in object log
        public readonly long[] segmentOffsets;
        // Buffer pool for object log related work
        SectorAlignedBufferPool ioBufferPool;

        public BlittableAllocator(LogSettings settings)
            : base(settings)
        {
            // Segment size
            LogSegmentSizeBits = settings.SegmentSizeBits;
            SegmentSize = 1 << LogSegmentSizeBits;
            SegmentSizeMask = SegmentSize - 1;
            SegmentBufferSize = 1 +
                (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

            values = new byte[BufferSize][];
            handles = new GCHandle[BufferSize];
            pointers = new long[BufferSize];
            segmentOffsets = new long[SegmentBufferSize];

            objectLogDevice = settings.ObjectLogDevice;

            if (KeyHasObjects() || ValueHasObjects())
            {
                if (objectLogDevice == null)
                    throw new Exception("Objects in key/value, but object log not provided during creation of FASTER instance");
            }

            epoch = LightEpoch.Instance;
            ioBufferPool = SectorAlignedBufferPool.GetPool(1, sectorSize);

            ptrHandle = GCHandle.Alloc(pointers, GCHandleType.Pinned);
            nativePointers = (long*)ptrHandle.AddrOfPinnedObject();
        }

        public override void Initialize()
        {
            Initialize(Constants.kFirstValidAddress);
        }

        public override ref RecordInfo GetInfo(long physicalAddress)
        {
            return ref Unsafe.AsRef<RecordInfo>((void*)physicalAddress);
        }

        public override ref Key GetKey(long physicalAddress)
        {
            return ref Unsafe.AsRef<Key>((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override ref Value GetValue(long physicalAddress)
        {
            return ref Unsafe.AsRef<Value>((byte*)physicalAddress + RecordInfo.GetLength() + default(Key).GetLength());
        }

        public override int GetRecordSize(long physicalAddress)
        {
            return RecordInfo.GetLength() + default(Key).GetLength() + default(Value).GetLength();
        }

        public override int GetAverageRecordSize()
        {
            return RecordInfo.GetLength() + default(Key).GetLength() + default(Value).GetLength();
        }

        public override int GetInitialRecordSize(ref Key key, int valueLength)
        {
            return RecordInfo.GetLength() + key.GetLength() + valueLength;
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
                values[i] = null;
            }
            handles = null;
            pointers = null;
            values = null;
            base.Dispose();
        }

        public override AddressInfo* GetKeyAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength() + default(Key).GetLength());
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        protected override void AllocatePage(int index)
        {
            var adjustedSize = PageSize + 2 * sectorSize;
            byte[] tmp = new byte[adjustedSize];
            Array.Clear(tmp, 0, adjustedSize);

            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
            pointers[index] = (p + (sectorSize - 1)) & ~(sectorSize - 1);
            values[index] = tmp;

            PageStatusIndicator[index].PageFlushCloseStatus.PageFlushStatus = PMMFlushStatus.Flushed;
            PageStatusIndicator[index].PageFlushCloseStatus.PageCloseStatus = PMMCloseStatus.Closed;
            Interlocked.MemoryBarrier();
        }

        public override long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            int pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return *(nativePointers + pageIndex) + offset;
        }

        protected override bool IsAllocated(int pageIndex)
        {
            return values[pageIndex] != null;
        }

        protected override void DeleteAddressRange(long fromAddress, long toAddress)
        {
            base.DeleteAddressRange(fromAddress, toAddress);
            objectLogDevice.DeleteSegmentRange((int)(fromAddress >> LogSegmentSizeBits), (int)(toAddress >> LogSegmentSizeBits));
        }

        protected override void WriteAsync<TContext>(long flushPage, IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)PageSize,
                    callback,
                    asyncResult, device, objectLogDevice);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, IOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice)
        {
            // We are writing to separate device, so use fresh segment offsets
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)PageSize, callback, asyncResult,
                        device, objectLogDevice, flushPage, new long[SegmentBufferSize]);
        }

        /// <summary>
        /// Get start logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public override long GetStartLogicalAddress(long page)
        {
            if (page == 0)
                return (page << LogPageSizeBits) + Constants.kFirstValidAddress;

            return page << LogPageSizeBits;
        }


        protected override void ClearPage(int page, bool pageZero)
        {
            Array.Clear(values[page], 0, values[page].Length);

            // Close segments
            var thisCloseSegment = page >> (LogSegmentSizeBits - LogPageSizeBits);
            var nextClosePage = page + 1;
            var nextCloseSegment = nextClosePage >> (LogSegmentSizeBits - LogPageSizeBits);

            if (thisCloseSegment != nextCloseSegment)
            {
                // Last page in current segment
                segmentOffsets[thisCloseSegment % SegmentBufferSize] = 0;
            }
        }

        private void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                        IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, IDevice objlogDevice, long intendedDestinationPage = -1, long[] localSegmentOffsets = null)
        {
            if (!(KeyHasObjects() || ValueHasObjects()))
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
                Serialize(ref ptr, untilptr, ms, kObjectBlockSize, out List<long> addresses);
                var _s = ms.ToArray();
                ms.Close();

                var _objBuffer = ioBufferPool.Get(_s.Length);

                asyncResult.done = new AutoResetEvent(false);

                var _alignedLength = (_s.Length + (sectorSize - 1)) & ~(sectorSize - 1);

                var _objAddr = Interlocked.Add(ref localSegmentOffsets[(long)(alignedDestinationAddress >> LogSegmentSizeBits) % SegmentBufferSize], _alignedLength) - _alignedLength;
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
                    asyncResult.freeBuffer2 = _objBuffer;
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

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            IOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            if (!(KeyHasObjects() || ValueHasObjects()))
            {
                device.ReadAsync(alignedSourceAddress, (IntPtr)pointers[destinationPageIndex],
                    aligned_read_length, callback, asyncResult);
                return;
            }

            asyncResult.callback = callback;
            asyncResult.count++;

            if (objlogDevice == null)
            {
                Debug.Assert(objectLogDevice != null);
                objlogDevice = objectLogDevice;
            }
            asyncResult.objlogDevice = objlogDevice;

            device.ReadAsync(alignedSourceAddress, (IntPtr)pointers[destinationPageIndex],
                    aligned_read_length, AsyncReadPageWithObjectsCallback<TContext>, asyncResult);
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
                Deserialize(ptr, result.untilptr, ms);
                ms.Dispose();

                ptr = result.untilptr;
                result.freeBuffer1.Return();
                result.freeBuffer1.buffer = null;
                result.resumeptr = ptr;
            }

            // If we have processed entire page, return
            if (ptr >= pointers[result.page % BufferSize] + PageSize)
            {

                result.Free();

                // Call the "real" page read callback
                result.callback(errorCode, numBytes, overlap);
                return;
            }

            // We will be re-issuing I/O, so free current overlap
            Overlapped.Free(overlap);

            GetObjectInfo(ref ptr, pointers[result.page % BufferSize] + PageSize, kObjectBlockSize, out long startptr, out long size);

            // Object log fragment should be aligned by construction
            Debug.Assert(startptr % sectorSize == 0);

            // We will be able to process all records until (but not including) ptr
            result.untilptr = ptr;

            if (size > int.MaxValue)
                throw new Exception("Unable to read object page, total size greater than 2GB: " + size);

            var objBuffer = ioBufferPool.Get((int)size);
            result.freeBuffer1 = objBuffer;
            var alignedLength = (size + (sectorSize - 1)) & ~(sectorSize - 1);

            // Request objects from objlog
            result.objlogDevice.ReadAsync(
                (int)(result.page >> (LogSegmentSizeBits - LogPageSizeBits)),
                (ulong)startptr,
                (IntPtr)objBuffer.aligned_pointer, (uint)alignedLength, AsyncReadPageWithObjectsCallback<TContext>, result);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, IOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default(SectorAlignedMemory))
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


        #region Page handlers for objects

        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        public void Deserialize(long ptr, long untilptr, Stream stream)
        {
            while (ptr < untilptr)
            {
                if (!GetInfo(ptr).Invalid)
                {
                    if (KeyHasObjects())
                        GetKey(ptr).Deserialize(stream);
                    if (ValueHasObjects())
                        GetValue(ptr).Deserialize(stream);
                }
                ptr += GetRecordSize(ptr);
            }
        }

        /// <summary>
        /// Serialize part of page to stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        /// <param name="objectBlockSize">Size of blocks to serialize in chunks of</param>
        /// <param name="addr">List of addresses that need to be updated with offsets</param>
        public void Serialize(ref long ptr, long untilptr, Stream stream, int objectBlockSize, out List<long> addr)
        {
            addr = new List<long>();
            while (ptr < untilptr)
            {
                if (!GetInfo(ptr).Invalid)
                {
                    long pos = stream.Position;

                    if (KeyHasObjects())
                    {
                        GetKey(ptr).Serialize(stream);
                        var key_address = GetKeyAddressInfo(ptr);
                        key_address->Address = pos;
                        key_address->Size = (int)(stream.Position - pos);
                        addr.Add((long)key_address);
                    }

                    if (ValueHasObjects())
                    {
                        pos = stream.Position;
                        var value_address = GetValueAddressInfo(ptr);
                        GetValue(ptr).Serialize(stream);
                        value_address->Address = pos;
                        value_address->Size = (int)(stream.Position - pos);
                        addr.Add((long)value_address);
                    }

                }
                ptr += GetRecordSize(ptr);

                if (stream.Position > objectBlockSize)
                    return;
            }
        }

        /// <summary>
        /// Get location and range of object log addresses for specified log page
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="untilptr"></param>
        /// <param name="objectBlockSize"></param>
        /// <param name="startptr"></param>
        /// <param name="size"></param>
        public void GetObjectInfo(ref long ptr, long untilptr, int objectBlockSize, out long startptr, out long size)
        {
            long minObjAddress = long.MaxValue;
            long maxObjAddress = long.MinValue;

            while (ptr < untilptr)
            {
                if (!GetInfo(ptr).Invalid)
                {

                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo(ptr);
                        var addr = key_addr->Address;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > objectBlockSize))
                        {
                            break;
                        }

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += key_addr->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;
                    }


                    if (ValueHasObjects())
                    {
                        var value_addr = GetValueAddressInfo(ptr);
                        var addr = value_addr->Address;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > objectBlockSize))
                        {
                            break;
                        }

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += value_addr->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;
                    }
                }
                ptr += GetRecordSize(ptr);
            }

            // Handle the case where no objects are to be written
            if (minObjAddress == long.MaxValue && maxObjAddress == long.MinValue)
            {
                minObjAddress = 0;
                maxObjAddress = 0;
            }

            startptr = minObjAddress;
            size = maxObjAddress - minObjAddress;
        }

        /// <summary>
        /// Retrieve objects from object log
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected override bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx)
        {
            if (!KeyHasObjects())
                GetKey((long)record).ShallowCopy(ref ctx.key);

            if (!ValueHasObjects())
                GetValue((long)record).ShallowCopy(ref ctx.value);

            if (!(KeyHasObjects() || ValueHasObjects()))
                return true;

            if (ctx.objBuffer.buffer == null)
            {
                // Issue IO for objects
                long startAddress = -1;
                long endAddress = -1;
                if (KeyHasObjects())
                {
                    var x = GetKeyAddressInfo((long)record);
                    startAddress = x->Address;
                    endAddress = x->Address + x->Size;
                }

                if (ValueHasObjects())
                {
                    var x = GetValueAddressInfo((long)record);
                    if (startAddress == -1)
                        startAddress = x->Address;
                    endAddress = x->Address + x->Size;
                }

                // We are limited to a 2GB size per key-value
                if (endAddress - startAddress > int.MaxValue)
                    throw new Exception("Size of key-value exceeds max of 2GB: " + (endAddress - startAddress));

                AsyncGetFromDisk(startAddress, (int)(endAddress - startAddress), ctx, ctx.record);
                return false;
            }

            // Parse the key and value objects
            MemoryStream ms = new MemoryStream(ctx.objBuffer.buffer);
            ms.Seek(ctx.objBuffer.offset + ctx.objBuffer.valid_offset, SeekOrigin.Begin);

            if (KeyHasObjects())
            {
                ctx.key = new Key();
                ctx.key.Deserialize(ms);
            }

            if (ValueHasObjects())
            {
                ctx.value = new Value();
                ctx.value.Deserialize(ms);
            }

            ctx.objBuffer.Return();
            return true;
        }

        /// <summary>
        /// Whether KVS has keys to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool KeyHasObjects()
        {
            return default(Key).HasObjectsToSerialize();
        }

        /// <summary>
        /// Whether KVS has values to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool ValueHasObjects()
        {
            return default(Value).HasObjectsToSerialize();
        }
        #endregion

        public override long[] GetSegmentOffsets()
        {
            return segmentOffsets;
        }

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new Exception("Memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
        }
    }
}

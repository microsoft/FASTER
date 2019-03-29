// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    [StructLayout(LayoutKind.Sequential, Pack=1)]
    public struct Record<Key, Value>
    {
        public RecordInfo info;
        public Key key;
        public Value value;
    }


    public unsafe sealed class GenericAllocator<Key, Value> : AllocatorBase<Key, Value>
        where Key : new()
        where Value : new()
    {
        // Circular buffer definition
        internal Record<Key, Value>[][] values;

        // Object log related variables
        private readonly IDevice objectLogDevice;
        // Size of object chunks beign written to storage
        private const int ObjectBlockSize = 100 * (1 << 20);
        // Tail offsets per segment, in object log
        public readonly long[] segmentOffsets;
        // Buffer pool for object log related work
        SectorAlignedBufferPool ioBufferPool;
        // Record sizes
        private static readonly int recordSize = Utility.GetSize(default(Record<Key, Value>));
        private readonly SerializerSettings<Key, Value> SerializerSettings;

        public GenericAllocator(LogSettings settings, SerializerSettings<Key, Value> serializerSettings, IFasterEqualityComparer<Key> comparer, Action<long, long> evictCallback = null)
            : base(settings, comparer, evictCallback)
        {
            SerializerSettings = serializerSettings;

            if (default(Key) == null && (settings.LogDevice as NullDevice == null) && ((SerializerSettings == null) || (SerializerSettings.keySerializer == null)))
            {
                throw new Exception("Key is a class, but no serializer specified via SerializerSettings");
            }

            if (default(Value) == null && (settings.LogDevice as NullDevice == null) && ((SerializerSettings == null) || (SerializerSettings.valueSerializer == null)))
            {
                throw new Exception("Value is a class, but no serializer specified via SerializerSettings");
            }

            values = new Record<Key, Value>[BufferSize][];
            segmentOffsets = new long[SegmentBufferSize];

            objectLogDevice = settings.ObjectLogDevice;

            if ((settings.LogDevice as NullDevice == null) && (KeyHasObjects() || ValueHasObjects()))
            {
                if (objectLogDevice == null)
                    throw new Exception("Objects in key/value, but object log not provided during creation of FASTER instance");
            }

            epoch = LightEpoch.Instance;
            ioBufferPool = SectorAlignedBufferPool.GetPool(1, sectorSize);
        }

        public override void Initialize()
        {
            Initialize(recordSize);
        }

        /// <summary>
        /// Get start logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public override long GetStartLogicalAddress(long page)
        {
            return page << LogPageSizeBits;
        }

        /// <summary>
        /// Get first valid logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public override long GetFirstValidLogicalAddress(long page)
        {
            if (page == 0)
                return (page << LogPageSizeBits) + recordSize;

            return page << LogPageSizeBits;
        }

        public override ref RecordInfo GetInfo(long physicalAddress)
        {
            // Offset within page
            int offset = (int)(physicalAddress & PageSizeMask);

            // Index of page within the circular buffer
            int pageIndex = (int)((physicalAddress >> LogPageSizeBits) & BufferSizeMask);

            return ref values[pageIndex][offset/recordSize].info;
        }

        public override ref RecordInfo GetInfoFromBytePointer(byte* ptr)
        {
            return ref Unsafe.AsRef<Record<Key, Value>>(ptr).info;
        }


        public override ref Key GetKey(long physicalAddress)
        {
            // Offset within page
            int offset = (int)(physicalAddress & PageSizeMask);

            // Index of page within the circular buffer
            int pageIndex = (int)((physicalAddress >> LogPageSizeBits) & BufferSizeMask);

            return ref values[pageIndex][offset / recordSize].key;
        }

        public override ref Value GetValue(long physicalAddress)
        {
            // Offset within page
            int offset = (int)(physicalAddress & PageSizeMask);

            // Index of page within the circular buffer
            int pageIndex = (int)((physicalAddress >> LogPageSizeBits) & BufferSizeMask);

            return ref values[pageIndex][offset / recordSize].value;
        }

        public override int GetRecordSize(long physicalAddress)
        {
            return recordSize;
        }

        public override int GetAverageRecordSize()
        {
            return recordSize;
        }

        public override int GetInitialRecordSize<Input>(ref Key key, ref Input input)
        {
            return recordSize;
        }

        public override int GetRecordSize(ref Key key, ref Value value)
        {
            return recordSize;
        }

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            if (values != null)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = null;
                }
                values = null;
            }
            base.Dispose();
        }

        /// <summary>
        /// Delete in-memory portion of the log
        /// </summary>
        internal override void DeleteFromMemory()
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = null;
            }
            values = null;
        }

        public override AddressInfo* GetKeyAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)Unsafe.AsPointer(ref Unsafe.AsRef<Record<Key, Value>>((byte*)physicalAddress).key);
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)Unsafe.AsPointer(ref Unsafe.AsRef<Record<Key, Value>>((byte*)physicalAddress).value);
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        protected override void AllocatePage(int index)
        {
            values[index] = AllocatePage();
            PageStatusIndicator[index].PageFlushCloseStatus.PageFlushStatus = PMMFlushStatus.Flushed;
            PageStatusIndicator[index].PageFlushCloseStatus.PageCloseStatus = PMMCloseStatus.Closed;
            Interlocked.MemoryBarrier();
        }

        internal Record<Key, Value>[] AllocatePage()
        {
            Record<Key, Value>[] tmp;
            if (PageSize % recordSize == 0)
                tmp = new Record<Key, Value>[PageSize / recordSize];
            else
                tmp = new Record<Key, Value>[1 + (PageSize / recordSize)];
            Array.Clear(tmp, 0, tmp.Length);
            return tmp;
        }

        public override long GetPhysicalAddress(long logicalAddress)
        {
            return logicalAddress;
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

        protected override void WriteAsync<TContext>(long flushPage, IOCompletionCallback callback,  PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync(flushPage,
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)PageSize,
                    callback,
                    asyncResult, device, objectLogDevice);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, IOCompletionCallback callback, 
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice)
        {
            // We are writing to separate device, so use fresh segment offsets
            WriteAsync(flushPage,
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)pageSize, callback, asyncResult, 
                        device, objectLogDevice, flushPage, new long[SegmentBufferSize]);
        }



        protected override void ClearPage(long page)
        {
            Array.Clear(values[page % BufferSize], 0, values[page % BufferSize].Length);

            // Close segments
            var thisCloseSegment = page >> (LogSegmentSizeBits - LogPageSizeBits);
            var nextCloseSegment = (page + 1) >> (LogSegmentSizeBits - LogPageSizeBits);

            if (thisCloseSegment != nextCloseSegment)
            {
                // We are clearing the last page in current segment
                segmentOffsets[thisCloseSegment % SegmentBufferSize] = 0;
            }
        }

        private void WriteAsync<TContext>(long flushPage, ulong alignedDestinationAddress, uint numBytesToWrite,
                        IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, IDevice objlogDevice, long intendedDestinationPage = -1, long[] localSegmentOffsets = null)
        {
            // Short circuit if we are using a null device
            if (device as NullDevice != null)
            {
                device.WriteAsync(IntPtr.Zero, 0, 0, numBytesToWrite, callback, asyncResult);
                return;
            }

            int start = 0, aligned_start = 0, end = (int)numBytesToWrite;
            if (asyncResult.partial)
            {
                start = (int)((asyncResult.fromAddress - (asyncResult.page << LogPageSizeBits)));
                aligned_start = (start / sectorSize) * sectorSize;
                end = (int)((asyncResult.untilAddress - (asyncResult.page << LogPageSizeBits)));
            }

            // Check if user did not override with special segment offsets
            if (localSegmentOffsets == null) localSegmentOffsets = segmentOffsets;

            var src = values[flushPage % BufferSize];
            var buffer = ioBufferPool.Get((int)numBytesToWrite);

            if (aligned_start < start && (KeyHasObjects() || ValueHasObjects()))
            {
                // Get the overlapping HLOG from disk as we wrote it with
                // object pointers previously. This avoids object reserialization
                PageAsyncReadResult<Empty> result =
                    new PageAsyncReadResult<Empty>
                    {
                        handle = new CountdownEvent(1)
                    };
                device.ReadAsync(alignedDestinationAddress + (ulong)aligned_start, (IntPtr)buffer.aligned_pointer + aligned_start,
                    (uint)sectorSize, AsyncReadPageCallback, result);
                result.handle.Wait();

                fixed (RecordInfo* pin = &src[0].info)
                {
                    Buffer.MemoryCopy((void*)((long)Unsafe.AsPointer(ref src[0]) + start), buffer.aligned_pointer + start, 
                        numBytesToWrite - start, numBytesToWrite - start);
                }
            }
            else
            {
                fixed (RecordInfo* pin = &src[0].info)
                {
                    Buffer.MemoryCopy((void*)((long)Unsafe.AsPointer(ref src[0]) + aligned_start), buffer.aligned_pointer + aligned_start, 
                        numBytesToWrite - aligned_start, numBytesToWrite - aligned_start);
                }
            }

            long ptr = (long)buffer.aligned_pointer;
            List<long> addr = new List<long>();
            asyncResult.freeBuffer1 = buffer;

            addr = new List<long>();
            MemoryStream ms = new MemoryStream();
            IObjectSerializer<Key> keySerializer = null;
            IObjectSerializer<Value> valueSerializer = null;

            if (KeyHasObjects())
            {
                keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginSerialize(ms);
            }
            if (ValueHasObjects())
            {
                valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginSerialize(ms);
            }


            for (int i=start/recordSize; i<end/recordSize; i++)
            {
                if (!src[i].info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        long pos = ms.Position;
                        keySerializer.Serialize(ref src[i].key);
                        var key_address = GetKeyAddressInfo((long)(buffer.aligned_pointer + i * recordSize));
                        key_address->Address = pos;
                        key_address->Size = (int)(ms.Position - pos);
                        addr.Add((long)key_address);
                    }

                    if (ValueHasObjects() && !src[i].info.Tombstone)
                    {
                        long pos = ms.Position;
                        valueSerializer.Serialize(ref src[i].value);
                        var value_address = GetValueAddressInfo((long)(buffer.aligned_pointer + i * recordSize));
                        value_address->Address = pos;
                        value_address->Size = (int)(ms.Position - pos);
                        addr.Add((long)value_address);
                    }
                }

                if (ms.Position > ObjectBlockSize || i == (end / recordSize) - 1)
                {
                    var _s = ms.ToArray();
                    ms.Close();
                    ms = new MemoryStream();

                    var _objBuffer = ioBufferPool.Get(_s.Length);

                    asyncResult.done = new AutoResetEvent(false);

                    var _alignedLength = (_s.Length + (sectorSize - 1)) & ~(sectorSize - 1);

                    var _objAddr = Interlocked.Add(ref localSegmentOffsets[(long)(alignedDestinationAddress >> LogSegmentSizeBits) % SegmentBufferSize], _alignedLength) - _alignedLength;
                    fixed (void* src_ = _s)
                        Buffer.MemoryCopy(src_, _objBuffer.aligned_pointer, _s.Length, _s.Length);

                    foreach (var address in addr)
                        ((AddressInfo*)address)->Address += _objAddr;

                    if (i < (end / recordSize) - 1)
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
                        // need to write both page and object cache
                        asyncResult.count++;

                        asyncResult.freeBuffer2 = _objBuffer;
                        objlogDevice.WriteAsync(
                            (IntPtr)_objBuffer.aligned_pointer,
                            (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                            (ulong)_objAddr, (uint)_alignedLength, callback, asyncResult);
                    }
                }
            }
            if (KeyHasObjects())
            {
                keySerializer.EndSerialize();
            }
            if (ValueHasObjects())
            {
                valueSerializer.EndSerialize();
            }

            if (asyncResult.partial)
            {
                var aligned_end = (int)((asyncResult.untilAddress - (asyncResult.page << LogPageSizeBits)));
                aligned_end = ((aligned_end + (sectorSize - 1)) & ~(sectorSize - 1));
                numBytesToWrite = (uint)(aligned_end - aligned_start);
            }

            var alignedNumBytesToWrite = (uint)((numBytesToWrite + (sectorSize - 1)) & ~(sectorSize - 1));

            // Finally write the hlog page
            device.WriteAsync((IntPtr)buffer.aligned_pointer + aligned_start, alignedDestinationAddress + (ulong)aligned_start,
                alignedNumBytesToWrite, callback, asyncResult);
        }

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            // Set the page status to flushed
            var result = (PageAsyncReadResult<Empty>)Overlapped.Unpack(overlap).AsyncResult;

            result.handle.Signal();

            Overlapped.Free(overlap);
        }

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            IOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            asyncResult.freeBuffer1 = readBufferPool.Get((int)aligned_read_length);
            asyncResult.freeBuffer1.required_bytes = (int)aligned_read_length;

            if (!(KeyHasObjects() || ValueHasObjects()))
            {
                device.ReadAsync(alignedSourceAddress, (IntPtr)asyncResult.freeBuffer1.aligned_pointer,
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

            device.ReadAsync(alignedSourceAddress, (IntPtr)asyncResult.freeBuffer1.aligned_pointer,
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

            var src = values[result.page % BufferSize];

            // We are reading into a frame
            if (result.frame != null)
            {
                var frame = (GenericFrame<Key, Value>)result.frame;
                src = frame.GetPage(result.page % frame.frameSize);

                if (result.freeBuffer1 != null && result.freeBuffer1.required_bytes > 0)
                {
                    PopulatePageFrame(result.freeBuffer1.GetValidPointer(), PageSize, src);
                    result.freeBuffer1.required_bytes = 0;
                }
            }
            else
            {
                if (result.freeBuffer1 != null && result.freeBuffer1.required_bytes > 0)
                {
                    PopulatePage(result.freeBuffer1.GetValidPointer(), PageSize, result.page);
                    result.freeBuffer1.required_bytes = 0;
                }
            }

            long ptr = 0;

            // Correct for page 0 of HLOG
            //if (result.page == 0)
            //    ptr += Constants.kFirstValidAddress;

            // Check if we are resuming
            if (result.resumeptr > ptr)
                ptr = result.resumeptr;

            // Deserialize all objects until untilptr
            if (ptr < result.untilptr)
            {
                MemoryStream ms = new MemoryStream(result.freeBuffer2.buffer);
                ms.Seek(result.freeBuffer2.offset, SeekOrigin.Begin);
                Deserialize(result.freeBuffer1.GetValidPointer(), ptr, result.untilptr, src, ms);
                ms.Dispose();

                ptr = result.untilptr;
                result.freeBuffer2.Return();
                result.freeBuffer2 = null;
                result.resumeptr = ptr;
            }

            // If we have processed entire page, return
            if (ptr >= PageSize)
            {
                result.Free();

                // Call the "real" page read callback
                result.callback(errorCode, numBytes, overlap);
                return;
            }

            // We will be re-issuing I/O, so free current overlap
            Overlapped.Free(overlap);

            GetObjectInfo(result.freeBuffer1.GetValidPointer(), ref ptr, PageSize, ObjectBlockSize, src, out long startptr, out long size);

            // Object log fragment should be aligned by construction
            Debug.Assert(startptr % sectorSize == 0);

            // We will be able to process all records until (but not including) ptr
            result.untilptr = ptr;

            if (size > int.MaxValue)
                throw new Exception("Unable to read object page, total size greater than 2GB: " + size);

            var objBuffer = ioBufferPool.Get((int)size);
            result.freeBuffer2 = objBuffer;
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

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="readPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="frame"></param>
        /// <param name="completed"></param>
        /// <param name="devicePageOffset"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        IOCompletionCallback callback,
                                        TContext context,
                                        GenericFrame<Key, Value> frame,
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
                int pageIndex = (int)(readPage % frame.frameSize);
                if (frame.GetPage(pageIndex) == null)
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
                    count = 1,
                    frame = frame
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                ReadAsync(offsetInFile, pageIndex, (uint)AlignedPageSizeBytes, callback, asyncResult, usedDevice, usedObjlogDevice);
            }
        }


        #region Page handlers for objects
        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="raw"></param>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="src"></param>
        /// <param name="stream">Stream</param>
        public void Deserialize(byte *raw, long ptr, long untilptr, Record<Key, Value>[] src, Stream stream)
        {
            IObjectSerializer<Key> keySerializer = null;
            IObjectSerializer<Value> valueSerializer = null;

            long streamStartPos = stream.Position;
            long start_addr = -1;
            if (KeyHasObjects())
            {
                keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginDeserialize(stream);
            }
            if (ValueHasObjects())
            {
                valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginDeserialize(stream);
            }

            while (ptr < untilptr)
            {
                if (!src[ptr / recordSize].info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo((long)raw + ptr);
                        if (start_addr == -1) start_addr = key_addr->Address;
                        if (stream.Position != streamStartPos + key_addr->Address - start_addr)
                        {
                            stream.Seek(streamStartPos + key_addr->Address - start_addr, SeekOrigin.Begin);
                        }

                        src[ptr/recordSize].key = new Key();
                        keySerializer.Deserialize(ref src[ptr/recordSize].key);
                    } 

                    if (ValueHasObjects() && !src[ptr / recordSize].info.Tombstone)
                    {
                        var value_addr = GetValueAddressInfo((long)raw + ptr);
                        if (start_addr == -1) start_addr = value_addr->Address;
                        if (stream.Position != streamStartPos + value_addr->Address - start_addr)
                        {
                            stream.Seek(streamStartPos + value_addr->Address - start_addr, SeekOrigin.Begin);
                        }

                        src[ptr / recordSize].value = new Value();
                        valueSerializer.Deserialize(ref src[ptr/recordSize].value);
                    }
                }
                ptr += GetRecordSize(ptr);
            }
            if (KeyHasObjects())
            {
                keySerializer.EndDeserialize();
            }
            if (ValueHasObjects())
            {
                valueSerializer.EndDeserialize();
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
            IObjectSerializer<Key> keySerializer = null;
            IObjectSerializer<Value> valueSerializer = null;

            if (KeyHasObjects())
            {
                keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginSerialize(stream);
            }
            if (ValueHasObjects())
            {
                valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginSerialize(stream);
            }

            addr = new List<long>();
            while (ptr < untilptr)
            {
                if (!GetInfo(ptr).Invalid)
                {
                    long pos = stream.Position;

                    if (KeyHasObjects())
                    {
                        keySerializer.Serialize(ref GetKey(ptr));
                        var key_address = GetKeyAddressInfo(ptr);
                        key_address->Address = pos;
                        key_address->Size = (int)(stream.Position - pos);
                        addr.Add((long)key_address);
                    }

                    if (ValueHasObjects() && !GetInfo(ptr).Tombstone)
                    {
                        pos = stream.Position;
                        var value_address = GetValueAddressInfo(ptr);
                        valueSerializer.Serialize(ref GetValue(ptr));
                        value_address->Address = pos;
                        value_address->Size = (int)(stream.Position - pos);
                        addr.Add((long)value_address);
                    }

                }
                ptr += GetRecordSize(ptr);

                if (stream.Position > objectBlockSize)
                    break;
            }

            if (KeyHasObjects())
            {
                keySerializer.EndSerialize();
            }
            if (ValueHasObjects())
            {
                valueSerializer.EndSerialize();
            }
        }

        /// <summary>
        /// Get location and range of object log addresses for specified log page
        /// </summary>
        /// <param name="raw"></param>
        /// <param name="ptr"></param>
        /// <param name="untilptr"></param>
        /// <param name="objectBlockSize"></param>
        /// <param name="src"></param>
        /// <param name="startptr"></param>
        /// <param name="size"></param>
        public void GetObjectInfo(byte* raw, ref long ptr, long untilptr, int objectBlockSize, Record<Key, Value>[] src, out long startptr, out long size)
        {
            long minObjAddress = long.MaxValue;
            long maxObjAddress = long.MinValue;

            while (ptr < untilptr)
            {
                if (!src[ptr/recordSize].info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo((long)raw + ptr);
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


                    if (ValueHasObjects() && !src[ptr / recordSize].info.Tombstone)
                    {
                        var value_addr = GetValueAddressInfo((long)raw + ptr);
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
            {
                ShallowCopy(ref Unsafe.AsRef<Record<Key, Value>>(record).key, ref ctx.key);
            }
            if (!ValueHasObjects())
            {
                ShallowCopy(ref Unsafe.AsRef<Record<Key, Value>>(record).value, ref ctx.value);
            }

            if (!(KeyHasObjects() || ValueHasObjects()))
                return true;

            if (ctx.objBuffer == null)
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

                if (ValueHasObjects() && !GetInfoFromBytePointer(record).Tombstone)
                {
                    var x = GetValueAddressInfo((long)record);
                    if (startAddress == -1)
                        startAddress = x->Address;
                    endAddress = x->Address + x->Size;
                }

                // We are limited to a 2GB size per key-value
                if (endAddress-startAddress > int.MaxValue)
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

                var keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginDeserialize(ms);
                keySerializer.Deserialize(ref ctx.key);
                keySerializer.EndDeserialize();
            }

            if (ValueHasObjects() && !GetInfoFromBytePointer(record).Tombstone)
            {
                ctx.value = new Value();

                var valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginDeserialize(ms);
                valueSerializer.Deserialize(ref ctx.value);
                valueSerializer.EndDeserialize();
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
            return SerializerSettings.keySerializer != null;
        }

        /// <summary>
        /// Whether KVS has values to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool ValueHasObjects()
        {
            return SerializerSettings.valueSerializer != null;
        }
        #endregion

        public override long[] GetSegmentOffsets()
        {
            return segmentOffsets;
        }

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            PopulatePage(src, required_bytes, ref values[destinationPage % BufferSize]);
        }

        internal void PopulatePageFrame(byte* src, int required_bytes, Record<Key, Value>[] frame)
        {
            PopulatePage(src, required_bytes, ref frame);
        }

        internal void PopulatePage(byte* src, int required_bytes, ref Record<Key, Value>[] destinationPage)
        {
            fixed (RecordInfo* pin = &destinationPage[0].info)
            {
                Buffer.MemoryCopy(src, Unsafe.AsPointer(ref destinationPage[0]), required_bytes, required_bytes);
            }
        }

        /// <summary>
        /// Iterator interface for scanning FASTER log
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <returns></returns>
        public override IFasterScanIterator<Key, Value> Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
        {
            return new GenericScanIterator<Key, Value>(this, beginAddress, endAddress, scanBufferingMode);
        }
    }
}

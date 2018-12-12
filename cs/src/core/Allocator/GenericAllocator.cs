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
    [StructLayout(LayoutKind.Sequential)]
    public struct Record<Key, Value>
    {
        public RecordInfo info;
        public Key key;
        public Value value;
    }


    public unsafe sealed class GenericAllocator<Key, Value> : AllocatorBase<Key, Value>
        where Key : IKey<Key>, new()
        where Value : new()
    {
        // Circular buffer definition
        private Record<Key, Value>[][] values;

        // Segment size
        private readonly int LogSegmentSizeBits;
        private readonly long SegmentSize;
        private readonly long SegmentSizeMask;
        private readonly int SegmentBufferSize;

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
        private static readonly int keySize = Utility.GetSize(default(Key));
        private readonly SerializerSettings<Key, Value> SerializerSettings;

        public GenericAllocator(LogSettings settings, SerializerSettings<Key, Value> serializerSettings)
            : base(settings)
        {
            SerializerSettings = serializerSettings;

            if (default(Key) == null && ((SerializerSettings == null) || (SerializerSettings.keySerializer == null)))
            {
                throw new Exception("Key is a class, but no serializer specified via SerializerSettings");
            }

            if (default(Value) == null && ((SerializerSettings == null) || (SerializerSettings.valueSerializer == null)))
            {
                throw new Exception("Value is a class, but no serializer specified via SerializerSettings");
            }

            // Segment size
            LogSegmentSizeBits = settings.SegmentSizeBits;
            SegmentSize = 1 << LogSegmentSizeBits;
            SegmentSizeMask = SegmentSize - 1;
            SegmentBufferSize = 1 +
                (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

            values = new Record<Key, Value>[BufferSize][];
            segmentOffsets = new long[SegmentBufferSize];

            objectLogDevice = settings.ObjectLogDevice;

            if (KeyHasObjects() || ValueHasObjects())
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
            return recordSize; // RecordInfo.GetLength() +  default(Key).GetLength() + default(Value).GetLength();
        }

        public override int GetAverageRecordSize()
        {
            return recordSize;// RecordInfo.GetLength() + default(Key).GetLength() + default(Value).GetLength();
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
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = null;
            }
            values = null;
            base.Dispose();
        }

        public override AddressInfo* GetKeyAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength() + keySize);
        }

        /// <summary>
        /// Allocate memory page, pinned in memory, and in sector aligned form, if possible
        /// </summary>
        /// <param name="index"></param>
        protected override void AllocatePage(int index)
        {
            Record<Key, Value>[] tmp;
            if (PageSize % recordSize == 0)
                tmp = new Record<Key, Value>[PageSize / recordSize];
            else
                tmp = new Record<Key, Value>[1 + (PageSize / recordSize)];
            Array.Clear(tmp, 0, tmp.Length);

            values[index] = tmp;
            PageStatusIndicator[index].PageFlushCloseStatus.PageFlushStatus = PMMFlushStatus.Flushed;
            PageStatusIndicator[index].PageFlushCloseStatus.PageCloseStatus = PMMCloseStatus.Closed;
            Interlocked.MemoryBarrier();
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
            (long startPage, long flushPage, IOCompletionCallback callback, 
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice)
        {
            // We are writing to separate device, so use fresh segment offsets
            WriteAsync(flushPage,
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)PageSize, callback, asyncResult, 
                        device, objectLogDevice, flushPage, new long[SegmentBufferSize]);
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

        private void WriteAsync<TContext>(long flushPage, ulong alignedDestinationAddress, uint numBytesToWrite,
                        IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device, IDevice objlogDevice, long intendedDestinationPage = -1, long[] localSegmentOffsets = null)
        {
            /*
            if (!(KeyHasObjects() || ValueHasObjects()))
            {
                device.WriteAsync((IntPtr)pointers[flushPage % BufferSize], alignedDestinationAddress,
                    numBytesToWrite, callback, asyncResult);
                return;
            }
            */

            // Check if user did not override with special segment offsets
            if (localSegmentOffsets == null) localSegmentOffsets = segmentOffsets;

            // need to write both page and object cache
            asyncResult.count++;

            var src = values[flushPage % BufferSize];
            var buffer = ioBufferPool.Get(PageSize);

            fixed (RecordInfo* pin = &src[0].info)
            {
                Buffer.MemoryCopy((void*)pin, buffer.aligned_pointer, numBytesToWrite, numBytesToWrite);
            }

            long ptr = (long)buffer.aligned_pointer;
            List<long> addr = new List<long>();
            asyncResult.freeBuffer1 = buffer;

            // Correct for page 0 of HLOG
            //if (intendedDestinationPage < 0)
            //{
            // By default, when we are not writing to a separate device, the intended 
            // destination page (logical) is the same as actual
            //  intendedDestinationPage = (long)(alignedDestinationAddress >> LogPageSizeBits);
            //}

            //if (intendedDestinationPage == 0)
            //    ptr += Constants.kFirstValidAddress;

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
            for (int i=0; i<numBytesToWrite/recordSize; i++)
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

                    if (ValueHasObjects())
                    {
                        long pos = ms.Position;
                        valueSerializer.Serialize(ref src[i].value);
                        var value_address = GetKeyAddressInfo((long)(buffer.aligned_pointer + i * recordSize + keySize));
                        value_address->Address = pos;
                        value_address->Size = (int)(ms.Position - pos);
                        addr.Add((long)value_address);
                    }
                }

                if (ms.Position > ObjectBlockSize || i == numBytesToWrite/recordSize - 1)
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
                        *((long*)address) += _objAddr;

                    if (i < numBytesToWrite / recordSize - 1)
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
            }
            if (KeyHasObjects())
            {
                keySerializer.EndSerialize();
            }
            if (ValueHasObjects())
            {
                valueSerializer.EndSerialize();
            }

            // Finally write the hlog page
            device.WriteAsync((IntPtr)buffer.aligned_pointer, alignedDestinationAddress,
                numBytesToWrite, callback, asyncResult);
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

            if (result.freeBuffer1.buffer != null && result.freeBuffer1.required_bytes > 0)
            {
                PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.required_bytes = 0;
            }

            var src = values[result.page % BufferSize];

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
                ms.Seek(result.freeBuffer2.offset + result.freeBuffer2.valid_offset, SeekOrigin.Begin);
                Deserialize(ptr, result.untilptr, ms);
                ms.Dispose();

                ptr = result.untilptr;
                result.freeBuffer2.Return();
                result.freeBuffer2.buffer = null;
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

            GetObjectInfo(result.freeBuffer1.GetValidPointer(), ref ptr, PageSize, ObjectBlockSize, out long startptr, out long size);

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




        #region Page handlers for objects
        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        public void Deserialize(long ptr, long untilptr, Stream stream)
        {
            IObjectSerializer<Key> keySerializer = null;
            IObjectSerializer<Value> valueSerializer = null;

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
                if (!GetInfo(ptr).Invalid)
                {
                    if (KeyHasObjects())
                    {
                        GetKey(ptr) = new Key();
                        keySerializer.Deserialize(ref GetKey(ptr));
                    } 

                    if (ValueHasObjects())
                    {
                        GetValue(ptr) = new Value();
                        valueSerializer.Deserialize(ref GetValue(ptr));
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

                    if (ValueHasObjects())
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
        /// <param name="startptr"></param>
        /// <param name="size"></param>
        public void GetObjectInfo(byte* raw, ref long ptr, long untilptr, int objectBlockSize, out long startptr, out long size)
        {
            long minObjAddress = long.MaxValue;
            long maxObjAddress = long.MinValue;

            while (ptr < untilptr)
            {
                if (!GetInfo(ptr).Invalid)
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


                    if (ValueHasObjects())
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
                ShallowCopy(ref Unsafe.AsRef<Key>(record + RecordInfo.GetLength()), ref ctx.key);
            }
            if (!ValueHasObjects())
            {
                ShallowCopy(ref Unsafe.AsRef<Value>(record + RecordInfo.GetLength() + keySize), ref ctx.value);
            }

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

            if (ValueHasObjects())
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
            fixed (RecordInfo* pin = &values[destinationPage % BufferSize][0].info)
            {
                Buffer.MemoryCopy(src, (void*)pin, required_bytes, required_bytes);
            }
        }
    }
}

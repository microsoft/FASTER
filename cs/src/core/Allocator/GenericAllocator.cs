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
    {
        // Circular buffer definition
        internal Record<Key, Value>[][] values;

        // Object log related variables
        private readonly IDevice objectLogDevice;
        // Size of object chunks beign written to storage
        private const int ObjectBlockSize = 100 * (1 << 20);
        // Tail offsets per segment, in object log
        public readonly long[] segmentOffsets;
        // Record sizes
        private static readonly int recordSize = Utility.GetSize(default(Record<Key, Value>));
        private readonly SerializerSettings<Key, Value> SerializerSettings;
        private readonly bool keyBlittable = Utility.IsBlittable<Key>();
        private readonly bool valueBlittable = Utility.IsBlittable<Value>();

        public GenericAllocator(LogSettings settings, SerializerSettings<Key, Value> serializerSettings, IFasterEqualityComparer<Key> comparer, Action<long, long> evictCallback = null, LightEpoch epoch = null, Action<CommitInfo> flushCallback = null)
            : base(settings, comparer, evictCallback, epoch, flushCallback)
        {
            if (settings.ObjectLogDevice == null)
            {
                throw new FasterException("LogSettings.ObjectLogDevice needs to be specified (e.g., use Devices.CreateLogDevice, AzureStorageDevice, or NullDevice)");
            }

            SerializerSettings = serializerSettings ?? new SerializerSettings<Key, Value>();

            if ((!keyBlittable) && (settings.LogDevice as NullDevice == null) && ((SerializerSettings == null) || (SerializerSettings.keySerializer == null)))
            {
                Debug.WriteLine("Key is not blittable, but no serializer specified via SerializerSettings. Using (slow) DataContractSerializer as default.");
                SerializerSettings.keySerializer = ObjectSerializer.Get<Key>();
            }

            if ((!valueBlittable) && (settings.LogDevice as NullDevice == null) && ((SerializerSettings == null) || (SerializerSettings.valueSerializer == null)))
            {
                Debug.WriteLine("Value is not blittable, but no serializer specified via SerializerSettings. Using (slow) DataContractSerializer as default.");
                SerializerSettings.valueSerializer = ObjectSerializer.Get<Value>();
            }

            values = new Record<Key, Value>[BufferSize][];
            segmentOffsets = new long[SegmentBufferSize];

            objectLogDevice = settings.ObjectLogDevice;

            if ((settings.LogDevice as NullDevice == null) && (KeyHasObjects() || ValueHasObjects()))
            {
                if (objectLogDevice == null)
                    throw new FasterException("Objects in key/value, but object log not provided during creation of FASTER instance");
                if (objectLogDevice.SegmentSize != -1)
                    throw new FasterException("Object log device should not have fixed segment size. Set preallocateFile to false when calling CreateLogDevice for object log");
            }
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

        public override (int, int) GetRecordSize(long physicalAddress)
        {
            return (recordSize, recordSize);
        }

        public override (int, int) GetRecordSize<Input, FasterSession>(long physicalAddress, ref Input input, FasterSession fasterSession)
        {
            return (recordSize, recordSize);
        }

        public override int GetAverageRecordSize()
        {
            return recordSize;
        }

        public override int GetFixedRecordSize() => recordSize;

        public override (int, int) GetInitialRecordSize<Input, FasterSession>(ref Key key, ref Input input, FasterSession fasterSession)
        {
            return (recordSize, recordSize);
        }

        public override (int, int) GetRecordSize(ref Key key, ref Value value)
        {
            return (recordSize, recordSize);
        }

        internal override bool TryComplete()
        {
            var b1 = objectLogDevice.TryComplete();
            var b2 = base.TryComplete();
            return b1 || b2;
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
        internal override void AllocatePage(int index)
        {
            values[index] = AllocatePage();
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

        internal override bool IsAllocated(int pageIndex)
        {
            return values[pageIndex] != null;
        }

        protected override void TruncateUntilAddress(long toAddress)
        {
            base.TruncateUntilAddress(toAddress);
            objectLogDevice.TruncateUntilSegment((int)(toAddress >> LogSegmentSizeBits));
        }

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback,  PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync(flushPage,
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)PageSize,
                    callback,
                    asyncResult, device, objectLogDevice);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets)
        {
            bool epochTaken = false;
            if (!epoch.ThisInstanceProtected())
            {
                epochTaken = true;
                epoch.Resume();
            }
            try
            {
                if (FlushedUntilAddress < (flushPage << LogPageSizeBits) + pageSize)
                {
                    // We are writing to separate device, so use fresh segment offsets
                    WriteAsync(flushPage,
                            (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                            (uint)pageSize, callback, asyncResult,
                            device, objectLogDevice, flushPage, localSegmentOffsets);
                }
            }
            finally
            {
                if (epochTaken)
                    epoch.Suspend();
            }
        }



        internal override void ClearPage(long page, int offset)
        {
            Array.Clear(values[page % BufferSize], offset / recordSize, values[page % BufferSize].Length - offset / recordSize);

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
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
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
            var buffer = bufferPool.Get((int)numBytesToWrite);

            if (aligned_start < start && (KeyHasObjects() || ValueHasObjects()))
            {
                // Do not read back the invalid header of page 0
                if ((flushPage > 0) || (start > GetFirstValidLogicalAddress(flushPage)))
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
                }
                fixed (RecordInfo* pin = &src[0].info)
                {
                    Debug.Assert(buffer.aligned_pointer + numBytesToWrite <= (byte*)buffer.handle.AddrOfPinnedObject() + buffer.buffer.Length);

                    Buffer.MemoryCopy((void*)((long)Unsafe.AsPointer(ref src[0]) + start), buffer.aligned_pointer + start, 
                        numBytesToWrite - start, numBytesToWrite - start);
                }
            }
            else
            {
                fixed (RecordInfo* pin = &src[0].info)
                {
                    Debug.Assert(buffer.aligned_pointer + numBytesToWrite <= (byte*)buffer.handle.AddrOfPinnedObject() + buffer.buffer.Length);

                    Buffer.MemoryCopy((void*)((long)Unsafe.AsPointer(ref src[0]) + aligned_start), buffer.aligned_pointer + aligned_start, 
                        numBytesToWrite - aligned_start, numBytesToWrite - aligned_start);
                }
            }

            long ptr = (long)buffer.aligned_pointer;
            List<long> addr = new List<long>();
            asyncResult.freeBuffer1 = buffer;

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
                    var memoryStreamLength = (int)ms.Position;

                    var _objBuffer = bufferPool.Get(memoryStreamLength);

                    asyncResult.done = new AutoResetEvent(false);

                    var _alignedLength = (memoryStreamLength + (sectorSize - 1)) & ~(sectorSize - 1);

                    var _objAddr = Interlocked.Add(ref localSegmentOffsets[(long)(alignedDestinationAddress >> LogSegmentSizeBits) % SegmentBufferSize], _alignedLength) - _alignedLength;

                    if (KeyHasObjects())
                        keySerializer.EndSerialize();
                    if (ValueHasObjects())
                        valueSerializer.EndSerialize();

                    ms.Close();

                    fixed (void* src_ = ms.GetBuffer())
                        Buffer.MemoryCopy(src_, _objBuffer.aligned_pointer, memoryStreamLength, memoryStreamLength);

                    foreach (var address in addr)
                        ((AddressInfo*)address)->Address += _objAddr;


                    if (i < (end / recordSize) - 1)
                    {
                        ms = new MemoryStream();
                        if (KeyHasObjects())
                            keySerializer.BeginSerialize(ms);
                        if (ValueHasObjects())
                            valueSerializer.BeginSerialize(ms);

                        // Reset address list for next chunk
                        addr = new List<long>();

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
                        Interlocked.Increment(ref asyncResult.count);

                        asyncResult.freeBuffer2 = _objBuffer;
                        objlogDevice.WriteAsync(
                            (IntPtr)_objBuffer.aligned_pointer,
                            (int)(alignedDestinationAddress >> LogSegmentSizeBits),
                            (ulong)_objAddr, (uint)_alignedLength, callback, asyncResult);
                    }
                }
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

        private void AsyncReadPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncReadPageCallback error: {0}", errorCode);
            }

            // Set the page status to flushed
            var result = (PageAsyncReadResult<Empty>)context;

            result.handle.Signal();
        }

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            asyncResult.freeBuffer1 = bufferPool.Get((int)aligned_read_length);
            asyncResult.freeBuffer1.required_bytes = (int)aligned_read_length;

            if (!(KeyHasObjects() || ValueHasObjects()))
            {
                device.ReadAsync(alignedSourceAddress, (IntPtr)asyncResult.freeBuffer1.aligned_pointer,
                    aligned_read_length, callback, asyncResult);
                return;
            }

            asyncResult.callback = callback;

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
        /// <param name="context"></param>
        private void AsyncFlushPartialObjectLogCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
               Trace.TraceError("AsyncFlushPartialObjectLogCallback error: {0}", errorCode);
            }

            // Set the page status to flushed
            PageAsyncFlushResult<TContext> result = (PageAsyncFlushResult<TContext>)context;
            result.done.Set();
        }

        private void AsyncReadPageWithObjectsCallback<TContext>(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncReadPageWithObjectsCallback error: {0}", errorCode);
            }

            PageAsyncReadResult<TContext> result = (PageAsyncReadResult<TContext>)context;

            Record<Key, Value>[] src;

            // We are reading into a frame
            if (result.frame != null)
            {
                var frame = (GenericFrame<Key, Value>)result.frame;
                src = frame.GetPage(result.page % frame.frameSize);
            }
            else
                src = values[result.page % BufferSize];


            // Deserialize all objects until untilptr
            if (result.resumePtr < result.untilPtr)
            {
                MemoryStream ms = new MemoryStream(result.freeBuffer2.buffer);
                ms.Seek(result.freeBuffer2.offset, SeekOrigin.Begin);
                Deserialize(result.freeBuffer1.GetValidPointer(), result.resumePtr, result.untilPtr, src, ms);
                ms.Dispose();

                result.freeBuffer2.Return();
                result.freeBuffer2 = null;
                result.resumePtr = result.untilPtr;
            }

            // If we have processed entire page, return
            if (result.untilPtr >= result.maxPtr)
            {
                result.Free();

                // Call the "real" page read callback
                result.callback(errorCode, numBytes, context);
                return;
            }

            // We will now be able to process all records until (but not including) untilPtr
            GetObjectInfo(result.freeBuffer1.GetValidPointer(), ref result.untilPtr, result.maxPtr, ObjectBlockSize, out long startptr, out long size);

            // Object log fragment should be aligned by construction
            Debug.Assert(startptr % sectorSize == 0);

            if (size > int.MaxValue)
                throw new FasterException("Unable to read object page, total size greater than 2GB: " + size);

            var alignedLength = (size + (sectorSize - 1)) & ~((long)sectorSize - 1);
            var objBuffer = bufferPool.Get((int)alignedLength);
            result.freeBuffer2 = objBuffer;

            // Request objects from objlog
            result.objlogDevice.ReadAsync(
                (int)((result.page - result.offset) >> (LogSegmentSizeBits - LogPageSizeBits)),
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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default)
        {
            ulong fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            ulong alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            uint alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = bufferPool.Get((int)alignedReadLength);
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
        /// <param name="untilAddress"></param>
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
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
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
                    maxPtr = PageSize,
                    frame = frame,
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);
                uint readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = (AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask));

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    asyncResult.maxPtr = readLength;
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                ReadAsync(offsetInFile, pageIndex, readLength, callback, asyncResult, usedDevice, usedObjlogDevice);
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
                ref Record<Key, Value> record = ref Unsafe.AsRef<Record<Key, Value>>(raw + ptr);
                src[ptr / recordSize].info = record.info;

                if (!record.info.Invalid)
                {
                    if (KeyHasObjects())
                    {
                        var key_addr = GetKeyAddressInfo((long)raw + ptr);
                        if (start_addr == -1) start_addr = key_addr->Address;
                        if (stream.Position != streamStartPos + key_addr->Address - start_addr)
                        {
                            stream.Seek(streamStartPos + key_addr->Address - start_addr, SeekOrigin.Begin);
                        }

                        keySerializer.Deserialize(out src[ptr/recordSize].key);
                    }
                    else
                    {
                        src[ptr / recordSize].key = record.key;
                    }

                    if (!record.info.Tombstone)
                    {
                        if (ValueHasObjects())
                        {
                            var value_addr = GetValueAddressInfo((long)raw + ptr);
                            if (start_addr == -1) start_addr = value_addr->Address;
                            if (stream.Position != streamStartPos + value_addr->Address - start_addr)
                            {
                                stream.Seek(streamStartPos + value_addr->Address - start_addr, SeekOrigin.Begin);
                            }

                            valueSerializer.Deserialize(out src[ptr / recordSize].value);
                        }
                        else
                        {
                            src[ptr / recordSize].value = record.value;
                        }
                    }
                }
                ptr += GetRecordSize(ptr).Item2;
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
                ref Record<Key, Value> record = ref Unsafe.AsRef<Record<Key, Value>>(raw + ptr);

                if (!record.info.Invalid)
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


                    if (ValueHasObjects() && !record.info.Tombstone)
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
                ptr += GetRecordSize(ptr).Item2;
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
                ctx.key = Unsafe.AsRef<Record<Key, Value>>(record).key;
            }
            if (!ValueHasObjects())
            {
                ctx.value = Unsafe.AsRef<Record<Key, Value>>(record).value;
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
                    throw new FasterException("Size of key-value exceeds max of 2GB: " + (endAddress - startAddress));

                if (startAddress < 0)
                    startAddress = 0;

                AsyncGetFromDisk(startAddress, (int)(endAddress - startAddress), ctx, ctx.record);
                return false;
            }

            // Parse the key and value objects
            MemoryStream ms = new MemoryStream(ctx.objBuffer.buffer);
            ms.Seek(ctx.objBuffer.offset + ctx.objBuffer.valid_offset, SeekOrigin.Begin);

            if (KeyHasObjects())
            {
                var keySerializer = SerializerSettings.keySerializer();
                keySerializer.BeginDeserialize(ms);
                keySerializer.Deserialize(out ctx.key);
                keySerializer.EndDeserialize();
            }

            if (ValueHasObjects() && !GetInfoFromBytePointer(record).Tombstone)
            {
                var valueSerializer = SerializerSettings.valueSerializer();
                valueSerializer.BeginDeserialize(ms);
                valueSerializer.Deserialize(out ctx.value);
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

        public override IHeapContainer<Key> GetKeyContainer(ref Key key) => new StandardHeapContainer<Key>(ref key);
        public override IHeapContainer<Value> GetValueContainer(ref Value value) => new StandardHeapContainer<Value>(ref value);

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
                Debug.Assert(required_bytes <= recordSize * destinationPage.Length);

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
            return new GenericScanIterator<Key, Value>(this, beginAddress, endAddress, scanBufferingMode, epoch);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress)
        {
            var page = (beginAddress >> LogPageSizeBits) % BufferSize;
            int start = (int)(beginAddress & PageSizeMask) / recordSize;
            int count = (int)(endAddress - beginAddress) / recordSize;
            int end = start + count;
            using var iter = new MemoryPageScanIterator<Key, Value>(values[page], start, end);
            Debug.Assert(epoch.ThisInstanceProtected());
            try
            {
                epoch.Suspend();
                OnEvictionObserver?.OnNext(iter);
            }
            finally
            {
                epoch.Resume();
            }
        }

        internal override void AsyncFlushDeltaToDevice(long startAddress, long endAddress, int version, DeltaLog deltaLog)
        {
            throw new FasterException("Incremental snapshots not supported with generic allocator");
        }
    }
}

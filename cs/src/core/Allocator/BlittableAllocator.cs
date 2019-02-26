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

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    internal class Frame : IDisposable
    {
        public readonly int frameSize, pageSize, sectorSize;
        public readonly byte[][] frame;
        public GCHandle[] handles;
        public long[] pointers;

        public Frame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            frame = new byte[frameSize][];
            handles = new GCHandle[frameSize];
            pointers = new long[frameSize];
        }
        public void Allocate(int index)
        {
            var adjustedSize = pageSize + 2 * sectorSize;
            byte[] tmp = new byte[adjustedSize];
            Array.Clear(tmp, 0, adjustedSize);

            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
            pointers[index] = (p + (sectorSize - 1)) & ~(sectorSize - 1);
            frame[index] = tmp;
        }

        public void Clear(int pageIndex)
        {
            Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
        }

        public long GetPhysicalAddress(long frameNumber, long offset)
        {
            return pointers[frameNumber % frameSize] + offset;
        }

        public void Dispose()
        {
            for (int i = 0; i < frameSize; i++)
            {
                handles[i].Free();
                frame[i] = null;
                pointers[i] = 0;
            }
        }
    }
    public unsafe sealed class BlittableAllocator<Key, Value> : AllocatorBase<Key, Value>
        where Key : new()
        where Value : new()
    {
        // Circular buffer definition
        private byte[][] values;
        private GCHandle[] handles;
        private long[] pointers;
        private readonly GCHandle ptrHandle;
        private readonly long* nativePointers;

        // Record sizes
        private static readonly int recordSize = Utility.GetSize(default(Record<Key, Value>));
        private static readonly int keySize = Utility.GetSize(default(Key));
        private static readonly int valueSize = Utility.GetSize(default(Value));

        public BlittableAllocator(LogSettings settings, IFasterEqualityComparer<Key> comparer)
            : base(settings, comparer)
        {
            values = new byte[BufferSize][];
            handles = new GCHandle[BufferSize];
            pointers = new long[BufferSize];

            epoch = LightEpoch.Instance;

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

        public override ref RecordInfo GetInfoFromBytePointer(byte* ptr)
        {
            return ref Unsafe.AsRef<RecordInfo>(ptr);
        }

        public override ref Key GetKey(long physicalAddress)
        {
            return ref Unsafe.AsRef<Key>((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override ref Value GetValue(long physicalAddress)
        {
            return ref Unsafe.AsRef<Value>((byte*)physicalAddress + RecordInfo.GetLength() + keySize);
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
            return (AddressInfo*)((byte*)physicalAddress + RecordInfo.GetLength() + keySize);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        }

        protected override void WriteAsync<TContext>(long flushPage, IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, IOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice)
        {
            var alignedPageSize = (pageSize + (sectorSize - 1)) & ~(sectorSize - 1);

            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)alignedPageSize, callback, asyncResult,
                        device);
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
            Array.Clear(values[page % BufferSize], 0, values[page % BufferSize].Length);
        }

        private void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                        IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device)
        {
            if (asyncResult.partial)
            {
                // Write only required bytes within the page
                int aligned_start = (int)((asyncResult.fromAddress - (asyncResult.page << LogPageSizeBits)));
                aligned_start = (aligned_start / sectorSize) * sectorSize;

                int aligned_end = (int)((asyncResult.untilAddress - (asyncResult.page << LogPageSizeBits)));
                aligned_end = ((aligned_end + (sectorSize - 1)) & ~(sectorSize - 1));

                numBytesToWrite = (uint)(aligned_end - aligned_start);
                device.WriteAsync(alignedSourceAddress + aligned_start, alignedDestinationAddress + (ulong)aligned_start, numBytesToWrite, callback, asyncResult);
            }
            else
            {
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress,
                    numBytesToWrite, callback, asyncResult);
            }
        }

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            IOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
        {
            device.ReadAsync(alignedSourceAddress, (IntPtr)pointers[destinationPageIndex],
                aligned_read_length, callback, asyncResult);
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
            throw new InvalidOperationException("AsyncReadRecordObjectsToMemory invalid for BlittableAllocator");
        }

        /// <summary>
        /// Retrieve objects from object log
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected override bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx)
        {
            ShallowCopy(ref GetKey((long)record), ref ctx.key);
            ShallowCopy(ref GetValue((long)record), ref ctx.value);
            return true;
        }

        /// <summary>
        /// Whether KVS has keys to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool KeyHasObjects()
        {
            return false;
        }

        /// <summary>
        /// Whether KVS has values to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public override bool ValueHasObjects()
        {
            return false;
        }

        public override long[] GetSegmentOffsets()
        {
            return null;
        }

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new Exception("BlittableAllocator memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
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
            return new BlittableScanIterator(this, beginAddress, endAddress, scanBufferingMode);
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
        private void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        IOCompletionCallback callback,
                                        TContext context,
                                        Frame frame,
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
                    count = 1,
                    frame = frame
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                usedDevice.ReadAsync(offsetInFile, (IntPtr)frame.pointers[pageIndex], (uint)AlignedPageSizeBytes, callback, asyncResult);
            }
        }

        /// <summary>
        /// Scan iterator for hybrid log
        /// </summary>
        public class BlittableScanIterator : IFasterScanIterator<Key, Value>
        {
            private readonly int frameSize;

            private readonly BlittableAllocator<Key, Value> hlog;
            private readonly long beginAddress, endAddress;

            private readonly Frame frame;
            private readonly CountdownEvent[] loaded;

            private bool first = true;
            private long currentAddress;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="hlog"></param>
            /// <param name="beginAddress"></param>
            /// <param name="endAddress"></param>
            /// <param name="scanBufferingMode"></param>
            public BlittableScanIterator(BlittableAllocator<Key, Value> hlog, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
            {
                this.hlog = hlog;
                this.beginAddress = beginAddress;
                this.endAddress = endAddress;
                currentAddress = beginAddress;

                if (scanBufferingMode == ScanBufferingMode.SinglePageBuffering)
                    frameSize = 1;
                else
                    frameSize = 2;

                frame = new Frame(frameSize, hlog.PageSize, hlog.sectorSize);
                loaded = new CountdownEvent[frameSize];

                var frameNumber = (currentAddress >> hlog.LogPageSizeBits) % frameSize;
                hlog.AsyncReadPagesFromDeviceToFrame
                    (currentAddress >> hlog.LogPageSizeBits,
                    1, AsyncReadPagesCallback, Empty.Default,
                    frame, out loaded[frameNumber]);
            }


            public bool GetNext(out Key key, out Value value)
            {
                key = default(Key);
                value = default(Value);

                while (true)
                {
                    // Check for boundary conditions
                    if (currentAddress >= endAddress)
                    {
                        return false;
                    }

                    if (currentAddress < hlog.BeginAddress)
                    {
                        throw new Exception("Iterator address is less than log BeginAddress " + hlog.BeginAddress);
                    }

                    var currentPage = currentAddress >> hlog.LogPageSizeBits;
                    var currentFrame = currentPage % frameSize;
                    var offset = currentAddress & hlog.PageSizeMask;

                    if (currentAddress < hlog.HeadAddress)
                        BufferAndLoad(currentAddress, currentPage, currentFrame);

                    // Check if record fits on page, if not skip to next page
                    if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                    {
                        currentAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                        continue;
                    }


                    if (currentAddress >= hlog.HeadAddress)
                    {
                        // Read record from cached page memory
                        var _physicalAddress = hlog.GetPhysicalAddress(currentAddress);

                        key = hlog.GetKey(_physicalAddress);
                        value = hlog.GetValue(_physicalAddress);
                        currentAddress += hlog.GetRecordSize(_physicalAddress);
                        return true;
                    }

                    var physicalAddress = frame.GetPhysicalAddress(currentFrame, offset);
                    key = hlog.GetKey(physicalAddress);
                    value = hlog.GetValue(physicalAddress);
                    currentAddress += hlog.GetRecordSize(physicalAddress);
                    return true;
                }
            }

            
            private void BufferAndLoad(long currentAddress, long currentPage, long currentFrame)
            {
                if (first || (currentAddress & hlog.PageSizeMask) == 0)
                {
                    // Prefetch pages based on buffering mode
                    if (frameSize == 1)
                    {
                        if (!first)
                        {
                            hlog.AsyncReadPagesFromDeviceToFrame(currentAddress >> hlog.LogPageSizeBits, 1, AsyncReadPagesCallback, Empty.Default, frame, out loaded[currentFrame]);
                        }
                    }
                    else
                    {
                        var endPage = endAddress >> hlog.LogPageSizeBits;
                        if ((endPage > currentPage) &&
                            ((endPage > currentPage + 1) || ((endAddress & hlog.PageSizeMask) != 0)))
                        {
                            hlog.AsyncReadPagesFromDeviceToFrame(1 + (currentAddress >> hlog.LogPageSizeBits), 1, AsyncReadPagesCallback, Empty.Default, frame, out loaded[(currentPage + 1) % frameSize]);
                        }
                    }
                    first = false;
                }
                loaded[currentFrame].Wait();
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            private void AsyncReadPagesCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }

                var result = (PageAsyncReadResult<Empty>)Overlapped.Unpack(overlap).AsyncResult;

                if (result.freeBuffer1.buffer != null)
                {
                    hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                    result.freeBuffer1.Return();
                }

                if (result.handle != null)
                {
                    result.handle.Signal();
                }

                Interlocked.MemoryBarrier();
                Overlapped.Free(overlap);
            }
        }
    }
}

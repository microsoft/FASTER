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

        // Segment size
        private readonly int LogSegmentSizeBits;
        private readonly long SegmentSize;
        private readonly long SegmentSizeMask;
        private readonly int SegmentBufferSize;

        // Record sizes
        private static readonly int recordSize = Utility.GetSize(default(Record<Key, Value>));
        private static readonly int keySize = Utility.GetSize(default(Key));
        private static readonly int valueSize = Utility.GetSize(default(Value));

        public BlittableAllocator(LogSettings settings, IFasterEqualityComparer<Key> comparer)
            : base(settings, comparer)
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
                    (uint)PageSize,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, IOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice)
        {
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                        (ulong)(AlignedPageSizeBytes * (flushPage - startPage)),
                        (uint)PageSize, callback, asyncResult,
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
            Array.Clear(values[page], 0, values[page].Length);
        }

        private void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                        IOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                        IDevice device)
        {
            device.WriteAsync(alignedSourceAddress, alignedDestinationAddress,
                numBytesToWrite, callback, asyncResult);
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
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    public unsafe sealed class BlittableAllocator<Key, Value> : AllocatorBase<Key, Value>
    {
        // Circular buffer definition
        private readonly byte[][] values;
        private readonly long[] pointers;
#if !NET5_0_OR_GREATER
        private readonly GCHandle[] handles;
        private readonly GCHandle ptrHandle;
#endif
        private readonly long* nativePointers;

        // Record sizes
        private static readonly int recordSize = Utility.GetSize(default(Record<Key, Value>));
        private static readonly int recordInfoSize = Utility.GetSize(default(RecordInfo));
        private static readonly int keySize = Utility.GetSize(default(Key));
        private static readonly int valueSize = Utility.GetSize(default(Value));

        private readonly OverflowPool<PageUnit> overflowPagePool;
        
        public BlittableAllocator(LogSettings settings, IFasterEqualityComparer<Key> comparer, Action<long, long> evictCallback = null, LightEpoch epoch = null, Action<CommitInfo> flushCallback = null, ILogger logger = null)
            : base(settings, comparer, evictCallback, epoch, flushCallback, logger)
        {
            overflowPagePool = new OverflowPool<PageUnit>(4, p =>
#if NET5_0_OR_GREATER
            { }
#else
                p.handle.Free()
#endif
            );


            if (BufferSize > 0)
            {
                values = new byte[BufferSize][];

#if NET5_0_OR_GREATER
                pointers = GC.AllocateArray<long>(BufferSize, true);
                nativePointers = (long*)Unsafe.AsPointer(ref pointers[0]);
#else
                pointers = new long[BufferSize];
                handles = new GCHandle[BufferSize];
                ptrHandle = GCHandle.Alloc(pointers, GCHandleType.Pinned);
                nativePointers = (long*)ptrHandle.AddrOfPinnedObject();
#endif
            }
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

        /// <summary>
        /// Dispose memory allocator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();

#if !NET5_0_OR_GREATER
            if (BufferSize > 0)
            {
                for (int i = 0; i < handles.Length; i++)
                {
                    if (handles[i].IsAllocated)
                        handles[i].Free();
                }
                ptrHandle.Free();
            }
#endif
            overflowPagePool.Dispose();
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
        internal override void AllocatePage(int index)
        {
            Interlocked.Increment(ref AllocatedPageCount);

            if (overflowPagePool.TryGet(out var item))
            {
#if !NET5_0_OR_GREATER
                handles[index] = item.handle;
#endif
                pointers[index] = item.pointer;
                values[index] = item.value;
                return;
            }

            var adjustedSize = PageSize + 2 * sectorSize;

#if NET5_0_OR_GREATER
            byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
            long p = (long)Unsafe.AsPointer(ref tmp[0]);
#else
            byte[] tmp = new byte[adjustedSize];
            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
#endif
            Array.Clear(tmp, 0, adjustedSize);
            pointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
            values[index] = tmp;
        }

        internal override int OverflowPageCount => overflowPagePool.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long GetPhysicalAddress(long logicalAddress)
        {
            // Offset within page
            int offset = (int)(logicalAddress & ((1L << LogPageSizeBits) - 1));

            // Index of page within the circular buffer
            int pageIndex = (int)((logicalAddress >> LogPageSizeBits) & (BufferSize - 1));
            return *(nativePointers + pageIndex) + offset;
        }

        internal override bool IsAllocated(int pageIndex)
        {
            return values[pageIndex] != null;
        }

        protected override void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult)
        {
            WriteAsync((IntPtr)pointers[flushPage % BufferSize],
                    (ulong)(AlignedPageSizeBytes * flushPage),
                    (uint)AlignedPageSizeBytes,
                    callback,
                    asyncResult, device);
        }

        protected override void WriteAsyncToDevice<TContext>
            (long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets)
        {
            base.VerifyCompatibleSectorSize(device);
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
                return (page << LogPageSizeBits) + Constants.kFirstValidAddress;

            return page << LogPageSizeBits;
        }

        internal override void ClearPage(long page, int offset)
        {
            if (offset == 0)
                Array.Clear(values[page % BufferSize], offset, values[page % BufferSize].Length - offset);
            else
            {
                // Adjust array offset for cache alignment
                offset += (int)(pointers[page % BufferSize] - (long)Unsafe.AsPointer(ref values[page % BufferSize][0]));
                Array.Clear(values[page % BufferSize], offset, values[page % BufferSize].Length - offset);
            }
        }

        internal override void FreePage(long page)
        {
            ClearPage(page, 0);
            if (EmptyPageCount > 0)
            {
                int index = (int)(page % BufferSize);
                overflowPagePool.TryAdd(new PageUnit
                {
#if !NET5_0_OR_GREATER
                    handle = handles[index],
#endif
                    pointer = pointers[index],
                    value = values[index]
                });
                values[index] = null;
                pointers[index] = 0;
#if !NET5_0_OR_GREATER
                handles[index] = default;
#endif
                Interlocked.Decrement(ref AllocatedPageCount);
            }
        }

        /// <summary>
        /// Delete in-memory portion of the log
        /// </summary>
        internal override void DeleteFromMemory()
        {
            for (int i = 0; i < values.Length; i++)
            {
#if !NET5_0_OR_GREATER
                if (handles[i].IsAllocated)
                    handles[i].Free();
#endif
                values[i] = null;
            }
        }

        protected override void ReadAsync<TContext>(
            ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice)
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
        protected override void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default)
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
            ctx.key = GetKey((long)record);
            ctx.value = GetValue((long)record);
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

        public override IHeapContainer<Key> GetKeyContainer(ref Key key) => new StandardHeapContainer<Key>(ref key);
        public override IHeapContainer<Value> GetValueContainer(ref Value value) => new StandardHeapContainer<Value>(ref value);

        public override long[] GetSegmentOffsets()
        {
            return null;
        }

        internal override void PopulatePage(byte* src, int required_bytes, long destinationPage)
        {
            throw new FasterException("BlittableAllocator memory pages are sector aligned - use direct copy");
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
            return new BlittableScanIterator<Key, Value>(this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<IFasterScanIterator<Key, Value>> observer)
        {
            using var iter = new BlittableScanIterator<Key, Value>(this, beginAddress, endAddress, ScanBufferingMode.NoBuffering, epoch, true, logger: logger);
            observer?.OnNext(iter);
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
        /// <param name="cts"></param>
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null,
                                        IDevice objectLogDevice = null,
                                        CancellationTokenSource cts = null)
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
                    frame = frame,
                    cts = cts
                };

                ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);

                uint readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = (AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask));

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                usedDevice.ReadAsync(offsetInFile, (IntPtr)frame.pointers[pageIndex], readLength, callback, asyncResult);
            }
        }
    }
}



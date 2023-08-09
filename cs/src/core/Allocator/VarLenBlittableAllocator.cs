// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
#if !NET5_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using System.Threading;
using static FASTER.core.Utility;

namespace FASTER.core
{
    internal unsafe sealed class VariableLengthBlittableAllocator<Key, Value> : AllocatorBase<Key, Value>
    {
        public const int kRecordAlignment = 8; // RecordInfo has a long field, so it should be aligned to 8-bytes

        // Circular buffer definition
        private readonly byte[][] values;
        private readonly long[] pointers;
#if !NET5_0_OR_GREATER
        private readonly GCHandle[] handles;
        private readonly GCHandle ptrHandle;
#endif
        private readonly long* nativePointers;
        private readonly bool fixedSizeKey;
        private readonly bool fixedSizeValue;

        internal readonly IVariableLengthStruct<Key> KeyLength;
        internal readonly IVariableLengthStruct<Value> ValueLength;

        private readonly OverflowPool<PageUnit> overflowPagePool;

        public VariableLengthBlittableAllocator(LogSettings settings, VariableLengthStructSettings<Key, Value> vlSettings, IFasterEqualityComparer<Key> comparer, 
                Action<long, long> evictCallback = null, LightEpoch epoch = null, Action<CommitInfo> flushCallback = null, ILogger logger = null)
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

            KeyLength = vlSettings.keyLength;
            ValueLength = vlSettings.valueLength;

            if (KeyLength == null)
            {
                fixedSizeKey = true;
                KeyLength = new FixedLengthStruct<Key>();
            }

            if (ValueLength == null)
            {
                fixedSizeValue = true;
                ValueLength = new FixedLengthStruct<Value>();
            }
        }

        internal override int OverflowPageCount => overflowPagePool.Count;

        public override void Reset()
        {
            base.Reset();
            for (int index = 0; index < BufferSize; index++)
            {
                ReturnPage(index);
            }
            Initialize();
        }

        void ReturnPage(int index)
        {
            Debug.Assert(index < BufferSize);
            if (values[index] != null)
            {
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
            return ref KeyLength.AsRef((byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override ref Value GetValue(long physicalAddress)
        {
            return ref ValueLength.AsRef((byte*)ValueOffset(physicalAddress));
        }

        public override ref Value GetAndInitializeValue(long physicalAddress, long endAddress)
        {
            var src = (byte*)ValueOffset(physicalAddress);
            ValueLength.Initialize(src, (void*)endAddress);
            return ref ValueLength.AsRef(src);
        }

        internal long ValueOffset(long physicalAddress)
            => physicalAddress + RecordInfo.GetLength() + AlignedKeySize(physicalAddress);

        private int AlignedKeySize(long physicalAddress)
        {
            int len = KeyLength.GetLength(ref GetKey(physicalAddress));
            return (len + kRecordAlignment - 1) & (~(kRecordAlignment - 1));
        }

        private int KeySize(long physicalAddress)
        {
            return KeyLength.GetLength(ref GetKey(physicalAddress));
        }

        private int ValueSize(long physicalAddress)
        {
            return ValueLength.GetLength(ref GetValue(physicalAddress));
        }

        public override (int actualSize, int allocatedSize) GetRecordSize(long physicalAddress)
        {
            ref var recordInfo = ref GetInfo(physicalAddress);
            if (recordInfo.IsNull())
            {
                var l = RecordInfo.GetLength();
                return (l, l);
            }

            var valueLen = ValueSize(physicalAddress);
            if (recordInfo.Filler)  // Get the extraValueLength
                valueLen += *(int*)(ValueOffset(physicalAddress) + RoundUp(valueLen, sizeof(int)));
            var size = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + valueLen;
            return (size, (size + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
        }

        public override (int actualSize, int allocatedSize, int keySize) GetCopyDestinationRecordSize<Input, FasterSession>(ref Key key, ref Input input, ref Value value, ref RecordInfo recordInfo, FasterSession fasterSession)
        {
            // Used by RMW to determine the length of copy destination (taking Input into account), so does not need to get filler length.
            var keySize = KeyLength.GetLength(ref key);
            var size = RecordInfo.GetLength() + RoundUp(keySize, kRecordAlignment) + fasterSession.GetLength(ref value, ref input);
            return (size, RoundUp(size, kRecordAlignment), keySize);
        }

        public override int GetRequiredRecordSize(long physicalAddress, int availableBytes)
        {
            // We need at least [average record size]...
            var reqBytes = GetAverageRecordSize();
            if (availableBytes < reqBytes)
                return reqBytes;

            // We need at least [RecordInfo size] + [actual key size]...
            reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + ValueLength.GetInitialLength();
            if (availableBytes < reqBytes)
                return reqBytes;

            // We need at least [RecordInfo size] + [actual key size] + [actual value size]
            var recordInfo = GetInfo(physicalAddress);
            var valueLen = ValueSize(physicalAddress);
            if (recordInfo.Filler)
            {
                // We have a filler, so the valueLen we have now is the usedValueLength; we need to offset to where the extraValueLength is and read that int
                var alignedUsedValueLength = RoundUp(valueLen, sizeof(int));
                reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + alignedUsedValueLength + sizeof(int);
                if (availableBytes < reqBytes)
                    return reqBytes;
                valueLen += *(int*)(ValueOffset(physicalAddress) + alignedUsedValueLength);
            }

            // Now we know the full record length.
            reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + valueLen;
            reqBytes = (reqBytes + kRecordAlignment - 1) & (~(kRecordAlignment - 1));
            return reqBytes;
        }

        public override int GetAverageRecordSize()
        {
            return RecordInfo.GetLength() +
                ((KeyLength.GetInitialLength() + kRecordAlignment - 1) & (~(kRecordAlignment - 1))) +
                ((ValueLength.GetInitialLength() + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
        }

        public override int GetFixedRecordSize()
        {
            return RecordInfo.GetLength()
                + (fixedSizeKey ? KeyLength.GetInitialLength() : 0)
                + (fixedSizeValue ? ValueLength.GetInitialLength() : 0);
        }

        public override (int actualSize, int allocatedSize, int keySize) GetInitialRecordSize<TInput, FasterSession>(ref Key key, ref TInput input, FasterSession fasterSession)
        {
            int keySize = KeyLength.GetLength(ref key);
            var actualSize = RecordInfo.GetLength() + RoundUp(keySize, kRecordAlignment) + fasterSession.GetInitialLength(ref input);
            return (actualSize, RoundUp(actualSize, kRecordAlignment), keySize);
        }

        public override (int actualSize, int allocatedSize, int keySize) GetRecordSize(ref Key key, ref Value value)
        {
            int keySize = KeyLength.GetLength(ref key);
            var actualSize = RecordInfo.GetLength() + RoundUp(keySize, kRecordAlignment) + ValueLength.GetLength(ref value);
            return (actualSize, RoundUp(actualSize, kRecordAlignment), keySize);
        }

        public override void Serialize(ref Key src, long physicalAddress)
        {
            KeyLength.Serialize(ref src, (byte*)physicalAddress + RecordInfo.GetLength());
        }

        public override void Serialize(ref Value src, long physicalAddress)
        {
            ValueLength.Serialize(ref src, (byte*)ValueOffset(physicalAddress));
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
            throw new NotSupportedException();
        }

        public override AddressInfo* GetValueAddressInfo(long physicalAddress)
        {
            throw new NotSupportedException();
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
            PageAsyncFlushResult<TContext> asyncResult, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets, long fuzzyStartLogicalAddress)
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
                ReturnPage((int)(page % BufferSize));
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
            return true;
        }

        public override ref Key GetContextRecordKey(ref AsyncIOContext<Key, Value> ctx)
        {
            return ref GetKey((long)ctx.record.GetValidPointer());
        }

        public override ref Value GetContextRecordValue(ref AsyncIOContext<Key, Value> ctx)
        {
            return ref GetValue((long)ctx.record.GetValidPointer());
        }

        public override IHeapContainer<Key> GetKeyContainer(ref Key key)
        {
            if (fixedSizeKey) return new StandardHeapContainer<Key>(ref key);
            else return new VarLenHeapContainer<Key>(ref key, KeyLength, bufferPool);
        }

        public override IHeapContainer<Value> GetValueContainer(ref Value value)
        {
            if (fixedSizeValue) return new StandardHeapContainer<Value>(ref value);
            else return new VarLenHeapContainer<Value>(ref value, ValueLength, bufferPool);
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
            throw new FasterException("BlittableAllocator memory pages are sector aligned - use direct copy");
            // Buffer.MemoryCopy(src, (void*)pointers[destinationPage % BufferSize], required_bytes, required_bytes);
        }

        /// <summary>
        /// Iterator interface for pull-scanning FASTER log
        /// </summary>
        public override IFasterScanIterator<Key, Value> Scan(FasterKV<Key, Value> store, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode) 
            => new VariableLengthBlittableScanIterator<Key, Value>(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);

        /// <summary>
        /// Implementation for push-scanning FASTER log, called from LogAccessor
        /// </summary>
        internal override bool Scan<TScanFunctions>(FasterKV<Key, Value> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions, ScanBufferingMode scanBufferingMode)
        {
            using VariableLengthBlittableScanIterator<Key, Value> iter = new(store, this, beginAddress, endAddress, scanBufferingMode, epoch, logger: logger);
            return PushScanImpl(store, beginAddress, endAddress, ref scanFunctions, iter);
        }

        /// <summary>
        /// Implementation for push-iterating key versions, called from LogAccessor
        /// </summary>
        internal override bool IterateKeyVersions<TScanFunctions>(FasterKV<Key, Value> store, ref Key key, long beginAddress, ref TScanFunctions scanFunctions)
        {
            using VariableLengthBlittableScanIterator<Key, Value> iter = new(store, this, store.comparer, beginAddress, epoch, logger: logger);
            return IterateKeyVersionsImpl(store, ref key, beginAddress, ref scanFunctions, iter);
        }

        /// <inheritdoc />
        internal override void MemoryPageScan(long beginAddress, long endAddress, IObserver<IFasterScanIterator<Key, Value>> observer)
        {
            using var iter = new VariableLengthBlittableScanIterator<Key, Value>(store: null, this, beginAddress, endAddress, ScanBufferingMode.NoBuffering, epoch, true, logger: logger);
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
        internal void AsyncReadPagesFromDeviceToFrame<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
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
                    frame = frame
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



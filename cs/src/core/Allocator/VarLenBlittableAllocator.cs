// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    public unsafe sealed class VariableLengthBlittableAllocator<Key, Value> : AllocatorBase<Key, Value>
    {
        public const int kRecordAlignment = 8; // RecordInfo has a long field, so it should be aligned to 8-bytes

        // Circular buffer definition
        private byte[][] values;
        private GCHandle[] handles;
        private long[] pointers;
        private readonly GCHandle ptrHandle;
        private readonly long* nativePointers;
        private readonly bool fixedSizeKey;
        private readonly bool fixedSizeValue;

        internal readonly IVariableLengthStruct<Key> KeyLength;
        internal readonly IVariableLengthStruct<Value> ValueLength;

        public VariableLengthBlittableAllocator(LogSettings settings, VariableLengthStructSettings<Key, Value> vlSettings, IFasterEqualityComparer<Key> comparer, Action<long, long> evictCallback = null, LightEpoch epoch = null, Action<CommitInfo> flushCallback = null)
            : base(settings, comparer, evictCallback, epoch, flushCallback)
        {
            values = new byte[BufferSize][];
            handles = new GCHandle[BufferSize];
            pointers = new long[BufferSize];

            ptrHandle = GCHandle.Alloc(pointers, GCHandleType.Pinned);
            nativePointers = (long*)ptrHandle.AddrOfPinnedObject();

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

        public override ref Value GetValue(long physicalAddress, long endAddress)
        {
            var src = (byte*)ValueOffset(physicalAddress);
            ValueLength.Initialize(src, (void*)endAddress);
            return ref ValueLength.AsRef(src);
        }

        private long ValueOffset(long physicalAddress)
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

        public override (int, int) GetRecordSize(long physicalAddress)
        {
            ref var recordInfo = ref GetInfo(physicalAddress);
            if (recordInfo.IsNull())
            {
                var l = RecordInfo.GetLength();
                return (l, l);
            }                

            var size = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + ValueSize(physicalAddress);
            return (size, (size + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
        }

        public override (int, int) GetRecordSize<Input, FasterSession>(long physicalAddress, ref Input input, FasterSession fasterSession)
        {
            ref var recordInfo = ref GetInfo(physicalAddress);
            if (recordInfo.IsNull())
            {
                var l = RecordInfo.GetLength();
                return (l, l);
            }

            var size = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + fasterSession.GetLength(ref GetValue(physicalAddress), ref input);
            return (size, (size + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
        }

        public override int GetRequiredRecordSize(long physicalAddress, int availableBytes)
        {
            // We need at least [record size] + [average key size] + [average value size]
            var reqBytes = GetAverageRecordSize();
            if (availableBytes < reqBytes)
            {
                return reqBytes;
            }

            // We need at least [record size] + [actual key size] + [average value size]
            reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + ValueLength.GetInitialLength();
            if (availableBytes < reqBytes)
            {
                return reqBytes;
            }

            // We need at least [record size] + [actual key size] + [actual value size]
            reqBytes = RecordInfo.GetLength() + AlignedKeySize(physicalAddress) + ValueSize(physicalAddress);
            reqBytes = (reqBytes + kRecordAlignment - 1) & (~(kRecordAlignment - 1));
            return reqBytes;
        }

        public override int GetAverageRecordSize()
        {
            return RecordInfo.GetLength() +
                ((KeyLength.GetInitialLength() + kRecordAlignment - 1) & (~(kRecordAlignment - 1))) +
                ((ValueLength.GetInitialLength() + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
        }

        public override (int, int) GetInitialRecordSize<TInput, FasterSession>(ref Key key, ref TInput input, FasterSession fasterSession)
        {
            var actualSize = RecordInfo.GetLength() +
                ((KeyLength.GetLength(ref key) + kRecordAlignment - 1) & (~(kRecordAlignment - 1))) +
                fasterSession.GetInitialLength(ref input);

            return (actualSize, (actualSize + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
        }

        public override (int, int) GetRecordSize(ref Key key, ref Value value)
        {
            var actualSize = RecordInfo.GetLength() +
                ((KeyLength.GetLength(ref key) + kRecordAlignment - 1) & (~(kRecordAlignment - 1))) +
                ValueLength.GetLength(ref value);

            return (actualSize, (actualSize + kRecordAlignment - 1) & (~(kRecordAlignment - 1)));
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
            if (values != null)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    if (handles[i].IsAllocated)
                        handles[i].Free();
                    values[i] = null;
                }
            }
            handles = null;
            pointers = null;
            values = null;
            base.Dispose();
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
            var adjustedSize = PageSize + 2 * sectorSize;
            byte[] tmp = new byte[adjustedSize];
            Array.Clear(tmp, 0, adjustedSize);

            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
            pointers[index] = (p + (sectorSize - 1)) & ~(sectorSize - 1);
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

        protected override bool IsAllocated(int pageIndex)
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

        protected override void ClearPage(long page, int offset)
        {
            if (offset == 0)
                Array.Clear(values[page % BufferSize], offset, values[page % BufferSize].Length - offset);
            else
            {
                // Adjust array offset for cache alignment
                offset += (int)(pointers[page % BufferSize] - (long)handles[page % BufferSize].AddrOfPinnedObject());
                Array.Clear(values[page % BufferSize], offset, values[page % BufferSize].Length - offset);
            }
        }

        /// <summary>
        /// Delete in-memory portion of the log
        /// </summary>
        internal override void DeleteFromMemory()
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
        }


        private void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                        DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
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
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, callback, asyncResult);
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
        /// Iterator interface for scanning FASTER log
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <returns></returns>
        public override IFasterScanIterator<Key, Value> Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode)
        {
            return new VariableLengthBlittableScanIterator<Key, Value>(this, beginAddress, endAddress, scanBufferingMode);
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



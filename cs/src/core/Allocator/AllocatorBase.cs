// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    internal enum PMMFlushStatus : int { Flushed, InProgress };

    internal enum PMMCloseStatus : int { Closed, Open };

    [StructLayout(LayoutKind.Explicit)]
    internal struct FullPageStatus
    {
        [FieldOffset(0)]
        public long LastFlushedUntilAddress;
        [FieldOffset(8)]
        public long LastClosedUntilAddress;
        [FieldOffset(16)]
        public int Dirty;
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct PageOffset
    {
        [FieldOffset(0)]
        public int Offset;
        [FieldOffset(4)]
        public int Page;
        [FieldOffset(0)]
        public long PageAndOffset;
    }

    /// <summary>
    /// Base class for hybrid log memory allocator
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public unsafe abstract partial class AllocatorBase<Key, Value> : IDisposable
    {
        /// <summary>
        /// Epoch information
        /// </summary>
        protected readonly LightEpoch epoch;
        private readonly bool ownedEpoch;

        /// <summary>
        /// Comparer
        /// </summary>
        protected readonly IFasterEqualityComparer<Key> comparer;

        #region Protected size definitions
        /// <summary>
        /// Buffer size
        /// </summary>
        internal readonly int BufferSize;
        /// <summary>
        /// Log page size
        /// </summary>
        internal readonly int LogPageSizeBits;

        /// <summary>
        /// Page size
        /// </summary>
        internal readonly int PageSize;
        /// <summary>
        /// Page size mask
        /// </summary>
        internal readonly int PageSizeMask;
        /// <summary>
        /// Buffer size mask
        /// </summary>
        protected readonly int BufferSizeMask;
        /// <summary>
        /// Aligned page size in bytes
        /// </summary>
        protected readonly int AlignedPageSizeBytes;

        /// <summary>
        /// Total hybrid log size (bits)
        /// </summary>
        protected readonly int LogTotalSizeBits;
        /// <summary>
        /// Total hybrid log size (bytes)
        /// </summary>
        protected readonly long LogTotalSizeBytes;

        /// <summary>
        /// Segment size in bits
        /// </summary>
        protected readonly int LogSegmentSizeBits;
        /// <summary>
        /// Segment size
        /// </summary>
        protected readonly long SegmentSize;
        /// <summary>
        /// Segment buffer size
        /// </summary>
        protected readonly int SegmentBufferSize;

        /// <summary>
        /// HeadOffset lag (from tail)
        /// </summary>
        protected readonly bool HeadOffsetExtraLag;

        /// <summary>
        /// HeadOFfset lag address
        /// </summary>
        protected readonly long HeadOffsetLagAddress;

        /// <summary>
        /// Log mutable fraction
        /// </summary>
        protected readonly double LogMutableFraction;
        /// <summary>
        /// ReadOnlyOffset lag (from tail)
        /// </summary>
        protected readonly long ReadOnlyLagAddress;

        #endregion

        #region Public addresses
        /// <summary>
        /// Read-only address
        /// </summary>
        public long ReadOnlyAddress;

        /// <summary>
        /// Safe read-only address
        /// </summary>
        public long SafeReadOnlyAddress;

        /// <summary>
        /// Head address
        /// </summary>
        public long HeadAddress;

        /// <summary>
        ///  Safe head address
        /// </summary>
        public long SafeHeadAddress;

        /// <summary>
        /// Flushed until address
        /// </summary>
        public long FlushedUntilAddress;

        /// <summary>
        /// Flushed until address
        /// </summary>
        public long ClosedUntilAddress;

        /// <summary>
        /// Begin address
        /// </summary>
        public long BeginAddress;

        #endregion

        #region Protected device info
        /// <summary>
        /// Device
        /// </summary>
        protected readonly IDevice device;
        /// <summary>
        /// Sector size
        /// </summary>
        protected readonly int sectorSize;
        #endregion

        #region Private page metadata

        // Array that indicates the status of each buffer page
        internal readonly FullPageStatus[] PageStatusIndicator;
        internal readonly PendingFlushList[] PendingFlush;

        /// <summary>
        /// Global address of the current tail (next element to be allocated from the circular buffer) 
        /// </summary>
        private PageOffset TailPageOffset;

        /// <summary>
        /// Whether log is disposed
        /// </summary>
        private bool disposed = false;

        /// <summary>
        /// Whether device is a null device
        /// </summary>
        internal readonly bool IsNullDevice;

        #endregion

        /// <summary>
        /// Buffer pool
        /// </summary>
        internal SectorAlignedBufferPool bufferPool;

        /// <summary>
        /// Read cache
        /// </summary>
        protected readonly bool ReadCache = false;

        /// <summary>
        /// Read cache eviction callback
        /// </summary>
        protected readonly Action<long, long> EvictCallback = null;

        /// <summary>
        /// Flush callback
        /// </summary>
        protected readonly Action<CommitInfo> FlushCallback = null;

        /// <summary>
        /// Whether to preallocate log on initialization
        /// </summary>
        private readonly bool PreallocateLog = false;

        /// <summary>
        /// Error handling
        /// </summary>
        private readonly ErrorList errorList = new ErrorList();

        /// <summary>
        /// Observer for records entering read-only region
        /// </summary>
        internal IObserver<IFasterScanIterator<Key, Value>> OnReadOnlyObserver;

        /// <summary>
        /// Observer for records getting evicted from memory (page closed)
        /// </summary>
        internal IObserver<IFasterScanIterator<Key, Value>> OnEvictionObserver;

        #region Abstract methods
        /// <summary>
        /// Initialize
        /// </summary>
        public abstract void Initialize();
        /// <summary>
        /// Get start logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public abstract long GetStartLogicalAddress(long page);
        /// <summary>
        /// Get first valid logical address
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public abstract long GetFirstValidLogicalAddress(long page);
        /// <summary>
        /// Get physical address
        /// </summary>
        /// <param name="newLogicalAddress"></param>
        /// <returns></returns>
        public abstract long GetPhysicalAddress(long newLogicalAddress);
        /// <summary>
        /// Get address info
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract ref RecordInfo GetInfo(long physicalAddress);

        /// <summary>
        /// Get info from byte pointer
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        public abstract ref RecordInfo GetInfoFromBytePointer(byte* ptr);

        /// <summary>
        /// Get key
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract ref Key GetKey(long physicalAddress);
        /// <summary>
        /// Get value
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract ref Value GetValue(long physicalAddress);
        /// <summary>
        /// Get value from address range
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <param name="endPhysicalAddress"></param>
        /// <returns></returns>
        public virtual ref Value GetValue(long physicalAddress, long endPhysicalAddress) => ref GetValue(physicalAddress);

        /// <summary>
        /// Get address info for key
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract AddressInfo* GetKeyAddressInfo(long physicalAddress);
        /// <summary>
        /// Get address info for value
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract AddressInfo* GetValueAddressInfo(long physicalAddress);

        /// <summary>
        /// Get record size
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <returns></returns>
        public abstract (int, int) GetRecordSize(long physicalAddress);

        /// <summary>
        /// Get record size
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <param name="input"></param>
        /// <param name="fasterSession"></param>
        /// <returns></returns>
        public abstract (int, int) GetRecordSize<Input, FasterSession>(long physicalAddress, ref Input input, FasterSession fasterSession)
            where FasterSession : IVariableLengthStruct<Value, Input>;

        /// <summary>
        /// Get number of bytes required
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <param name="availableBytes"></param>
        /// <returns></returns>
        public virtual int GetRequiredRecordSize(long physicalAddress, int availableBytes) => GetAverageRecordSize();

        /// <summary>
        /// Get average record size
        /// </summary>
        /// <returns></returns>
        public abstract int GetAverageRecordSize();

        /// <summary>
        /// Get size of fixed (known) part of record on the main log
        /// </summary>
        /// <returns></returns>
        public abstract int GetFixedRecordSize();

        /// <summary>
        /// Get initial record size
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="fasterSession"></param>
        /// <returns></returns>
        public abstract (int, int) GetInitialRecordSize<Input, FasterSession>(ref Key key, ref Input input, FasterSession fasterSession)
            where FasterSession : IVariableLengthStruct<Value, Input>;

        /// <summary>
        /// Get record size
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public abstract (int, int) GetRecordSize(ref Key key, ref Value value);

        /// <summary>
        /// Allocate page
        /// </summary>
        /// <param name="index"></param>
        internal abstract void AllocatePage(int index);
        /// <summary>
        /// Whether page is allocated
        /// </summary>
        /// <param name="pageIndex"></param>
        /// <returns></returns>
        internal abstract bool IsAllocated(int pageIndex);
        /// <summary>
        /// Populate page
        /// </summary>
        /// <param name="src"></param>
        /// <param name="required_bytes"></param>
        /// <param name="destinationPage"></param>
        internal abstract void PopulatePage(byte* src, int required_bytes, long destinationPage);
        /// <summary>
        /// Write async to device
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="startPage"></param>
        /// <param name="flushPage"></param>
        /// <param name="pageSize"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="localSegmentOffsets"></param>
        protected abstract void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> result, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets);

        /// <summary>
        /// Delta flush
        /// </summary>
        /// <param name="startAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="version"></param>
        /// <param name="device"></param>
        /// <param name="tailAddress"></param>
        /// <param name="completedSemaphore"></param>
        internal virtual void AsyncFlushDeltaToDevice(long startAddress, long endAddress, int version, IDevice device, ref long tailAddress, out SemaphoreSlim completedSemaphore)
        {
            using var deltaLog = new DeltaLog(device, LogPageSizeBits, tailAddress);
            deltaLog.InitializeForWrites(bufferPool);

            long startPage = GetPage(startAddress);
            long endPage = GetPage(endAddress);
            if (endAddress > GetStartLogicalAddress(endPage))
                endPage++;

            deltaLog.Allocate(out int entryLength, out long destPhysicalAddress);
            int destOffset = 0;

            for (long p = startPage; p < endPage; p++)
            {
                if (PageStatusIndicator[p % BufferSize].Dirty < version)
                    continue;

                var logicalAddress = p << LogPageSizeBits;
                var physicalAddress = GetPhysicalAddress(logicalAddress);
                var endPhysicalAddress = physicalAddress + PageSize;

                if (p == startPage)
                {
                    physicalAddress += (int)(startAddress & PageSizeMask);
                    logicalAddress += (int)(startAddress & PageSizeMask);
                }

                while (physicalAddress < endPhysicalAddress)
                {
                    ref var info = ref GetInfo(physicalAddress);
                    var (recordSize, alignedRecordSize) = GetRecordSize(physicalAddress);
                    if (info.Version == version)
                    {
                        int size = sizeof(long) + sizeof(int) + alignedRecordSize;
                        if (destOffset + size > entryLength)
                        {
                            deltaLog.Seal(destOffset);
                            deltaLog.Allocate(out entryLength, out destPhysicalAddress);
                            destOffset = 0;
                            if (destOffset + size > entryLength)
                                throw new FasterException("Insufficient page size to write delta");
                        }
                        *((long*)(destPhysicalAddress + destOffset)) = logicalAddress;
                        destOffset += sizeof(long);
                        *((int*)(destPhysicalAddress + destOffset)) = alignedRecordSize;
                        destOffset += sizeof(int);
                        Buffer.MemoryCopy((void*)physicalAddress, (void*)(destPhysicalAddress + destOffset), alignedRecordSize, alignedRecordSize);
                        destOffset += alignedRecordSize;
                    }
                    physicalAddress += alignedRecordSize;
                    logicalAddress += alignedRecordSize;
                }
            }

            if (destOffset > 0)
                deltaLog.Seal(destOffset);

            tailAddress = deltaLog.TailAddress;
            completedSemaphore = deltaLog.Flush();
        }

        internal void ApplyDelta(DeltaLog log, long startPage, long endPage, ref int version)
        {
            long startLogicalAddress = GetStartLogicalAddress(startPage);
            long endLogicalAddress = GetStartLogicalAddress(endPage);

            log.Reset();
            while (log.GetNext(out long physicalAddress, out int entryLength, out int type))
            {
                if (type != 0) continue; // consider only delta records
                long endAddress = physicalAddress + entryLength;
                while (physicalAddress < endAddress)
                {
                    long address = *(long*)physicalAddress;
                    physicalAddress += sizeof(long);
                    int size = *(int*)physicalAddress;
                    physicalAddress += sizeof(int);
                    if (address >= startLogicalAddress && address < endLogicalAddress)
                    {
                        var destination = GetPhysicalAddress(address);
                        Buffer.MemoryCopy((void*)physicalAddress, (void*)destination, size, size);
                        version = GetInfo(destination).Version;
                    }
                    physicalAddress += size;
                }
            }
        }

        internal void Mark(long logicalAddress, ref RecordInfo info, int version)
        {
            var offset = (logicalAddress >> LogPageSizeBits) % BufferSize;
            if (PageStatusIndicator[offset].Dirty != version)
                PageStatusIndicator[offset].Dirty = version;
            info.Version = version;
        }

        internal void MarkPage(long logicalAddress, int version)
        {
            var offset = (logicalAddress >> LogPageSizeBits) % BufferSize;
            if (PageStatusIndicator[offset].Dirty != version)
                PageStatusIndicator[offset].Dirty = version;
        }

        internal void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
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
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress,
                    numBytesToWrite, callback, asyncResult);
            }
        }


        /// <summary>
        /// Read objects to memory (async)
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        protected abstract void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<Key, Value> context, SectorAlignedMemory result = default);
        /// <summary>
        /// Read page (async)
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="alignedSourceAddress"></param>
        /// <param name="destinationPageIndex"></param>
        /// <param name="aligned_read_length"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        /// <param name="device"></param>
        /// <param name="objlogDevice"></param>
        protected abstract void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length, DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice);
        /// <summary>
        /// Clear page
        /// </summary>
        /// <param name="page">Page number to be cleared</param>
        /// <param name="offset">Offset to clear from (if partial clear)</param>
        internal abstract void ClearPage(long page, int offset = 0);
        /// <summary>
        /// Write page (async)
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="flushPage"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        protected abstract void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult);
        /// <summary>
        /// Retrieve full record
        /// </summary>
        /// <param name="record"></param>
        /// <param name="ctx"></param>
        /// <returns></returns>
        protected abstract bool RetrievedFullRecord(byte* record, ref AsyncIOContext<Key, Value> ctx);

        /// <summary>
        /// Retrieve value from context
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public virtual ref Key GetContextRecordKey(ref AsyncIOContext<Key, Value> ctx) => ref ctx.key;

        /// <summary>
        /// Retrieve value from context
        /// </summary>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public virtual ref Value GetContextRecordValue(ref AsyncIOContext<Key, Value> ctx) => ref ctx.value;

        /// <summary>
        /// Get heap container for pending key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public abstract IHeapContainer<Key> GetKeyContainer(ref Key key);

        /// <summary>
        /// Get heap container for pending value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public abstract IHeapContainer<Value> GetValueContainer(ref Value value);

        /// <summary>
        /// Copy value to context
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="value"></param>
        public virtual void PutContext(ref AsyncIOContext<Key, Value> ctx, ref Value value) => ctx.value = value;

        /// <summary>
        /// Whether key has objects
        /// </summary>
        /// <returns></returns>
        public abstract bool KeyHasObjects();

        /// <summary>
        /// Whether value has objects
        /// </summary>
        /// <returns></returns>
        public abstract bool ValueHasObjects();

        /// <summary>
        /// Get segment offsets
        /// </summary>
        /// <returns></returns>
        public abstract long[] GetSegmentOffsets();

        /// <summary>
        /// Pull-based scan interface for HLOG
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <returns></returns>
        public abstract IFasterScanIterator<Key, Value> Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering);

        /// <summary>
        /// Scan page guaranteed to be in memory
        /// </summary>
        /// <param name="beginAddress">Begin address</param>
        /// <param name="endAddress">End address</param>
        internal abstract void MemoryPageScan(long beginAddress, long endAddress);
        #endregion


        /// <summary>
        /// Instantiate base allocator
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="comparer"></param>
        /// <param name="evictCallback"></param>
        /// <param name="epoch"></param>
        /// <param name="flushCallback"></param>
        public AllocatorBase(LogSettings settings, IFasterEqualityComparer<Key> comparer, Action<long, long> evictCallback, LightEpoch epoch, Action<CommitInfo> flushCallback)
        {
            if (settings.LogDevice == null)
            {
                throw new FasterException("LogSettings.LogDevice needs to be specified (e.g., use Devices.CreateLogDevice, AzureStorageDevice, or NullDevice)");
            }
            if (evictCallback != null)
            {
                ReadCache = true;
                EvictCallback = evictCallback;
            }
            FlushCallback = flushCallback;
            PreallocateLog = settings.PreallocateLog;

            if (settings.LogDevice is NullDevice)
                IsNullDevice = true;

            this.comparer = comparer;
            if (epoch == null)
            {
                this.epoch = new LightEpoch();
                ownedEpoch = true;
            }
            else
                this.epoch = epoch;

            settings.LogDevice.Initialize(1L << settings.SegmentSizeBits, epoch);
            settings.ObjectLogDevice?.Initialize(-1, epoch);

            // Page size
            LogPageSizeBits = settings.PageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;

            // Total HLOG size
            LogTotalSizeBits = settings.MemorySizeBits;
            LogTotalSizeBytes = 1L << LogTotalSizeBits;
            BufferSize = (int)(LogTotalSizeBytes / (1L << LogPageSizeBits));
            BufferSizeMask = BufferSize - 1;

            // HeadOffset lag (from tail).
            var headOffsetLagSize = BufferSize - 1; // (ReadCache ? ReadCacheHeadOffsetLagNumPages : HeadOffsetLagNumPages);
            if (BufferSize > 1 && HeadOffsetExtraLag) headOffsetLagSize--;

            HeadOffsetLagAddress = (long)headOffsetLagSize << LogPageSizeBits;

            // ReadOnlyOffset lag (from tail). This should not exceed HeadOffset lag.
            LogMutableFraction = settings.MutableFraction;
            ReadOnlyLagAddress = Math.Min((long)(LogMutableFraction * BufferSize) << LogPageSizeBits, HeadOffsetLagAddress);

            // Segment size
            LogSegmentSizeBits = settings.SegmentSizeBits;
            SegmentSize = 1L << LogSegmentSizeBits;
            SegmentBufferSize = 1 + (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

            if (SegmentSize < PageSize)
                throw new FasterException("Segment must be at least of page size");

            PageStatusIndicator = new FullPageStatus[BufferSize];

            if (!IsNullDevice)
            {
                PendingFlush = new PendingFlushList[BufferSize];
                for (int i = 0; i < BufferSize; i++)
                    PendingFlush[i] = new PendingFlushList();
            }
            device = settings.LogDevice;
            sectorSize = (int)device.SectorSize;

            if (PageSize < sectorSize)
                throw new FasterException($"Page size must be at least of device sector size ({sectorSize} bytes). Set PageSizeBits accordingly.");

            AlignedPageSizeBytes = ((PageSize + (sectorSize - 1)) & ~(sectorSize - 1));
        }

        /// <summary>
        /// Initialize allocator
        /// </summary>
        /// <param name="firstValidAddress"></param>
        protected void Initialize(long firstValidAddress)
        {
            Debug.Assert(firstValidAddress <= PageSize);

            bufferPool = new SectorAlignedBufferPool(1, sectorSize);

            if (BufferSize > 0)
            {
                long tailPage = firstValidAddress >> LogPageSizeBits;
                int tailPageIndex = (int)(tailPage % BufferSize);
                AllocatePage(tailPageIndex);

                // Allocate next page as well
                int nextPageIndex = (int)(tailPage + 1) % BufferSize;
                if ((!IsAllocated(nextPageIndex)))
                {
                    AllocatePage(nextPageIndex);
                }
            }

            if (PreallocateLog)
            {
                for (int i = 0; i < BufferSize; i++)
                {
                    if ((!IsAllocated(i)))
                    {
                        AllocatePage(i);
                    }
                }
            }

            SafeReadOnlyAddress = firstValidAddress;
            ReadOnlyAddress = firstValidAddress;
            SafeHeadAddress = firstValidAddress;
            HeadAddress = firstValidAddress;
            ClosedUntilAddress = firstValidAddress;
            FlushedUntilAddress = firstValidAddress;
            BeginAddress = firstValidAddress;

            TailPageOffset.Page = (int)(firstValidAddress >> LogPageSizeBits);
            TailPageOffset.Offset = (int)(firstValidAddress & PageSizeMask);
        }

        /// <summary>
        /// Dispose allocator
        /// </summary>
        public virtual void Dispose()
        {
            disposed = true;

            if (ownedEpoch)
                epoch.Dispose();
            bufferPool.Free();

            OnReadOnlyObserver?.OnCompleted();
            OnEvictionObserver?.OnCompleted();
        }

        /// <summary>
        /// Delete in-memory portion of the log
        /// </summary>
        internal abstract void DeleteFromMemory();


        /// <summary>
        /// Segment size
        /// </summary>
        /// <returns></returns>
        public long GetSegmentSize()
        {
            return SegmentSize;
        }

        /// <summary>
        /// Get tail address
        /// </summary>
        /// <returns></returns>
        public long GetTailAddress()
        {
            var local = TailPageOffset;
            if (local.Offset >= PageSize)
            {
                local.Page++;
                local.Offset = 0;
            }
            return ((long)local.Page << LogPageSizeBits) | (uint)local.Offset;
        }

        /// <summary>
        /// Get page
        /// </summary>
        /// <param name="logicalAddress"></param>
        /// <returns></returns>
        public long GetPage(long logicalAddress)
        {
            return (logicalAddress >> LogPageSizeBits);
        }

        /// <summary>
        /// Get page index for page
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public int GetPageIndexForPage(long page)
        {
            return (int)(page % BufferSize);
        }

        /// <summary>
        /// Get page index for address
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public int GetPageIndexForAddress(long address)
        {
            return (int)((address >> LogPageSizeBits) % BufferSize);
        }

        /// <summary>
        /// Get capacity (number of pages)
        /// </summary>
        /// <returns></returns>
        public int GetCapacityNumPages()
        {
            return BufferSize;
        }


        /// <summary>
        /// Get page size
        /// </summary>
        /// <returns></returns>
        public long GetPageSize()
        {
            return PageSize;
        }

        /// <summary>
        /// Get offset in page
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public long GetOffsetInPage(long address)
        {
            return address & PageSizeMask;
        }

        /// <summary>
        /// Get sector size for main hlog device
        /// </summary>
        /// <returns></returns>
        public int GetDeviceSectorSize()
        {
            return sectorSize;
        }

        /// <summary>
        /// Try allocate, no thread spinning allowed
        /// May return 0 in case of inability to allocate
        /// </summary>
        /// <param name="numSlots"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryAllocate(int numSlots = 1)
        {
            if (numSlots > PageSize)
                throw new FasterException("Entry does not fit on page");

            PageOffset localTailPageOffset = default;

            // Necessary to check because threads keep retrying and we do not
            // want to overflow offset more than once per thread
            if (TailPageOffset.Offset > PageSize)
                return 0;

            // Determine insertion index.
            localTailPageOffset.PageAndOffset = Interlocked.Add(ref TailPageOffset.PageAndOffset, numSlots);

            int page = localTailPageOffset.Page;
            int offset = localTailPageOffset.Offset - numSlots;

            #region HANDLE PAGE OVERFLOW
            if (localTailPageOffset.Offset > PageSize)
            {
                if (offset > PageSize)
                {
                    return 0;
                }

                // The thread that "makes" the offset incorrect
                // is the one that is elected to fix it and
                // shift read-only/head.

                long shiftAddress = ((long)(localTailPageOffset.Page + 1)) << LogPageSizeBits;
                PageAlignedShiftReadOnlyAddress(shiftAddress);
                PageAlignedShiftHeadAddress(shiftAddress);

                if (CannotAllocate(localTailPageOffset.Page + 1))
                {
                    // We should not allocate the next page; reset to end of page
                    // so that next attempt can retry
                    localTailPageOffset.Offset = PageSize;
                    Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
                    return 0;
                }

                // Allocate next page in advance, if needed
                int nextPageIndex = (localTailPageOffset.Page + 2) % BufferSize;
                if ((!IsAllocated(nextPageIndex)))
                {
                    AllocatePage(nextPageIndex);
                }

                localTailPageOffset.Page++;
                localTailPageOffset.Offset = 0;
                TailPageOffset = localTailPageOffset;

                return 0;
            }
            #endregion

            return (((long)page) << LogPageSizeBits) | ((long)offset);
        }

        private bool CannotAllocate(int page)
        {
            return
                (page >= BufferSize + (ClosedUntilAddress >> LogPageSizeBits));
        }

        /// <summary>
        /// Used by applications to make the current state of the database immutable quickly
        /// </summary>
        /// <param name="tailAddress"></param>
        /// <param name="notifyDone"></param>
        public bool ShiftReadOnlyToTail(out long tailAddress, out SemaphoreSlim notifyDone)
        {
            notifyDone = null;
            tailAddress = GetTailAddress();
            long localTailAddress = tailAddress;
            long currentReadOnlyOffset = ReadOnlyAddress;
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, tailAddress, out long oldReadOnlyOffset))
            {
                notifyFlushedUntilAddressSemaphore = new SemaphoreSlim(0);
                notifyDone = notifyFlushedUntilAddressSemaphore;
                notifyFlushedUntilAddress = localTailAddress;
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(localTailAddress));
                return true;
            }
            return false;
        }

        /// <summary>
        /// Used by applications to move read-only forward
        /// </summary>
        /// <param name="newReadOnlyAddress"></param>
        public bool ShiftReadOnlyAddress(long newReadOnlyAddress)
        {
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, newReadOnlyAddress, out long oldReadOnlyOffset))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(newReadOnlyAddress));
                return true;
            }
            return false;
        }

        /// <summary>
        /// Shift begin address
        /// </summary>
        /// <param name="newBeginAddress"></param>
        public void ShiftBeginAddress(long newBeginAddress)
        {
            // First update the begin address
            if (!Utility.MonotonicUpdate(ref BeginAddress, newBeginAddress, out long oldBeginAddress))
                return;

            var b = oldBeginAddress >> LogSegmentSizeBits != newBeginAddress >> LogSegmentSizeBits;

            // Shift read-only address
            try
            {
                epoch.Resume();
                ShiftReadOnlyAddress(newBeginAddress);
            }
            finally
            {
                epoch.Suspend();
            }

            // Wait for flush to complete
            while (FlushedUntilAddress < newBeginAddress) Thread.Yield();

            // Then shift head address
            var h = Utility.MonotonicUpdate(ref HeadAddress, newBeginAddress, out long old);

            if (h || b)
            {
                try
                {
                    epoch.Resume();
                    epoch.BumpCurrentEpoch(() =>
                    {
                        if (h)
                            OnPagesClosed(newBeginAddress);
                        if (b)
                            TruncateUntilAddress(newBeginAddress);
                    });
                }
                finally
                {
                    epoch.Suspend();
                }
            }
        }

        /// <summary>
        /// Wraps <see cref="IDevice.TruncateUntilAddress(long)"/> when an allocator potentially has to interact with multiple devices
        /// </summary>
        /// <param name="toAddress"></param>
        protected virtual void TruncateUntilAddress(long toAddress)
        {
            device.TruncateUntilAddress(toAddress);
        }

        internal virtual bool TryComplete()
        {
            return device.TryComplete();
        }

        /// <summary>
        /// Seal: make sure there are no longer any threads writing to the page
        /// Flush: send page to secondary store
        /// </summary>
        /// <param name="newSafeReadOnlyAddress"></param>
        public void OnPagesMarkedReadOnly(long newSafeReadOnlyAddress)
        {
            if (Utility.MonotonicUpdate(ref SafeReadOnlyAddress, newSafeReadOnlyAddress, out long oldSafeReadOnlyAddress))
            {
                Debug.WriteLine("SafeReadOnly shifted from {0:X} to {1:X}", oldSafeReadOnlyAddress, newSafeReadOnlyAddress);
                if (OnReadOnlyObserver != null)
                {
                    using var iter = Scan(oldSafeReadOnlyAddress, newSafeReadOnlyAddress, ScanBufferingMode.NoBuffering);
                    OnReadOnlyObserver?.OnNext(iter);
                }
                AsyncFlushPages(oldSafeReadOnlyAddress, newSafeReadOnlyAddress);
            }
        }

        /// <summary>
        /// Action to be performed for when all threads have 
        /// agreed that a page range is closed.
        /// </summary>
        /// <param name="newSafeHeadAddress"></param>
        public void OnPagesClosed(long newSafeHeadAddress)
        {
            if (Utility.MonotonicUpdate(ref SafeHeadAddress, newSafeHeadAddress, out long oldSafeHeadAddress))
            {
                Debug.WriteLine("SafeHeadOffset shifted from {0:X} to {1:X}", oldSafeHeadAddress, newSafeHeadAddress);

                // Also shift begin address if we are using a null storage device
                if (IsNullDevice)
                    Utility.MonotonicUpdate(ref BeginAddress, newSafeHeadAddress, out _);

                for (long closePageAddress = oldSafeHeadAddress & ~PageSizeMask; closePageAddress < newSafeHeadAddress; closePageAddress += PageSize)
                {
                    long start = oldSafeHeadAddress > closePageAddress ? oldSafeHeadAddress : closePageAddress;
                    long end = newSafeHeadAddress < closePageAddress + PageSize ? newSafeHeadAddress : closePageAddress + PageSize;
                    MemoryPageScan(start, end);

                    if (newSafeHeadAddress < closePageAddress + PageSize)
                    {
                        // Partial page - do not close
                        // Future work: clear partial page here
                        return;
                    }

                    int closePage = (int)(closePageAddress >> LogPageSizeBits);
                    int closePageIndex = closePage % BufferSize;

                    if (!IsAllocated(closePageIndex))
                        AllocatePage(closePageIndex);
                    else
                        ClearPage(closePage);

                    Utility.MonotonicUpdate(ref PageStatusIndicator[closePageIndex].LastClosedUntilAddress, closePageAddress + PageSize, out _);
                    ShiftClosedUntilAddress();
                    if (ClosedUntilAddress > FlushedUntilAddress)
                    {
                        throw new FasterException($"Closed address {ClosedUntilAddress} exceeds flushed address {FlushedUntilAddress}");
                    }
                }
            }
        }

        internal void DebugPrintAddresses(long closePageAddress)
        {
            var _flush = FlushedUntilAddress;
            var _readonly = ReadOnlyAddress;
            var _safereadonly = SafeReadOnlyAddress;
            var _tail = GetTailAddress();
            var _head = HeadAddress;
            var _safehead = SafeHeadAddress;

            Console.WriteLine("ClosePageAddress: {0}.{1}", GetPage(closePageAddress), GetOffsetInPage(closePageAddress));
            Console.WriteLine("FlushedUntil: {0}.{1}", GetPage(_flush), GetOffsetInPage(_flush));
            Console.WriteLine("Tail: {0}.{1}", GetPage(_tail), GetOffsetInPage(_tail));
            Console.WriteLine("Head: {0}.{1}", GetPage(_head), GetOffsetInPage(_head));
            Console.WriteLine("SafeHead: {0}.{1}", GetPage(_safehead), GetOffsetInPage(_safehead));
            Console.WriteLine("ReadOnly: {0}.{1}", GetPage(_readonly), GetOffsetInPage(_readonly));
            Console.WriteLine("SafeReadOnly: {0}.{1}", GetPage(_safereadonly), GetOffsetInPage(_safereadonly));
        }

        /// <summary>
        /// Called every time a new tail page is allocated. Here the read-only is 
        /// shifted only to page boundaries unlike ShiftReadOnlyToTail where shifting
        /// can happen to any fine-grained address.
        /// </summary>
        /// <param name="currentTailAddress"></param>
        private void PageAlignedShiftReadOnlyAddress(long currentTailAddress)
        {
            long currentReadOnlyAddress = ReadOnlyAddress;
            long pageAlignedTailAddress = currentTailAddress & ~PageSizeMask;
            long desiredReadOnlyAddress = (pageAlignedTailAddress - ReadOnlyLagAddress);
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, desiredReadOnlyAddress, out long oldReadOnlyAddress))
            {
                Debug.WriteLine("Allocate: Moving read-only offset from {0:X} to {1:X}", oldReadOnlyAddress, desiredReadOnlyAddress);
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(desiredReadOnlyAddress));
            }
        }

        /// <summary>
        /// Called whenever a new tail page is allocated or when the user is checking for a failed memory allocation
        /// Tries to shift head address based on the head offset lag size.
        /// </summary>
        /// <param name="currentTailAddress"></param>
        private void PageAlignedShiftHeadAddress(long currentTailAddress)
        {
            //obtain local values of variables that can change
            long currentHeadAddress = HeadAddress;
            long currentFlushedUntilAddress = FlushedUntilAddress;
            long pageAlignedTailAddress = currentTailAddress & ~PageSizeMask;
            long desiredHeadAddress = (pageAlignedTailAddress - HeadOffsetLagAddress);

            long newHeadAddress = desiredHeadAddress;
            if (currentFlushedUntilAddress < newHeadAddress)
            {
                newHeadAddress = currentFlushedUntilAddress;
            }
            newHeadAddress &= ~PageSizeMask;

            if (ReadCache && (newHeadAddress > HeadAddress))
                EvictCallback(HeadAddress, newHeadAddress);

            if (Utility.MonotonicUpdate(ref HeadAddress, newHeadAddress, out long oldHeadAddress))
            {
                Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
            }
        }

        /// <summary>
        /// Tries to shift head address to specified value
        /// </summary>
        /// <param name="desiredHeadAddress"></param>
        public long ShiftHeadAddress(long desiredHeadAddress)
        {
            //obtain local values of variables that can change
            long currentFlushedUntilAddress = FlushedUntilAddress;

            long newHeadAddress = desiredHeadAddress;
            if (currentFlushedUntilAddress < newHeadAddress)
            {
                newHeadAddress = currentFlushedUntilAddress;
            }

            if (ReadCache && (newHeadAddress > HeadAddress))
                EvictCallback(HeadAddress, newHeadAddress);

            if (Utility.MonotonicUpdate(ref HeadAddress, newHeadAddress, out long oldHeadAddress))
            {
                Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
            }
            return newHeadAddress;
        }

        /// <summary>
        /// Every async flush callback tries to update the flushed until address to the latest value possible
        /// Is there a better way to do this with enabling fine-grained addresses (not necessarily at page boundaries)?
        /// </summary>
        protected void ShiftFlushedUntilAddress()
        {
            long currentFlushedUntilAddress = FlushedUntilAddress;
            long page = GetPage(currentFlushedUntilAddress);

            bool update = false;
            long pageLastFlushedAddress = PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress;
            while (pageLastFlushedAddress >= currentFlushedUntilAddress && currentFlushedUntilAddress >= (page << LogPageSizeBits))
            {
                currentFlushedUntilAddress = pageLastFlushedAddress;
                update = true;
                page++;
                pageLastFlushedAddress = PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress;
            }

            if (update)
            {
                if (Utility.MonotonicUpdate(ref FlushedUntilAddress, currentFlushedUntilAddress, out long oldFlushedUntilAddress))
                {
                    uint errorCode = 0;
                    if (errorList.Count > 0)
                    {
                        errorCode = errorList.CheckAndWait(oldFlushedUntilAddress, currentFlushedUntilAddress);
                    }
                    FlushCallback?.Invoke(
                        new CommitInfo
                        {
                            BeginAddress = BeginAddress,
                            FromAddress = oldFlushedUntilAddress,
                            UntilAddress = currentFlushedUntilAddress,
                            ErrorCode = errorCode
                        });

                    if (errorList.Count > 0)
                    {
                        errorList.RemoveUntil(currentFlushedUntilAddress);
                    }

                    if ((oldFlushedUntilAddress < notifyFlushedUntilAddress) && (currentFlushedUntilAddress >= notifyFlushedUntilAddress))
                    {
                        notifyFlushedUntilAddressSemaphore.Release();
                    }
                }
            }
        }

        /// <summary>
        /// Shift ClosedUntil address
        /// </summary>
        protected void ShiftClosedUntilAddress()
        {
            long currentClosedUntilAddress = ClosedUntilAddress;
            long page = GetPage(currentClosedUntilAddress);

            bool update = false;
            long pageLastClosedAddress = PageStatusIndicator[page % BufferSize].LastClosedUntilAddress;
            while (pageLastClosedAddress >= currentClosedUntilAddress && currentClosedUntilAddress >= (page << LogPageSizeBits))
            {
                currentClosedUntilAddress = pageLastClosedAddress;
                update = true;
                page++;
                pageLastClosedAddress = PageStatusIndicator[(int)(page % BufferSize)].LastClosedUntilAddress;
            }

            if (update)
            {
                Utility.MonotonicUpdate(ref ClosedUntilAddress, currentClosedUntilAddress, out _);
            }
        }

        /// <summary>
        /// Address for notification of flushed-until
        /// </summary>
        public long notifyFlushedUntilAddress;

        /// <summary>
        /// Semaphore for notification of flushed-until
        /// </summary>
        public SemaphoreSlim notifyFlushedUntilAddressSemaphore;


        /// <summary>
        /// Reset for recovery
        /// </summary>
        /// <param name="tailAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="beginAddress"></param>
        /// <param name="readonlyAddress"></param>
        public void RecoveryReset(long tailAddress, long headAddress, long beginAddress, long readonlyAddress)
        {
            long tailPage = GetPage(tailAddress);
            long offsetInPage = GetOffsetInPage(tailAddress);
            TailPageOffset.Page = (int)tailPage;
            TailPageOffset.Offset = (int)offsetInPage;

            // Allocate current page if necessary
            var pageIndex = (TailPageOffset.Page % BufferSize);
            if (!IsAllocated(pageIndex))
                AllocatePage(pageIndex);

            // Allocate next page as well - this is an invariant in the allocator!
            var nextPageIndex = (pageIndex + 1) % BufferSize;
            if (!IsAllocated(nextPageIndex))
                AllocatePage(nextPageIndex);

            BeginAddress = beginAddress;
            HeadAddress = headAddress;
            SafeHeadAddress = headAddress;
            ClosedUntilAddress = headAddress;
            FlushedUntilAddress = readonlyAddress;
            ReadOnlyAddress = readonlyAddress;
            SafeReadOnlyAddress = readonlyAddress;

            // for the last page which contains tailoffset, it must be open
            pageIndex = GetPageIndexForAddress(tailAddress);

            // clear the last page starting from tail address
            ClearPage(pageIndex, (int)GetOffsetInPage(tailAddress));

            // Printing debug info
            Debug.WriteLine("******* Recovered HybridLog Stats *******");
            Debug.WriteLine("Head Address: {0}", HeadAddress);
            Debug.WriteLine("Safe Head Address: {0}", SafeHeadAddress);
            Debug.WriteLine("ReadOnly Address: {0}", ReadOnlyAddress);
            Debug.WriteLine("Safe ReadOnly Address: {0}", SafeReadOnlyAddress);
            Debug.WriteLine("Tail Address: {0}", tailAddress);
        }

        /// <summary>
        /// Invoked by users to obtain a record from disk. It uses sector aligned memory to read 
        /// the record efficiently into memory.
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        /// 
        internal void AsyncReadRecordToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<Key, Value> context)
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
            asyncResult.context.record = record;
            device.ReadAsync(alignedFileOffset,
                        (IntPtr)asyncResult.context.record.aligned_pointer,
                        alignedReadLength,
                        callback,
                        asyncResult);
        }

        /// <summary>
        /// Read record to memory - simple version
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        internal void AsyncReadRecordToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, ref SimpleReadContext context)
        {
            ulong fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            ulong alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            uint alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            context.record = bufferPool.Get((int)alignedReadLength);
            context.record.valid_offset = (int)(fileOffset - alignedFileOffset);
            context.record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            context.record.required_bytes = numBytes;

            device.ReadAsync(alignedFileOffset,
                        (IntPtr)context.record.aligned_pointer,
                        alignedReadLength,
                        callback,
                        context);
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
        /// <param name="devicePageOffset"></param>
        /// <param name="logDevice"></param>
        /// <param name="objectLogDevice"></param>
        public void AsyncReadPagesFromDevice<TContext>(
                                long readPageStart,
                                int numPages,
                                long untilAddress,
                                DeviceIOCompletionCallback callback,
                                TContext context,
                                long devicePageOffset = 0,
                                IDevice logDevice = null, IDevice objectLogDevice = null)
        {
            AsyncReadPagesFromDevice(readPageStart, numPages, untilAddress, callback, context,
                out _, devicePageOffset, logDevice, objectLogDevice);
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
        /// <param name="completed"></param>
        /// <param name="devicePageOffset"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        private void AsyncReadPagesFromDevice<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
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
                int pageIndex = (int)(readPage % BufferSize);
                if (!IsAllocated(pageIndex))
                {
                    // Allocate a new page
                    AllocatePage(pageIndex);
                }
                else
                {
                    ClearPage(readPage);
                }
                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    offset = devicePageOffset,
                    context = context,
                    handle = completed,
                    maxPtr = PageSize
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

        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        public void AsyncFlushPages(long fromAddress, long untilAddress)
        {
            long startPage = fromAddress >> LogPageSizeBits;
            long endPage = untilAddress >> LogPageSizeBits;
            int numPages = (int)(endPage - startPage);

            long offsetInStartPage = GetOffsetInPage(fromAddress);
            long offsetInEndPage = GetOffsetInPage(untilAddress);

            // Extra (partial) page being flushed
            if (offsetInEndPage > 0)
                numPages++;

            /* Request asynchronous writes to the device. If waitForPendingFlushComplete
             * is set, then a CountDownEvent is set in the callback handle.
             */
            for (long flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                long pageStartAddress = flushPage << LogPageSizeBits;
                long pageEndAddress = (flushPage + 1) << LogPageSizeBits;

                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    page = flushPage,
                    count = 1,
                    partial = false,
                    fromAddress = pageStartAddress,
                    untilAddress = pageEndAddress
                };
                if (
                    ((fromAddress > pageStartAddress) && (fromAddress < pageEndAddress)) ||
                    ((untilAddress > pageStartAddress) && (untilAddress < pageEndAddress))
                    )
                {
                    asyncResult.partial = true;

                    if (untilAddress < pageEndAddress)
                        asyncResult.untilAddress = untilAddress;

                    if (fromAddress > pageStartAddress)
                        asyncResult.fromAddress = fromAddress;
                }

                if (asyncResult.untilAddress <= BeginAddress)
                {
                    // Short circuit as no flush needed
                    Utility.MonotonicUpdate(ref PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress, BeginAddress, out _);
                    ShiftFlushedUntilAddress();
                    continue;
                }

                if (IsNullDevice)
                {
                    // Short circuit as no flush needed
                    Utility.MonotonicUpdate(ref PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress, asyncResult.untilAddress, out _);
                    ShiftFlushedUntilAddress();
                    continue;
                }

                // Partial page starting point, need to wait until the
                // ongoing adjacent flush is completed to ensure correctness
                if (GetOffsetInPage(asyncResult.fromAddress) > 0)
                {
                    var index = GetPageIndexForAddress(asyncResult.fromAddress);

                    // Try to merge request with existing adjacent (earlier) pending requests
                    while (PendingFlush[index].RemovePreviousAdjacent(asyncResult.fromAddress, out var existingRequest))
                    {
                        asyncResult.fromAddress = existingRequest.fromAddress;
                    }

                    // Enqueue work in shared queue
                    if (PendingFlush[index].Add(asyncResult))
                    {
                        // Perform work from shared queue if possible
                        if (PendingFlush[index].RemoveNextAdjacent(FlushedUntilAddress, out PageAsyncFlushResult<Empty> request))
                        {
                            WriteAsync(request.fromAddress >> LogPageSizeBits, AsyncFlushPageCallback, request);
                        }
                    }
                    else
                    {
                        // Could not add to pending flush list, treat as a failed write
                        AsyncFlushPageCallback(1, 0, asyncResult);
                    }
                }
                else
                    WriteAsync(flushPage, AsyncFlushPageCallback, asyncResult);
            }
        }

        /// <summary>
        /// Flush pages asynchronously
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="flushPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void AsyncFlushPages<TContext>(long flushPageStart, int numPages, DeviceIOCompletionCallback callback, TContext context)
        {
            for (long flushPage = flushPageStart; flushPage < (flushPageStart + numPages); flushPage++)
            {
                int pageIndex = GetPageIndexForPage(flushPage);
                var asyncResult = new PageAsyncFlushResult<TContext>()
                {
                    page = flushPage,
                    context = context,
                    count = 1,
                    partial = false,
                    untilAddress = (flushPage + 1) << LogPageSizeBits
                };

                WriteAsync(flushPage, callback, asyncResult);
            }
        }

        /// <summary>
        /// Flush pages from startPage (inclusive) to endPage (exclusive)
        /// to specified log device and obj device
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="endPage"></param>
        /// <param name="endLogicalAddress"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="completedSemaphore"></param>
        public void AsyncFlushPagesToDevice(long startPage, long endPage, long endLogicalAddress, IDevice device, IDevice objectLogDevice, out SemaphoreSlim completedSemaphore)
        {
            int totalNumPages = (int)(endPage - startPage);
            completedSemaphore = new SemaphoreSlim(0);
            var asyncResult = new PageAsyncFlushResult<Empty>
            {
                completedSemaphore = completedSemaphore,
                count = totalNumPages
            };
            var localSegmentOffsets = new long[SegmentBufferSize];

            for (long flushPage = startPage; flushPage < endPage; flushPage++)
            {

                var pageSize = PageSize;

                if (flushPage == endPage - 1)
                    pageSize = (int)(endLogicalAddress - (flushPage << LogPageSizeBits));

                // Intended destination is flushPage
                WriteAsyncToDevice(startPage, flushPage, pageSize, AsyncFlushPageToDeviceCallback, asyncResult, device, objectLogDevice, localSegmentOffsets);
            }
        }

        /// <summary>
        /// Async get from disk
        /// </summary>
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        public void AsyncGetFromDisk(long fromLogical,
                              int numBytes,
                              AsyncIOContext<Key, Value> context,
                              SectorAlignedMemory result = default)
        {
            if (epoch.ThisInstanceProtected()) // Do not spin for unprotected IO threads
            {
                while (device.Throttle())
                {
                    device.TryComplete();
                    Thread.Yield();
                    epoch.ProtectAndDrain();
                }
            }

            if (result == null)
                AsyncReadRecordToMemory(fromLogical, numBytes, AsyncGetFromDiskCallback, context);
            else
                AsyncReadRecordObjectsToMemory(fromLogical, numBytes, AsyncGetFromDiskCallback, context, result);
        }

        private void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncGetFromDiskCallback error: {0}", errorCode);
            }

            var result = (AsyncGetFromDiskResult<AsyncIOContext<Key, Value>>)context;

            var ctx = result.context;
            try
            {
                var record = ctx.record.GetValidPointer();
                int requiredBytes = GetRequiredRecordSize((long)record, ctx.record.available_bytes);
                if (ctx.record.available_bytes >= requiredBytes)
                {
                    // We have the complete record.
                    if (RetrievedFullRecord(record, ref ctx))
                    {
                        // ReadAtAddress does not have a request key, so it is an implicit match.
                        if (ctx.request_key is null || comparer.Equals(ref ctx.request_key.Get(), ref GetContextRecordKey(ref ctx)))
                        {
                            // The keys are same, so I/O is complete
                            // ctx.record = result.record;
                            if (ctx.callbackQueue != null)
                                ctx.callbackQueue.Enqueue(ctx);
                            else
                                ctx.asyncOperation.TrySetResult(ctx);
                        }
                        else
                        {
                            // Keys are not same. I/O is not complete
                            ctx.logicalAddress = GetInfoFromBytePointer(record).PreviousAddress;
                            if (ctx.logicalAddress >= BeginAddress)
                            {
                                ctx.record.Return();
                                ctx.record = ctx.objBuffer = default;
                                AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, ctx);
                            }
                            else
                            {
                                if (ctx.callbackQueue != null)
                                    ctx.callbackQueue.Enqueue(ctx);
                                else
                                    ctx.asyncOperation.TrySetResult(ctx);
                            }
                        }
                    }
                }
                else
                {
                    ctx.record.Return();
                    AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, ctx);
                }
            }
            catch (Exception e)
            {
                if (ctx.asyncOperation != null)
                    ctx.asyncOperation.TrySetException(e);
                else
                    throw;
            }
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        private void AsyncFlushPageCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("AsyncFlushPageCallback error: {0}", errorCode);
                }

                // Set the page status to flushed
                PageAsyncFlushResult<Empty> result = (PageAsyncFlushResult<Empty>)context;

                if (Interlocked.Decrement(ref result.count) == 0)
                {
                    if (errorCode != 0)
                    {
                        errorList.Add(result.fromAddress);
                    }
                    Utility.MonotonicUpdate(ref PageStatusIndicator[result.page % BufferSize].LastFlushedUntilAddress, result.untilAddress, out _);
                    ShiftFlushedUntilAddress();
                    result.Free();
                }

                var _flush = FlushedUntilAddress;
                if (GetOffsetInPage(_flush) > 0 && PendingFlush[GetPage(_flush) % BufferSize].RemoveNextAdjacent(_flush, out PageAsyncFlushResult<Empty> request))
                {
                    WriteAsync(request.fromAddress >> LogPageSizeBits, AsyncFlushPageCallback, request);
                }
            }
            catch when (disposed) { }
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        protected void AsyncFlushPageToDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("AsyncFlushPageToDeviceCallback error: {0}", errorCode);
                }

                PageAsyncFlushResult<Empty> result = (PageAsyncFlushResult<Empty>)context;
                if (Interlocked.Decrement(ref result.count) == 0)
                {
                    result.Free();
                }
            }
            catch when (disposed) { }
        }

        /// <summary>
        /// Serialize to log
        /// </summary>
        /// <param name="src"></param>
        /// <param name="physicalAddress"></param>
        public virtual void Serialize(ref Key src, long physicalAddress)
        {
            GetKey(physicalAddress) = src;
        }

        /// <summary>
        /// Serialize to log
        /// </summary>
        /// <param name="src"></param>
        /// <param name="physicalAddress"></param>
        public virtual void Serialize(ref Value src, long physicalAddress)
        {
            GetValue(physicalAddress) = src;
        }

        internal string PrettyPrint(long address)
        {
            return $"{GetPage(address)}:{GetOffsetInPage(address)}";
        }
    }
}

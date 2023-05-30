﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class VariableLengthBlittableScanIterator<Key, Value> : ScanIteratorBase, IFasterScanIterator<Key, Value>, IPushScanIterator<Key>
    {
        private readonly FasterKV<Key, Value> store;
        private readonly VariableLengthBlittableAllocator<Key, Value> hlog;
        private readonly IFasterEqualityComparer<Key> comparer;
        private readonly BlittableFrame frame;

        private SectorAlignedMemory memory;
        private readonly bool forceInMemory;

        private long currentPhysicalAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store"></param>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="forceInMemory"/> is true.</param>
        /// <param name="forceInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        internal VariableLengthBlittableScanIterator(FasterKV<Key, Value> store, VariableLengthBlittableAllocator<Key, Value> hlog, long beginAddress, long endAddress, 
                ScanBufferingMode scanBufferingMode, LightEpoch epoch, bool forceInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            this.forceInMemory = forceInMemory;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Constructor for use with tail-to-head push iteration of the passed key's record versions
        /// </summary>
        internal VariableLengthBlittableScanIterator(FasterKV<Key, Value> store, VariableLengthBlittableAllocator<Key, Value> hlog, IFasterEqualityComparer<Key> comparer, long beginAddress, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, hlog.GetTailAddress(), ScanBufferingMode.SinglePageBuffering, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.hlog = hlog;
            this.comparer = comparer;
            this.forceInMemory = false;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Gets reference to current key
        /// </summary>
        public ref Key GetKey() => ref hlog.GetKey(currentPhysicalAddress);

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        public ref Value GetValue() => ref hlog.GetValue(currentPhysicalAddress);

        ref RecordInfo IPushScanIterator<Key>.GetLockableInfo()
        {
            // hlog.HeadAddress may have been incremented so use ClosedUntilAddress to avoid a false negative assert (not worth raising the temp headAddress out of BeginGetNext just for this).
            Debug.Assert(currentPhysicalAddress >= hlog.ClosedUntilAddress, "GetLockableInfo() should be in-memory");
            Debug.Assert(epoch.ThisInstanceProtected(), "GetLockableInfo() should be called with the epoch held");
            return ref hlog.GetInfo(currentPhysicalAddress);
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool GetNext(out RecordInfo recordInfo)
        {
            recordInfo = default;

            while (true)
            {
                currentAddress = nextAddress;
                if (currentAddress >= endAddress)
                    return false;

                epoch?.Resume();
                long headAddress = hlog.HeadAddress;

                if (currentAddress < hlog.BeginAddress && !forceInMemory)
                {
                    epoch?.Suspend();
                    throw new FasterException("Iterator address is less than log BeginAddress " + hlog.BeginAddress);
                }

                // If currentAddress < headAddress and we're not buffering and not guaranteeing the records are in memory, fail.
                if (frameSize == 0 && currentAddress < headAddress && !forceInMemory)
                {
                    epoch?.Suspend();
                    throw new FasterException("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                var currentPage = currentAddress >> hlog.LogPageSizeBits;
                var offset = currentAddress & hlog.PageSizeMask;

                if (currentAddress < headAddress && !forceInMemory)
                    BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, endAddress);

                long physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);
                int recordSize = hlog.GetRecordSize(physicalAddress).Item2;

                // If record does not fit on page, skip to the next page.
                if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                {
                    nextAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                recordInfo = hlog.GetInfo(physicalAddress);
                if (recordInfo.SkipOnScan || recordInfo.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }

                currentPhysicalAddress = physicalAddress;

                // We will return control to the caller, which means releasing epoch protection, and we don't want the caller to lock.
                // Copy the entire record into bufferPool memory, so we do not have a ref to log data outside epoch protection.
                // Lock to ensure no value tearing while copying to temp storage.
                memory?.Return();
                memory = null;
                if (currentAddress >= headAddress || forceInMemory)
                {
                    OperationStackContext<Key, Value> stackCtx = default;
                    try
                    {
                        // GetKey() and GetLockableInfo() should work but for safety and consistency with other allocators use physicalAddress.
                        if (currentAddress >= headAddress && store is not null)
                            store.LockForScan(ref stackCtx, ref hlog.GetKey(physicalAddress), ref hlog.GetInfo(physicalAddress));

                        memory = hlog.bufferPool.Get(recordSize);
                        unsafe
                        { 
                            Buffer.MemoryCopy((byte*)currentPhysicalAddress, memory.aligned_pointer, recordSize, recordSize);
                            currentPhysicalAddress = (long)memory.aligned_pointer;
                        }
                    }
                    finally
                    {
                        if (stackCtx.recSrc.HasLock)
                            store.UnlockForScan(ref stackCtx, ref GetKey(), ref ((IPushScanIterator<Key>)this).GetLockableInfo());
                    }
                }

                // Success
                epoch?.Suspend();
                return true;
            }
        }

        /// <summary>
        /// Get previous record and keep the epoch held while we call the user's scan functions
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool IPushScanIterator<Key>.BeginGetPrevInMemory(ref Key key, out RecordInfo recordInfo, out bool continueOnDisk)
        {
            recordInfo = default;
            continueOnDisk = false;

            while (true)
            {
                // "nextAddress" is reused as "previous address" for this operation.
                currentAddress = nextAddress;
                if (currentAddress < hlog.HeadAddress)
                {
                    continueOnDisk = currentAddress >= hlog.BeginAddress;
                    return false;
                }

                epoch?.Resume();
                var headAddress = hlog.HeadAddress;

                var currentPage = currentAddress >> hlog.LogPageSizeBits;
                var offset = currentAddress & hlog.PageSizeMask;

                long physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);

                recordInfo = hlog.GetInfo(physicalAddress);
                nextAddress = recordInfo.PreviousAddress;
                if (recordInfo.SkipOnScan || recordInfo.IsNull() || !comparer.Equals(ref hlog.GetKey(physicalAddress), ref key))
                {
                    epoch?.Suspend();
                    continue;
                }

                // Success; defer epoch?.Suspend(); to EndGet
                currentPhysicalAddress = physicalAddress;
                return true;
            }
        }

        bool IPushScanIterator<Key>.EndGetPrevInMemory()
        {
            epoch?.Suspend();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long currentAddress, long headAddress, long currentPage, long offset)
        {
            long physicalAddress;
            if (currentAddress >= headAddress || forceInMemory)
                physicalAddress = hlog.GetPhysicalAddress(currentAddress);
            else
                physicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
            return physicalAddress;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns></returns>
        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
            => throw new NotSupportedException("Use GetNext(out RecordInfo) to retrieve references to key/value");

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            memory?.Return();
            memory = null;
            frame?.Dispose();
        }

        internal override void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed, long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => hlog.AsyncReadPagesFromDeviceToFrame(readPageStart, numPages, untilAddress, AsyncReadPagesCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice);

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, object context)
        {
            var result = (PageAsyncReadResult<Empty>)context;

            if (errorCode != 0)
            {
                logger?.LogError($"AsyncReadPagesCallback error: {errorCode}");
                result.cts?.Cancel();
            }

            if (result.freeBuffer1 != null)
            {
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
                result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }
    }
}
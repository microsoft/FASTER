// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class BlittableScanIterator<Key, Value> : ScanIteratorBase, IFasterScanIterator<Key, Value>, IPushScanIterator
    {
        private readonly BlittableAllocator<Key, Value> hlog;
        private readonly BlittableFrame frame;
        private readonly bool forceInMemory;

        private Key currentKey;
        private Value currentValue;
        private long framePhysicalAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="forceInMemory"/> is true.</param>
        /// <param name="forceInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        public BlittableScanIterator(BlittableAllocator<Key, Value> hlog, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, LightEpoch epoch, bool forceInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.hlog = hlog;
            this.forceInMemory = forceInMemory;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Get a reference to the current key
        /// </summary>
        public ref Key GetKey() => ref framePhysicalAddress != 0 ? ref hlog.GetKey(framePhysicalAddress) : ref currentKey;

        /// <summary>
        /// Get a reference to the current value
        /// </summary>
        public ref Value GetValue() => ref framePhysicalAddress != 0 ? ref hlog.GetValue(framePhysicalAddress) : ref currentValue;

        ref RecordInfo IPushScanIterator.GetLockableInfo()
        {
            Debug.Assert(framePhysicalAddress == 0, "GetLockableInfo should be in memory (i.e. should not have a frame)");
            return ref hlog.GetInfo(hlog.GetPhysicalAddress(this.currentAddress));
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool GetNext(out RecordInfo recordInfo) => ((IPushScanIterator)this).BeginGetNext(out recordInfo) && ((IPushScanIterator)this).EndGetNext();

        /// <summary>
        /// Get next record
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool IPushScanIterator.BeginGetNext(out RecordInfo recordInfo)
        {
            recordInfo = default;

            while (true)
            {
                currentAddress = nextAddress;
                if (currentAddress >= endAddress)
                    return false;

                epoch?.Resume();
                var headAddress = hlog.HeadAddress;

                if (currentAddress < hlog.BeginAddress && !forceInMemory)
                {
                    epoch?.Suspend();
                    throw new FasterException("Iterator address is less than log BeginAddress " + hlog.BeginAddress);
                }

                if (frameSize == 0 && currentAddress < headAddress && !forceInMemory)
                {
                    epoch?.Suspend();
                    throw new FasterException("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                var currentPage = currentAddress >> hlog.LogPageSizeBits;
                var offset = currentAddress & hlog.PageSizeMask;

                if (currentAddress < headAddress && !forceInMemory)
                    BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, endAddress);

                long physicalAddress;
                if (currentAddress >= headAddress || forceInMemory)
                {
                    // physicalAddress is in memory; set framePhysicalAddress to 0 so we'll set currentKey and currentValue from physicalAddress below
                    physicalAddress = hlog.GetPhysicalAddress(currentAddress);
                    framePhysicalAddress = 0;
                }
                else
                {
                    // physicalAddress is not in memory, so we'll GetKey and GetValue will use framePhysicalAddress
                    framePhysicalAddress = physicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
                }

                // Check if record fits on page, if not skip to next page
                var recordSize = hlog.GetRecordSize(physicalAddress).Item2;
                if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                {
                    nextAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                ref var info = ref hlog.GetInfo(physicalAddress);
                if (info.SkipOnScan || info.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }

                recordInfo = info;
                if (framePhysicalAddress == 0)
                {
                    // Copy the blittable values to data members; we have no ref into the log after the epoch.Suspend().
                    // Do the copy only for log data, not frame, because this could be a large structure.
                    currentKey = hlog.GetKey(physicalAddress);
                    currentValue = hlog.GetValue(physicalAddress);
                }

                // Success; defer epoch?.Suspend(); to EndGetNext
                return true;
            }
        }

        bool IPushScanIterator.EndGetNext()
        {
            epoch?.Suspend();
            return true;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
        {
            if (GetNext(out recordInfo))
            {
                key = GetKey();
                value = GetValue();
                return true;
            }

            key = default;
            value = default;
            return false;
        }

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
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

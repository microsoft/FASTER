// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class GenericScanIterator<Key, Value> : ScanIteratorBase, IFasterScanIterator<Key, Value>, IPushScanIterator
    {
        private readonly GenericAllocator<Key, Value> hlog;
        private readonly GenericFrame<Key, Value> frame;
        private readonly int recordSize;

        private Key currentKey;
        private Value currentValue;

        private long currentPage = -1, currentOffset = -1, currentFrame = -1;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch"></param>
        /// <param name="logger"></param>
        public GenericScanIterator(GenericAllocator<Key, Value> hlog, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, LightEpoch epoch, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.hlog = hlog;
            recordSize = hlog.GetRecordSize(0).Item2;
            if (frameSize > 0)
                frame = new GenericFrame<Key, Value>(frameSize, hlog.PageSize);
        }

        /// <summary>
        /// Gets reference to current key
        /// </summary>
        /// <returns></returns>
        public ref Key GetKey() => ref currentKey;

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        /// <returns></returns>
        public ref Value GetValue() => ref currentValue;

        ref RecordInfo IPushScanIterator.GetLockableInfo()
        {
            Debug.Assert(currentFrame < 0, "GetLockableInfo() should be in-memory (i.e.should not have a frame)");
            Debug.Assert(epoch.ThisInstanceProtected(), "GetLockableInfo() should be called with the epoch held");
            return ref hlog.values[currentPage][currentOffset].info;
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool GetNext(out RecordInfo recordInfo) => ((IPushScanIterator)this).BeginGetNext(out recordInfo) && ((IPushScanIterator)this).EndGetNext();

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <returns></returns>
        bool IPushScanIterator.BeginGetNext(out RecordInfo recordInfo)
        {
            recordInfo = default;
            currentKey = default;
            currentValue = default;
            currentPage = currentOffset = currentFrame = -1;

            while (true)
            {
                currentAddress = nextAddress;
                if (currentAddress >= endAddress)
                    return false;

                epoch?.Resume();
                var headAddress = hlog.HeadAddress;

                if (currentAddress < hlog.BeginAddress)
                {
                    epoch?.Suspend();
                    throw new FasterException("Iterator address is less than log BeginAddress " + hlog.BeginAddress);
                }

                if (frameSize == 0 && currentAddress < headAddress)
                {
                    epoch?.Suspend();
                    throw new FasterException("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                currentPage = currentAddress >> hlog.LogPageSizeBits;
                currentOffset = (currentAddress & hlog.PageSizeMask) / recordSize;

                if (currentAddress < headAddress)
                    BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, endAddress);

                // Check if record fits on page, if not skip to next page
                if ((currentAddress & hlog.PageSizeMask) + recordSize > hlog.PageSize)
                {
                    nextAddress = (1 + (currentAddress >> hlog.LogPageSizeBits)) << hlog.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                if (currentAddress >= headAddress)
                {
                    // Read record from cached page memory
                    currentPage %= hlog.BufferSize;
                    currentFrame = -1;      // Frame is not used in this case.

                    if (hlog.values[currentPage][currentOffset].info.SkipOnScan)
                    {
                        epoch?.Suspend();
                        continue;
                    }

                    // Copy the object values from cached page memory to data members; we have no ref into the log after the epoch.Suspend()
                    // (except for GetLockableInfo which we know is safe). These are pointer-sized shallow copies.
                    recordInfo = hlog.values[currentPage][currentOffset].info;
                    currentKey = hlog.values[currentPage][currentOffset].key;
                    currentValue = hlog.values[currentPage][currentOffset].value;

                    // Success; defer epoch?.Suspend(); to EndGetNext
                    return true;
                }

                currentFrame = currentPage % frameSize;

                if (frame.GetInfo(currentFrame, currentOffset).SkipOnScan)
                {
                    epoch?.Suspend();
                    continue;
                }

                // Copy the object values from the frame to data members.
                recordInfo = frame.GetInfo(currentFrame, currentOffset);
                currentKey = frame.GetKey(currentFrame, currentOffset);
                currentValue = frame.GetValue(currentFrame, currentOffset);
                currentPage = currentOffset = -1; // We should no longer use these except for GetLockableInfo()
                
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
        /// Get next record using iterator
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
        {
            if (GetNext(out recordInfo))
            {
                key = currentKey;
                value = currentValue;
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
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, ref frame.GetPage(result.page % frame.frameSize));
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
                result.handle?.Signal();

            Interlocked.MemoryBarrier();
        }
    }
}

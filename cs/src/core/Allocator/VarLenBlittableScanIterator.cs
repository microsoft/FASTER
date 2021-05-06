// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class VariableLengthBlittableScanIterator<Key, Value> : ScanIteratorBase, IFasterScanIterator<Key, Value>
    {
        private readonly VariableLengthBlittableAllocator<Key, Value> hlog;
        private readonly BlittableFrame frame;

        private SectorAlignedMemory memory;
        private readonly bool forceInMemory;

        private long currentPhysicalAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch"></param>
        /// <param name="forceInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        public VariableLengthBlittableScanIterator(VariableLengthBlittableAllocator<Key, Value> hlog, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, LightEpoch epoch, bool forceInMemory = false)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, epoch, hlog.LogPageSizeBits)
        {
            this.hlog = hlog;
            this.forceInMemory = forceInMemory;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Gets reference to current key
        /// </summary>
        /// <returns></returns>
        public ref Key GetKey()
        {
            return ref hlog.GetKey(currentPhysicalAddress);
        }

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        /// <returns></returns>
        public ref Value GetValue()
        {
            return ref hlog.GetValue(currentPhysicalAddress);
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <returns></returns>
        public unsafe bool GetNext(out RecordInfo recordInfo)
        {
            recordInfo = default;

            while (true)
            {
                currentAddress = nextAddress;

                // Check for boundary conditions
                if (currentAddress >= endAddress)
                {
                    return false;
                }

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
                    physicalAddress = hlog.GetPhysicalAddress(currentAddress);
                else
                    physicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);

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
                if (info.Invalid || info.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }
                
                currentPhysicalAddress = physicalAddress;
                recordInfo = info;
                if (currentAddress >= headAddress || forceInMemory)
                {
                    memory?.Return();
                    memory = hlog.bufferPool.Get(recordSize);
                    Buffer.MemoryCopy((byte*)currentPhysicalAddress, memory.aligned_pointer, recordSize, recordSize);
                    currentPhysicalAddress = (long)memory.aligned_pointer;
                }
                epoch?.Suspend();
                return true;
            }
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
        {
            throw new NotSupportedException("Use GetNext(out RecordInfo) to retrieve references to key/value");
        }

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
                Trace.TraceError("AsyncReadPagesCallback error: {0}", errorCode);
                result.cts?.Cancel();
            }

            if (result.freeBuffer1 != null)
            {
                hlog.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
            {
                result.handle?.Signal();
            }

            Interlocked.MemoryBarrier();
        }
    }
}
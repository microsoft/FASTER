// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public class FasterLogScanIterator : IDisposable
    {
        private readonly int frameSize;
        private readonly string name;
        private readonly FasterLog fasterLog;
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly long endAddress;
        private readonly BlittableFrame frame;
        private readonly CountdownEvent[] loaded;
        private readonly CancellationTokenSource[] loadedCancel;
        private readonly long[] loadedPage;
        private readonly LightEpoch epoch;
        private readonly GetMemory getMemory;
        private readonly int headerSize;
        private bool disposed = false;
        private long currentAddress, nextAddress;

        /// <summary>
        /// Current address
        /// </summary>
        public long CurrentAddress => currentAddress;

        /// <summary>
        /// Next address
        /// </summary>
        public long NextAddress => nextAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fasterLog"></param>
        /// <param name="hlog"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="epoch"></param>
        /// <param name="headerSize"></param>
        /// <param name="name"></param>
        /// <param name="getMemory"></param>
        internal unsafe FasterLogScanIterator(FasterLog fasterLog, BlittableAllocator<Empty, byte> hlog, long beginAddress, long endAddress, GetMemory getMemory, ScanBufferingMode scanBufferingMode, LightEpoch epoch, int headerSize, string name)
        {
            this.fasterLog = fasterLog;
            this.allocator = hlog;
            this.getMemory = getMemory;
            this.epoch = epoch;
            this.headerSize = headerSize;

            if (beginAddress == 0)
                beginAddress = hlog.GetFirstValidLogicalAddress(0);

            this.name = name;
            this.endAddress = endAddress;
            currentAddress = beginAddress;
            nextAddress = beginAddress;

            if (scanBufferingMode == ScanBufferingMode.SinglePageBuffering)
                frameSize = 1;
            else if (scanBufferingMode == ScanBufferingMode.DoublePageBuffering)
                frameSize = 2;
            else if (scanBufferingMode == ScanBufferingMode.NoBuffering)
            {
                frameSize = 0;
                return;
            }

            frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
            loaded = new CountdownEvent[frameSize];
            loadedCancel = new CancellationTokenSource[frameSize];
            loadedPage = new long[frameSize];
            for (int i = 0; i < frameSize; i++)
            {
                loadedPage[i] = -1;
                loadedCancel[i] = new CancellationTokenSource();
            }
        }

#if DOTNETCORE
        /// <summary>
        /// Async enumerable for iterator
        /// </summary>
        /// <returns>Entry and entry length</returns>
        public async IAsyncEnumerable<(byte[], int)> GetAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            while (!disposed)
            {
                byte[] result;
                int length;
                while (!GetNext(out result, out length))
                {
                    if (currentAddress >= endAddress)
                        yield break;
                    if (!await WaitAsync(token))
                        yield break;
                }
                yield return (result, length);
            }
        }

        /// <summary>
        /// Async enumerable for iterator (memory pool based version)
        /// </summary>
        /// <returns>Entry and entry length</returns>
        public async IAsyncEnumerable<(IMemoryOwner<byte>, int)> GetAsyncEnumerable(MemoryPool<byte> pool, [EnumeratorCancellation] CancellationToken token = default)
        {
            while (!disposed)
            {
                IMemoryOwner<byte> result;
                int length;
                while (!GetNext(pool, out result, out length))
                {
                    if (currentAddress >= endAddress)
                        yield break;
                    if (!await WaitAsync(token))
                        yield break;
                }
                yield return (result, length);
            }
        }
#endif

        /// <summary>
        /// Wait for iteration to be ready to continue
        /// </summary>
        /// <returns>true if there's more data available to be read; false if there will never be more data (log has been shutdown / iterator has reached endAddress)</returns>
        public ValueTask<bool> WaitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // if (nextAddress >= this.endAddress)
            //    return new ValueTask<bool>(false);

            if (nextAddress < fasterLog.CommittedUntilAddress)
                return new ValueTask<bool>(true);

            return SlowWaitAsync(this, token);

            // use static local function to guarantee there's no accidental closure getting allocated here
            static async ValueTask<bool> SlowWaitAsync(FasterLogScanIterator @this, CancellationToken token)
            {
                while (true)
                {
                    if (@this.disposed)
                        return false;
                    var commitTask = @this.fasterLog.CommitTask;
                    if (@this.nextAddress < @this.fasterLog.CommittedUntilAddress)
                        return true;
                    // Ignore commit exceptions, except when the token is signaled
                    try
                    {
                        await commitTask.WithCancellationAsync(token);
                    }
                    catch (ObjectDisposedException) { return false; }
                    catch when (!token.IsCancellationRequested) { }
                }
            }
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(out byte[] entry, out int entryLength)
        {
            if (GetNextInternal(out long physicalAddress, out entryLength, out bool epochTaken))
            {
                if (getMemory != null)
                {
                    // Use user delegate to allocate memory
                    entry = getMemory(entryLength);
                    if (entry.Length < entryLength)
                        throw new Exception("Byte array provided has invalid length");
                }
                else
                {
                    // We allocate a byte array from heap
                    entry = new byte[entryLength];
                }

                fixed (byte* bp = entry)
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);

                if (epochTaken)
                    epoch.Suspend();

                return true;
            }
            entry = default;
            return false;
        }

        /// <summary>
        /// GetNext supporting memory pools
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="entry"></param>
        /// <param name="entryLength"></param>
        /// <returns></returns>
        public unsafe bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength)
        {
            if (GetNextInternal(out long physicalAddress, out entryLength, out bool epochTaken))
            {
                entry = pool.Rent(entryLength);

                fixed (byte* bp = &entry.Memory.Span.GetPinnableReference())
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);

                if (epochTaken)
                    epoch.Suspend();

                return true;
            }
            entry = default;
            entryLength = default;
            return false;
        }

        /// <summary>
        /// Dispose the iterator
        /// </summary>
        public void Dispose()
        {
            if (!disposed)
            {
                if (frame != null)
                {
                    // Wait for ongoing reads to complete/fail
                    for (int i = 0; i < loaded.Length; i++)
                    {
                        if (loadedPage[i] != -1)
                        {
                            try
                            {
                                loaded[i].Wait(loadedCancel[i].Token);
                            }
                            catch { }
                        }
                    }
                }

                // Dispose/unpin the frame from memory
                frame?.Dispose();

                if (name != null)
                    fasterLog.PersistedIterators.TryRemove(name, out _);

                disposed = true;
            }
            if (Interlocked.Decrement(ref fasterLog.logRefCount) == 0)
                fasterLog.TrueDispose();
        }

        private unsafe void BufferAndLoad(long currentAddress, long currentPage, long currentFrame)
        {
            if (loadedPage[currentFrame] != currentPage)
            {
                if (loadedPage[currentFrame] != -1)
                {
                    WaitForFrameLoad(currentFrame);
                }

                allocator.AsyncReadPagesFromDeviceToFrame(currentAddress >> allocator.LogPageSizeBits, 1, endAddress, AsyncReadPagesCallback, Empty.Default, frame, out loaded[currentFrame], 0, null, null, loadedCancel[currentFrame]);
                loadedPage[currentFrame] = currentAddress >> allocator.LogPageSizeBits;
            }

            if (frameSize == 2)
            {
                var nextPage = currentPage + 1;
                var nextFrame = (currentFrame + 1) % frameSize;

                if (loadedPage[nextFrame] != nextPage)
                {
                    if (loadedPage[nextFrame] != -1)
                    {
                        WaitForFrameLoad(nextFrame);
                    }

                    allocator.AsyncReadPagesFromDeviceToFrame(1 + (currentAddress >> allocator.LogPageSizeBits), 1, endAddress, AsyncReadPagesCallback, Empty.Default, frame, out loaded[nextFrame], 0, null, null, loadedCancel[nextFrame]);
                    loadedPage[nextFrame] = 1 + (currentAddress >> allocator.LogPageSizeBits);
                }
            }

            WaitForFrameLoad(currentFrame);
        }

        private void WaitForFrameLoad(long frame)
        {
            if (loaded[frame].IsSet) return;

            try
            {
                loaded[frame].Wait(loadedCancel[frame].Token); // Ensure we have completed ongoing load
            }
            catch (Exception e)
            {
                loadedPage[frame] = -1;
                loadedCancel[frame] = new CancellationTokenSource();
                nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                throw new Exception("Page read from storage failed, skipping page. Inner exception: " + e.ToString());
            }
        }

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            try
            {
                var result = (PageAsyncReadResult<Empty>)Overlapped.Unpack(overlap).AsyncResult;

                if (errorCode != 0)
                {
                    Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                    result.cts?.Cancel();
                }

                if (result.freeBuffer1 != null)
                {
                    if (errorCode == 0)
                        allocator.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                    result.freeBuffer1.Return();
                    result.freeBuffer1 = null;
                }

                if (errorCode == 0)
                    result.handle?.Signal();

                Interlocked.MemoryBarrier();
            }
            catch when (disposed) { }
            finally
            {
                Overlapped.Free(overlap);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Align(int length)
        {
            return (length + 3) & ~3;
        }

        /// <summary>
        /// Retrieve physical address of next iterator value
        /// (under epoch protection if it is from main page buffer)
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <param name="entryLength"></param>
        /// <param name="epochTaken"></param>
        /// <returns></returns>
        private unsafe bool GetNextInternal(out long physicalAddress, out int entryLength, out bool epochTaken)
        {
            while (true)
            {
                if (disposed)
                    return false;

                physicalAddress = 0;
                entryLength = 0;
                epochTaken = false;

                var _currentAddress = nextAddress;
                long _nextAddress;

                // Check for boundary conditions
                if (_currentAddress < allocator.BeginAddress)
                {
                    Debug.WriteLine("Iterator address is less than log BeginAddress " + allocator.BeginAddress + ", adjusting iterator address");
                    _currentAddress = allocator.BeginAddress;
                }

                if ((_currentAddress >= endAddress) || (_currentAddress >= fasterLog.CommittedUntilAddress))
                {
                    return false;
                }

                if (frameSize == 0 && _currentAddress < allocator.HeadAddress)
                {
                    throw new Exception("Iterator address is less than log HeadAddress in memory-scan mode");
                }


                var headAddress = allocator.HeadAddress;

                if (_currentAddress < headAddress)
                {
                    var currentPage = _currentAddress >> allocator.LogPageSizeBits;
                    var offset = _currentAddress & allocator.PageSizeMask;
                    BufferAndLoad(_currentAddress, currentPage, currentPage % frameSize);
                    physicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
                }
                else
                {
                    epoch.Resume();
                    headAddress = allocator.HeadAddress;
                    if (_currentAddress < headAddress) // rare case
                    {
                        epoch.Suspend();
                        continue;
                    }

                    physicalAddress = allocator.GetPhysicalAddress(_currentAddress);
                }

                // Get and check entry length
                entryLength = fasterLog.GetLength((byte*)physicalAddress);
                if (entryLength == 0)
                {
                    if (_currentAddress >= headAddress)
                        epoch.Suspend();

                    // We are likely at end of page, skip to next
                    _nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;

                    Utility.MonotonicUpdate(ref nextAddress, _nextAddress, out _);

                    if (0 != fasterLog.GetChecksum((byte*)physicalAddress))
                    {
                        var curPage = _currentAddress >> allocator.LogPageSizeBits;
                        throw new Exception("Invalid checksum found during scan, skipping page " + curPage);
                    }
                    else
                    {
                        continue;
                    }
                }

                int recordSize = headerSize + Align(entryLength);
                if ((_currentAddress & allocator.PageSizeMask) + recordSize > allocator.PageSize)
                {
                    if (_currentAddress >= headAddress)
                        epoch.Suspend();
                    _nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    Utility.MonotonicUpdate(ref nextAddress, _nextAddress, out _);
                    throw new Exception("Invalid length of record found: " + entryLength + ", skipping page");
                }

                // Verify checksum if needed
                if (_currentAddress < headAddress)
                {
                    if (!fasterLog.VerifyChecksum((byte*)physicalAddress, entryLength))
                    {
                        var curPage = _currentAddress >> allocator.LogPageSizeBits;
                        _nextAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                        Utility.MonotonicUpdate(ref nextAddress, _nextAddress, out _);
                        throw new Exception("Invalid checksum found during scan, skipping page " + curPage);
                    }
                }

                
                if ((_currentAddress & allocator.PageSizeMask) + recordSize == allocator.PageSize)
                    _nextAddress = (1 + (_currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                else
                    _nextAddress = currentAddress + recordSize;

                epochTaken = _currentAddress >= headAddress;
                if (Utility.MonotonicUpdate(ref nextAddress, _nextAddress, out _))
                    return true;
            }
        }

    }
}



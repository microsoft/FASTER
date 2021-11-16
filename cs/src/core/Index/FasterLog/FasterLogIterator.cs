// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    public sealed class FasterLogScanIterator : ScanIteratorBase, IDisposable
    {
        private readonly string name;
        private readonly FasterLog fasterLog;
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly BlittableFrame frame;
        private readonly GetMemory getMemory;
        private readonly int headerSize;
        private readonly bool scanUncommitted;
        private bool disposed = false;
        internal long requestedCompletedUntilAddress;

        /// <summary>
        /// Iteration completed until (as part of commit)
        /// </summary>
        public long CompletedUntilAddress;

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
        /// <param name="scanUncommitted"></param>
        internal unsafe FasterLogScanIterator(FasterLog fasterLog, BlittableAllocator<Empty, byte> hlog, long beginAddress, long endAddress, GetMemory getMemory, ScanBufferingMode scanBufferingMode, LightEpoch epoch, int headerSize, string name, bool scanUncommitted = false)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, epoch, hlog.LogPageSizeBits)
        {
            this.fasterLog = fasterLog;
            this.allocator = hlog;
            this.getMemory = getMemory;
            this.headerSize = headerSize;
            this.scanUncommitted = scanUncommitted;

            this.name = name;
            CompletedUntilAddress = beginAddress;

            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Async enumerable for iterator
        /// </summary>
        /// <returns>Entry, actual entry length, logical address of entry, logical address of next entry</returns>
        public async IAsyncEnumerable<(byte[] entry, int entryLength, long currentAddress, long nextAddress)> GetAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            while (!disposed)
            {
                byte[] result;
                int length;
                long currentAddress;
                long nextAddress;
                while (!GetNext(out result, out length, out currentAddress, out nextAddress))
                {
                    if (currentAddress >= endAddress)
                        yield break;
                    if (!await WaitAsync(token).ConfigureAwait(false))
                        yield break;
                }
                yield return (result, length, currentAddress, nextAddress);
            }
        }

        /// <summary>
        /// Async enumerable for iterator (memory pool based version)
        /// </summary>
        /// <returns>Entry, actual entry length, logical address of entry, logical address of next entry</returns>
        public async IAsyncEnumerable<(IMemoryOwner<byte> entry, int entryLength, long currentAddress, long nextAddress)> GetAsyncEnumerable(MemoryPool<byte> pool, [EnumeratorCancellation] CancellationToken token = default)
        {
            while (!disposed)
            {
                IMemoryOwner<byte> result;
                int length;
                long currentAddress;
                long nextAddress;
                while (!GetNext(pool, out result, out length, out currentAddress, out nextAddress))
                {
                    if (currentAddress >= endAddress)
                        yield break;
                    if (!await WaitAsync(token).ConfigureAwait(false))
                        yield break;
                }
                yield return (result, length, currentAddress, nextAddress);
            }
        }

        /// <summary>
        /// Wait for iteration to be ready to continue
        /// </summary>
        /// <returns>true if there's more data available to be read; false if there will never be more data (log has been shutdown / iterator has reached endAddress)</returns>
        public ValueTask<bool> WaitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (!scanUncommitted)
            {
                if (NextAddress >= endAddress)
                    return new ValueTask<bool>(false);
                if (NextAddress < fasterLog.CommittedUntilAddress)
                    return new ValueTask<bool>(true);

                return SlowWaitAsync(this, token);
            }
            else
            {
                if (NextAddress < fasterLog.SafeTailAddress)
                    return new ValueTask<bool>(true);

                return SlowWaitUncommittedAsync(this, token);
            }
        }

        private static async ValueTask<bool> SlowWaitAsync(FasterLogScanIterator @this, CancellationToken token)
        {
            while (true)
            {
                if (@this.disposed)
                    return false;
                var commitTask = @this.fasterLog.CommitTask;
                if (@this.NextAddress < @this.fasterLog.CommittedUntilAddress)
                    return true;
                // Ignore commit exceptions, except when the token is signaled
                try
                {
                    await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (ObjectDisposedException) { return false; }
                catch when (!token.IsCancellationRequested) { }
            }
        }

        private static async ValueTask<bool> SlowWaitUncommittedAsync(FasterLogScanIterator @this, CancellationToken token)
        {
            while (true)
            {
                if (@this.disposed)
                    return false;
                var refreshUncommittedTask = @this.fasterLog.RefreshUncommittedTask;
                if (@this.NextAddress < @this.fasterLog.SafeTailAddress)
                    return true;

                // Ignore refresh-uncommitted exceptions, except when the token is signaled
                try
                {
                    await refreshUncommittedTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (ObjectDisposedException) { return false; }
                catch when (!token.IsCancellationRequested) { }
            }
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(out byte[] entry, out int entryLength, out long currentAddress)
        {
            return GetNext(out entry, out entryLength, out currentAddress, out _);
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <param name="nextAddress">Logical address of next entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(out byte[] entry, out int entryLength, out long currentAddress, out long nextAddress)
        {
            if (disposed)
            {
                entry = default;
                entryLength = default;
                currentAddress = default;
                nextAddress = default;
                return false;
            }
            epoch.Resume();
            // Continue looping until we find a record that is not a commit record
            while (true)
            {
                long physicalAddress;
                bool isCommitRecord;
                try
                {
                    var hasNext = GetNextInternal(out physicalAddress, out entryLength, out currentAddress,
                        out nextAddress,
                        out isCommitRecord);
                    if (!hasNext)
                    {
                        entry = default;
                        epoch.Suspend();
                        return false;
                    }
                }
                catch (Exception)
                {
                    // Throw upwards, but first, suspend the epoch we are in 
                    epoch.Suspend();
                    throw;
                }

                if (isCommitRecord) continue;

                if (getMemory != null)
                {
                    // Use user delegate to allocate memory
                    entry = getMemory(entryLength);
                    if (entry.Length < entryLength)
                    {
                        epoch.Suspend();
                        throw new FasterException("Byte array provided has invalid length");
                    }
                }
                else
                {
                    // We allocate a byte array from heap
                    entry = new byte[entryLength];
                }

                fixed (byte* bp = entry)
                    Buffer.MemoryCopy((void*) (headerSize + physicalAddress), bp, entryLength, entryLength);

                epoch.Suspend();
                return true;
            }
        }

        /// <summary>
        /// GetNext supporting memory pools
        /// </summary>
        /// <param name="pool">Memory pool</param>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength, out long currentAddress)
        {
            return GetNext(pool, out entry, out entryLength, out currentAddress, out _);
        }

        /// <summary>
        /// GetNext supporting memory pools
        /// </summary>
        /// <param name="pool">Memory pool</param>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <param name="nextAddress">Logical address of next entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength, out long currentAddress, out long nextAddress)
        {
            if (disposed)
            {
                entry = default;
                entryLength = default;
                currentAddress = default;
                nextAddress = default;
                return false;
            }

            epoch.Resume();
            // Continue looping until we find a record that is not a commit record
            while (true)
            {
                long physicalAddress;
                bool isCommitRecord;
                try
                {
                    var hasNext = GetNextInternal(out physicalAddress, out entryLength, out currentAddress,
                        out nextAddress,
                        out isCommitRecord);
                    if (!hasNext)
                    {
                        entry = default;
                        entryLength = default;
                        epoch.Suspend();
                        return false;
                    }
                }
                catch (Exception)
                {
                    // Throw upwards, but first, suspend the epoch we are in 
                    epoch.Suspend();
                    throw;
                }

                if (isCommitRecord) continue;

                entry = pool.Rent(entryLength);

                fixed (byte* bp = &entry.Memory.Span.GetPinnableReference())
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);

                epoch.Suspend();
                return true;
            }
        }

        /// <summary>
        /// Mark iterator complete until specified address. Info is not
        /// persisted until a subsequent commit operation on the log.
        /// </summary>
        /// <param name="address"></param>
        public void CompleteUntil(long address)
        {
            Utility.MonotonicUpdate(ref requestedCompletedUntilAddress, address, out _);
        }

        /// <summary>
        /// Mark iterator complete until the end of the record at specified
        /// address. Info is not persisted until a subsequent commit operation
        /// on the log. Note: this is slower than CompleteUntil() because the
        /// record's length needs to be looked up first.
        /// </summary>
        /// <param name="recordStartAddress"></param>
        /// <param name="token"></param>
        public async ValueTask CompleteUntilRecordAtAsync(long recordStartAddress, CancellationToken token = default)
        {
            int len = await fasterLog.ReadRecordLengthAsync(recordStartAddress, token: token);
            CompleteUntil(recordStartAddress + headerSize + len);
        }

        internal void UpdateCompletedUntilAddress(long address)
        {
            Utility.MonotonicUpdate(ref CompletedUntilAddress, address, out _);
        }

        /// <summary>
        /// Dispose the iterator
        /// </summary>
        public override void Dispose()
        {
            if (!disposed)
            {
                base.Dispose();

                // Dispose/unpin the frame from memory
                frame?.Dispose();

                if (name != null)
                    fasterLog.PersistedIterators.TryRemove(name, out _);

                disposed = true;
            }
            if (Interlocked.Decrement(ref fasterLog.logRefCount) == 0)
                fasterLog.TrueDispose();
        }

        internal override void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed, long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => allocator.AsyncReadPagesFromDeviceToFrame(readPageStart, numPages, untilAddress, AsyncReadPagesCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice, cts);

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                var result = (PageAsyncReadResult<Empty>)context;

                if (errorCode != 0)
                {
                    Trace.TraceError("AsyncReadPagesCallback error: {0}", errorCode);
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
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Align(int length)
        {
            return (length + 3) & ~3;
        }

        internal unsafe bool ScanForwardForCommit(ref FasterLogRecoveryInfo info, long commitNum = -1)
        {
            epoch.Resume();
            var foundCommit = false;
            try
            {
                // Continue looping until we find a record that is a commit record
                while (GetNextInternal(out long physicalAddress, out var entryLength, out currentAddress,
                    out nextAddress,
                    out var isCommitRecord))
                {
                    if (!isCommitRecord) continue;

                    foundCommit = true;
                    byte[] entry;
                    // We allocate a byte array from heap
                    entry = new byte[entryLength];
                    fixed (byte* bp = entry)
                        Buffer.MemoryCopy((void*) (headerSize + physicalAddress), bp, entryLength, entryLength);
                    info.Initialize(new BinaryReader(new MemoryStream(entry)));
                    
                    Debug.Assert(info.CommitNum != -1);

                    // If we have already found the commit number we are looking for, can stop early
                    if (info.CommitNum == commitNum) break;
                }
            }
            catch (FasterException)
            {
                // If we are here --- simply stop scanning because we ran into an incomplete entry
            }
            finally
            {
                epoch.Suspend();
            }
            
            if (info.CommitNum == commitNum)
                return true;
            // User wants any commie
            if (commitNum == -1)
                return foundCommit;
            // requested commit not found
            return false;
        }

        /// <summary>
        /// Retrieve physical address of next iterator value
        /// (under epoch protection if it is from main page buffer)
        /// </summary>
        /// <param name="physicalAddress"></param>
        /// <param name="entryLength"></param>
        /// <param name="currentAddress"></param>
        /// <param name="outNextAddress"></param>
        /// <param name="commitRecord"></param>
        /// <returns></returns>
        private unsafe bool GetNextInternal(out long physicalAddress, out int entryLength, out long currentAddress, out long outNextAddress, out bool commitRecord)
        {
            while (true)
            {
                physicalAddress = 0;
                entryLength = 0;
                currentAddress = nextAddress;
                outNextAddress = nextAddress;
                commitRecord = false;

                // Check for boundary conditions
                if (currentAddress < allocator.BeginAddress)
                {
                    currentAddress = allocator.BeginAddress;
                }

                var _currentPage = currentAddress >> allocator.LogPageSizeBits;
                var _currentFrame = _currentPage % frameSize;
                var _currentOffset = currentAddress & allocator.PageSizeMask;
                var _headAddress = allocator.HeadAddress;

                if (disposed)
                    return false;


                if ((currentAddress >= endAddress) || (currentAddress >= (scanUncommitted ? fasterLog.SafeTailAddress : fasterLog.CommittedUntilAddress)))
                {
                    return false;
                }

                if (currentAddress < _headAddress)
                {
                    var _endAddress = endAddress;
                    if (fasterLog.readOnlyMode)
                    {
                        // Support partial page reads of committed data
                        var _flush = fasterLog.CommittedUntilAddress;
                        if (_flush < endAddress)
                            _endAddress = _flush;
                    }

                    if (BufferAndLoad(currentAddress, _currentPage, _currentFrame, _headAddress, _endAddress))
                        continue;
                    physicalAddress = frame.GetPhysicalAddress(_currentFrame, _currentOffset);
                }
                else
                {
                    physicalAddress = allocator.GetPhysicalAddress(currentAddress);
                }

                // Get and check entry length
                entryLength = fasterLog.GetLength((byte*)physicalAddress);
                // We may encounter zeroed out bits at the end of page in a normal log, therefore, we need to check
                // whether that is the case
                if (entryLength == 0)
                {
                    // If zeroed out field is at page start, we encountered an uninitialized page and should signal up
                    var pageOffset = currentAddress & ((1 << allocator.LogPageSizeBits) - 1);
                    if (pageOffset == 0)
                    {
                        var curPage = currentAddress >> allocator.LogPageSizeBits;
                        throw new FasterException("Uninitialized page found during scan at page " + curPage);
                    }

                    // Otherwise, we must assume that zeroed out bits are due to page end and skip forward to the next
                    // page. If that's not the case, next iteration of the loop will either hit EOF exception or a
                    // blank page, and propagate failure upwards appropriately
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _);
                    continue;
                }

                // commit records have negative length fields
                if (entryLength < 0)
                {
                    commitRecord = true;
                    entryLength = -entryLength;
                }

                int recordSize = headerSize + Align(entryLength);
                if (_currentOffset + recordSize > allocator.PageSize)
                {
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                    {
                        throw new FasterException("Invalid length of record found: " + entryLength + " at address " + currentAddress + ", skipping page");
                    }
                    continue;
                }

                // Verify checksum if needed
                if (currentAddress < _headAddress)
                {
                    if (!fasterLog.VerifyChecksum((byte*)physicalAddress, entryLength))
                    {
                        var curPage = currentAddress >> allocator.LogPageSizeBits;
                        currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                        if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        {
                            throw new FasterException("Invalid checksum found during scan, skipping page " + curPage);
                        }
                        continue;
                    }
                }

                if ((currentAddress & allocator.PageSizeMask) + recordSize == allocator.PageSize)
                    currentAddress = (1 + (currentAddress >> allocator.LogPageSizeBits)) << allocator.LogPageSizeBits;
                else
                    currentAddress += recordSize;

                if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out long oldCurrentAddress))
                {
                    outNextAddress = currentAddress;
                    currentAddress = oldCurrentAddress;
                    return true;
                }
            }
        }
    }
}

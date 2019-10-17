// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// FASTER log
    /// </summary>
    public class FasterLog : IDisposable
    {
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly LightEpoch epoch;
        private readonly ILogCommitManager logCommitManager;
        private readonly GetMemory getMemory;
        private readonly int headerSize;
        private readonly LogChecksumType logChecksum;
        private TaskCompletionSource<CommitInfo> commitTcs 
            = new TaskCompletionSource<CommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
        
        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => allocator.BeginAddress;

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocator.GetTailAddress();

        /// <summary>
        /// Log flushed until address
        /// </summary>
        public long FlushedUntilAddress => allocator.FlushedUntilAddress;

        /// <summary>
        /// Log committed until address
        /// </summary>
        public long CommittedUntilAddress;

        /// <summary>
        /// Log committed begin address
        /// </summary>
        public long CommittedBeginAddress;

        /// <summary>
        /// Task notifying commit completions
        /// </summary>
        internal Task<CommitInfo> CommitTask => commitTcs.Task;

        /// <summary>
        /// Create new log instance
        /// </summary>
        /// <param name="logSettings"></param>
        public FasterLog(FasterLogSettings logSettings)
        {
            logCommitManager = logSettings.LogCommitManager ?? 
                new LocalLogCommitManager(logSettings.LogCommitFile ??
                logSettings.LogDevice.FileName + ".commit");

            // Reserve 8 byte checksum in header if requested
            logChecksum = logSettings.LogChecksum;
            headerSize = logChecksum == LogChecksumType.PerEntry ? 12 : 4;
            getMemory = logSettings.GetMemory;
            epoch = new LightEpoch();
            CommittedUntilAddress = Constants.kFirstValidAddress;
            CommittedBeginAddress = Constants.kFirstValidAddress;

            allocator = new BlittableAllocator<Empty, byte>(
                logSettings.GetLogSettings(), null, 
                null, epoch, CommitCallback);
            allocator.Initialize();
            Restore();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            allocator.Dispose();
            epoch.Dispose();
            commitTcs.TrySetException(new ObjectDisposedException("Log has been disposed"));
        }

        #region Enqueue
        /// <summary>
        /// Enqueue entry to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue(byte[] entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress)) ;
            return logicalAddress;
        }

        /// <summary>
        /// Enqueue entry to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue(ReadOnlySpan<byte> entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress)) ;
            return logicalAddress;
        }

        /// <summary>
        /// Enqueue batch of entries to log (in memory) - no guarantee of flush/commit
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch of entries to be enqueued to log</param>
        /// <returns>Logical address of added entry</returns>
        public long Enqueue(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;
            while (!TryEnqueue(readOnlySpanBatch, out logicalAddress)) ;
            return logicalAddress;
        }
        #endregion

        #region TryEnqueue
        /// <summary>
        /// Try to enqueue entry to log (in memory). If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entry">Entry to be enqueued to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue(byte[] entry, out long logicalAddress)
        {
            logicalAddress = 0;

            epoch.Resume();

            var length = entry.Length;
            logicalAddress = allocator.TryAllocate(headerSize + Align(length));
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            fixed (byte* bp = entry)
                Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), length, length);
            SetHeader(length, (byte*)physicalAddress);
            epoch.Suspend();
            return true;
        }

        /// <summary>
        /// Try to append entry to log. If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entry">Entry to be appended to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryEnqueue(ReadOnlySpan<byte> entry, out long logicalAddress)
        {
            logicalAddress = 0;

            epoch.Resume();

            var length = entry.Length;
            logicalAddress = allocator.TryAllocate(headerSize + Align(length));
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            fixed (byte* bp = &entry.GetPinnableReference())
                Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), length, length);
            SetHeader(length, (byte*)physicalAddress);
            epoch.Suspend();
            return true;
        }

        /// <summary>
        /// Try to enqueue batch of entries as a single atomic unit (to memory). Entire 
        /// batch needs to fit on one log page.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to be appended to log</param>
        /// <param name="logicalAddress">Logical address of first added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public bool TryEnqueue(IReadOnlySpanBatch readOnlySpanBatch, out long logicalAddress)
        {
            return TryAppend(readOnlySpanBatch, out logicalAddress, out _);
        }
        #endregion

        #region EnqueueAsync
        /// <summary>
        /// Enqueue entry to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAsync(byte[] entry)
        {
            long logicalAddress;

            while (true)
            {
                var task = CommitTask;
                if (TryEnqueue(entry, out logicalAddress))
                    break;

                // Wait for *some* commit - failure can be ignored
                try
                {
                    await task;
                }
                catch { }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Enqueue entry to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAsync(ReadOnlyMemory<byte> entry)
        {
            long logicalAddress;

            while (true)
            {
                var task = CommitTask;
                if (TryEnqueue(entry.Span, out logicalAddress))
                    break;

                // Wait for *some* commit - failure can be ignored
                try
                {
                    await task;
                }
                catch { }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Enqueue batch of entries to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="readOnlySpanBatch"></param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAsync(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;

            while (true)
            {
                var task = CommitTask;
                if (TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;

                // Wait for *some* commit - failure can be ignored
                try
                {
                    await task;
                }
                catch { }
            }

            return logicalAddress;
        }
        #endregion

        #region WaitForCommit and WaitForCommitAsync

        /// <summary>
        /// Spin-wait for enqueues, until tail or specified address, to commit to 
        /// storage. Does NOT itself issue a commit, just waits for commit. So you should 
        /// ensure that someone else causes the commit to happen.
        /// </summary>
        /// <param name="untilAddress">Address until which we should wait for commit, default 0 for tail of log</param>
        /// <returns></returns>
        public void WaitForCommit(long untilAddress = 0)
        {
            var tailAddress = untilAddress;
            if (tailAddress == 0) tailAddress = allocator.GetTailAddress();

            while (CommittedUntilAddress < tailAddress) ;
        }

        /// <summary>
        /// Wait for appends (in memory), until tail or specified address, to commit to 
        /// storage. Does NOT itself issue a commit, just waits for commit. So you should 
        /// ensure that someone else causes the commit to happen.
        /// </summary>
        /// <param name="untilAddress">Address until which we should wait for commit, default 0 for tail of log</param>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(long untilAddress = 0)
        {
            var task = CommitTask;
            var tailAddress = untilAddress;
            if (tailAddress == 0) tailAddress = allocator.GetTailAddress();

            while (true)
            {
                var commitInfo = await task;
                if (commitInfo.untilAddress < tailAddress)
                    task = commitInfo.nextTcs.Task;
                else
                    break;
            }
        }
        #endregion

        #region Commit

        /// <summary>
        /// Issue commit request for log (until tail)
        /// </summary>
        /// <param name="spinWait">If true, spin-wait until commit completes. Otherwise, issue commit and return immediately.</param>
        /// <returns></returns>
        public void Commit(bool spinWait = false)
        {
            CommitInternal(spinWait);
        }

        /// <summary>
        /// Async commit log (until tail), completes only when we 
        /// complete the commit. Throws exception if this or any 
        /// ongoing commit fails.
        /// </summary>
        /// <returns></returns>
        public async ValueTask CommitAsync()
        {
            var task = CommitTask;
            var tailAddress = CommitInternal();

            while (true)
            {
                var commitInfo = await task;
                if (commitInfo.untilAddress < tailAddress)
                    task = commitInfo.nextTcs.Task;
                else
                    break;
            }
        }

        /// <summary>
        /// Async commit log (until tail), completes only when we 
        /// complete the commit. Throws exception if any commit
        /// from prevCommitTask to current fails.
        /// </summary>
        /// <returns></returns>
        public async ValueTask<Task<CommitInfo>> CommitAsync(Task<CommitInfo> prevCommitTask)
        {
            if (prevCommitTask == null) prevCommitTask = commitTcs.Task;
            var tailAddress = CommitInternal();

            while (true)
            {
                var commitInfo = await prevCommitTask;
                if (commitInfo.untilAddress < tailAddress)
                    prevCommitTask = commitInfo.nextTcs.Task;
                else
                    return commitInfo.nextTcs.Task;
            }
        }

        #endregion

        #region EnqueueAndWaitForCommit

        /// <summary>
        /// Append entry to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public long EnqueueAndWaitForCommit(byte[] entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress)) ;
            while (CommittedUntilAddress < logicalAddress + 1) ;
            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public long EnqueueAndWaitForCommit(ReadOnlySpan<byte> entry)
        {
            long logicalAddress;
            while (!TryEnqueue(entry, out logicalAddress)) ;
            while (CommittedUntilAddress < logicalAddress + 1) ;
            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log - spin-waits until entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="readOnlySpanBatch"></param>
        /// <returns></returns>
        public long EnqueueAndWaitForCommit(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;
            while (!TryEnqueue(readOnlySpanBatch, out logicalAddress)) ;
            while (CommittedUntilAddress < logicalAddress + 1) ;
            return logicalAddress;
        }

        #endregion

        #region EnqueueAndWaitForCommitAsync

        /// <summary>
        /// Append entry to log (async) - completes after entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(byte[] entry)
        {
            long logicalAddress;
            Task<CommitInfo> task;

            // Phase 1: wait for commit to memory
            while (true)
            {
                task = CommitTask;
                if (TryEnqueue(entry, out logicalAddress))
                    break;
                await task;
            }

            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                var commitInfo = await task;
                if (commitInfo.untilAddress < logicalAddress + 1)
                    task = commitInfo.nextTcs.Task;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log (async) - completes after entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(ReadOnlyMemory<byte> entry)
        {
            long logicalAddress;
            Task<CommitInfo> task;

            // Phase 1: wait for commit to memory
            while (true)
            {
                task = CommitTask;
                if (TryEnqueue(entry.Span, out logicalAddress))
                    break;
                await task;
            }

            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                var commitInfo = await task;
                if (commitInfo.untilAddress < logicalAddress + 1)
                    task = commitInfo.nextTcs.Task;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log (async) - completes after batch is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="readOnlySpanBatch"></param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;
            Task<CommitInfo> task;

            // Phase 1: wait for commit to memory
            while (true)
            {
                task = CommitTask;
                if (TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;
                await task;
            }

            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                var commitInfo = await task;
                if (commitInfo.untilAddress < logicalAddress + 1)
                    task = commitInfo.nextTcs.Task;
                else
                    break;
            }

            return logicalAddress;
        }
        #endregion

        /// <summary>
        /// Truncate the log until, but not including, untilAddress
        /// </summary>
        /// <param name="untilAddress"></param>
        public void TruncateUntil(long untilAddress)
        {
            allocator.ShiftBeginAddress(untilAddress);
        }

        /// <summary>
        /// Pull-based iterator interface for scanning FASTER log
        /// </summary>
        /// <param name="beginAddress">Begin address for scan</param>
        /// <param name="endAddress">End address for scan (or long.MaxValue for tailing)</param>
        /// <param name="scanBufferingMode">Use single or double buffering</param>
        /// <returns></returns>
        public FasterLogScanIterator Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
        {
            return new FasterLogScanIterator(this, allocator, beginAddress, endAddress, getMemory, scanBufferingMode, epoch, headerSize);
        }

        /// <summary>
        /// Random read record from log, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="estimatedLength">Estimated length of entry, if known</param>
        /// <returns></returns>
        public async ValueTask<(byte[], int)> ReadAsync(long address, int estimatedLength = 0)
        {
            epoch.Resume();
            if (address >= CommittedUntilAddress || address < BeginAddress)
            {
                epoch.Suspend();
                return default;
            }
            var ctx = new SimpleReadContext
            {
                logicalAddress = address,
                completedRead = new SemaphoreSlim(0)
            };
            unsafe
            {
                allocator.AsyncReadRecordToMemory(address, headerSize + estimatedLength, AsyncGetFromDiskCallback, ref ctx);
            }
            epoch.Suspend();
            await ctx.completedRead.WaitAsync();
            return GetRecordAndFree(ctx.record);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Align(int length)
        {
            return (length + 3) & ~3;
        }

        /// <summary>
        /// Commit log
        /// </summary>
        private void CommitCallback(long oldFlushAddress, long flushAddress, uint errorCode)
        {
            long beginAddress = allocator.BeginAddress;
            long oldCommittedUntilAddress = 0;
            TaskCompletionSource<CommitInfo> _commitTcs = default;

            // We can only allow serial monotonic synchronous commit
            lock (this)
            {
                if ((beginAddress > CommittedBeginAddress) || (flushAddress > CommittedUntilAddress))
                {
                    flushAddress = flushAddress > CommittedUntilAddress ? flushAddress : CommittedUntilAddress;
                    FasterLogRecoveryInfo info = new FasterLogRecoveryInfo
                    {
                        BeginAddress = beginAddress > CommittedBeginAddress ? beginAddress : CommittedBeginAddress,
                        FlushedUntilAddress = flushAddress
                    };

                    logCommitManager.Commit(info.BeginAddress, info.FlushedUntilAddress, info.ToByteArray());
                    CommittedBeginAddress = info.BeginAddress;
                    oldCommittedUntilAddress = CommittedUntilAddress;
                    CommittedUntilAddress = info.FlushedUntilAddress;

                    _commitTcs = commitTcs;
                    if (commitTcs.Task.Status != TaskStatus.Faulted)
                    {
                        commitTcs = new TaskCompletionSource<CommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }
                }
            }
            if (errorCode == 0)
                _commitTcs?.TrySetResult(new CommitInfo { fromAddress = oldCommittedUntilAddress, untilAddress = flushAddress, errorCode = errorCode, nextTcs = commitTcs });
            else
                _commitTcs.TrySetException(new CommitFailureException(commitTcs.Task, $"Commit of address range [{oldCommittedUntilAddress}-{flushAddress}] failed with error code {errorCode}"));
        }

        /// <summary>
        /// Restore log
        /// </summary>
        private void Restore()
        {
            FasterLogRecoveryInfo info = new FasterLogRecoveryInfo();
            var commitInfo = logCommitManager.GetCommitMetadata();

            if (commitInfo == null) return;

            using (var r = new BinaryReader(new MemoryStream(commitInfo)))
            {
                info.Initialize(r);
            }

            var headAddress = info.FlushedUntilAddress - allocator.GetOffsetInPage(info.FlushedUntilAddress);
            if (headAddress == 0) headAddress = Constants.kFirstValidAddress;

            allocator.RestoreHybridLog(info.FlushedUntilAddress, headAddress, info.BeginAddress);
            CommittedUntilAddress = info.FlushedUntilAddress;
            CommittedBeginAddress = info.BeginAddress;
        }

        /// <summary>
        /// Try to append batch of entries as a single atomic unit. Entire batch
        /// needs to fit on one page.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to be appended to log</param>
        /// <param name="logicalAddress">Logical address of first added entry</param>
        /// <param name="allocatedLength">Actual allocated length</param>
        /// <returns>Whether the append succeeded</returns>
        private unsafe bool TryAppend(IReadOnlySpanBatch readOnlySpanBatch, out long logicalAddress, out int allocatedLength)
        {
            logicalAddress = 0;

            int totalEntries = readOnlySpanBatch.TotalEntries();
            allocatedLength = 0;
            for (int i = 0; i < totalEntries; i++)
            {
                allocatedLength += Align(readOnlySpanBatch.Get(i).Length) + headerSize;
            }

            epoch.Resume();

            logicalAddress = allocator.TryAllocate(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            for (int i = 0; i < totalEntries; i++)
            {
                var span = readOnlySpanBatch.Get(i);
                var entryLength = span.Length;
                fixed (byte* bp = &span.GetPinnableReference())
                    Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), entryLength, entryLength);
                SetHeader(entryLength, (byte*)physicalAddress);
                physicalAddress += Align(entryLength) + headerSize;
            }

            epoch.Suspend();
            return true;
        }

        private unsafe void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            var ctx = (SimpleReadContext)Overlapped.Unpack(overlap).AsyncResult;

            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                ctx.record.Return();
                ctx.record = null;
                ctx.completedRead.Release();
            }
            else
            {
                var record = ctx.record.GetValidPointer();
                var length = GetLength(record);

                if (length < 0 || length > allocator.PageSize)
                {
                    Debug.WriteLine("Invalid record length found: " + length);
                    ctx.record.Return();
                    ctx.record = null;
                    ctx.completedRead.Release();
                }
                else
                {
                    int requiredBytes = headerSize + length;
                    if (ctx.record.available_bytes >= requiredBytes)
                    {
                        ctx.completedRead.Release();
                    }
                    else
                    {
                        ctx.record.Return();
                        allocator.AsyncReadRecordToMemory(ctx.logicalAddress, requiredBytes, AsyncGetFromDiskCallback, ref ctx);
                    }
                }
            }
            Overlapped.Free(overlap);
        }

        private (byte[], int) GetRecordAndFree(SectorAlignedMemory record)
        {
            if (record == null)
                return (null, 0);

            byte[] result;
            int length;
            unsafe
            {
                var ptr = record.GetValidPointer();
                length = GetLength(ptr);
                if (!VerifyChecksum(ptr, length))
                {
                    throw new Exception("Checksum failed for read");
                }
                result = getMemory != null ? getMemory(length) : new byte[length];
                fixed (byte* bp = result)
                {
                    Buffer.MemoryCopy(ptr + headerSize, bp, length, length);
                }
            }
            record.Return();
            return (result, length);
        }

        private long CommitInternal(bool spinWait = false)
        {
            epoch.Resume();
            if (allocator.ShiftReadOnlyToTail(out long tailAddress))
            {
                if (spinWait)
                {
                    while (CommittedUntilAddress < tailAddress)
                    {
                        epoch.ProtectAndDrain();
                        Thread.Yield();
                    }
                }
                epoch.Suspend();
            }
            else
            {
                // May need to commit begin address
                epoch.Suspend();
                CommitCallback(CommittedUntilAddress, CommittedUntilAddress, 0);
            }

            return tailAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe int GetLength(byte* ptr)
        {
            if (logChecksum == LogChecksumType.None)
                return *(int*)ptr;
            else if (logChecksum == LogChecksumType.PerEntry)
                return *(int*)(ptr + 8);
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool VerifyChecksum(byte* ptr, int length)
        {
            if (logChecksum == LogChecksumType.PerEntry)
            {
                var cs = Utility.XorBytes(ptr + 8, length + 4);
                if (cs != *(ulong*)ptr)
                {
                    return false;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ulong GetChecksum(byte* ptr)
        {
            if (logChecksum == LogChecksumType.PerEntry)
            {
                return *(ulong*)ptr;
            }
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void SetHeader(int length, byte* dest)
        {
            if (logChecksum == LogChecksumType.None)
            {
                *(int*)dest = length;
                return;
            }
            else if (logChecksum == LogChecksumType.PerEntry)
            {
                *(int*)(dest + 8) = length;
                *(ulong*)dest = Utility.XorBytes(dest + 8, length + 4);
            }
        }
    }
}

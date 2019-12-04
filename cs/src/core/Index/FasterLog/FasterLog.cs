// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private readonly Dictionary<string, long> RecoveredIterators;
        private TaskCompletionSource<LinkedCommitInfo> commitTcs
            = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);

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
        internal Task<LinkedCommitInfo> CommitTask => commitTcs.Task;

        /// <summary>
        /// Table of persisted iterators
        /// </summary>
        internal readonly ConcurrentDictionary<string, FasterLogScanIterator> PersistedIterators 
            = new ConcurrentDictionary<string, FasterLogScanIterator>();

        /// <summary>
        /// Numer of references to log, including itself
        /// Used to determine disposability of log
        /// </summary>
        internal int logRefCount = 1;

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
            Restore(out RecoveredIterators);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Decrement(ref logRefCount) == 0)
                TrueDispose();
        }

        internal void TrueDispose()
        {
            commitTcs.TrySetException(new ObjectDisposedException("Log has been disposed"));
            allocator.Dispose();
            epoch.Dispose();
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
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public ValueTask<long> EnqueueAsync(byte[] entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(entry, out long logicalAddress))
                return new ValueTask<long>(logicalAddress);

            return SlowEnqueueAsync(this, entry, token);
            static async ValueTask<long> SlowEnqueueAsync(FasterLog @this, byte[] entry, CancellationToken token)
            {
                long logicalAddress;
                while (true)
                {
                    var task = @this.CommitTask;
                    if (@this.TryEnqueue(entry, out logicalAddress))
                        break;
                    if (@this.NeedToWait(@this.CommittedUntilAddress, @this.TailAddress))
                    {
                        // Wait for *some* commit - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                        try
                        {
                            await task.WithCancellationAsync(token);
                        }
                        catch when (!token.IsCancellationRequested) { }
                    }
                }

                return logicalAddress;
            }
        }

        /// <summary>
        /// Enqueue entry to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public ValueTask<long> EnqueueAsync(ReadOnlyMemory<byte> entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(entry.Span, out long logicalAddress))
                return new ValueTask<long>(logicalAddress);

            return SlowEnqueueAsync(this, entry, token);
            static async ValueTask<long> SlowEnqueueAsync(FasterLog @this, ReadOnlyMemory<byte> entry, CancellationToken token)
            {
                long logicalAddress;
                while (true)
                {
                    var task = @this.CommitTask;
                    if (@this.TryEnqueue(entry.Span, out logicalAddress))
                        break;
                    if (@this.NeedToWait(@this.CommittedUntilAddress, @this.TailAddress))
                    {
                        // Wait for *some* commit - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                        try
                        {
                            await task.WithCancellationAsync(token);
                        }
                        catch when (!token.IsCancellationRequested) { }
                    }
                }

                return logicalAddress;
            }
        }

        /// <summary>
        /// Enqueue batch of entries to log in memory (async) - completes after entry is 
        /// appended to memory, NOT committed to storage.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public ValueTask<long> EnqueueAsync(IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (TryEnqueue(readOnlySpanBatch, out long address))
                return new ValueTask<long>(address);

            return SlowEnqueueAsync(this, readOnlySpanBatch, token);
            static async ValueTask<long> SlowEnqueueAsync(FasterLog @this, IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token)
            {
                long logicalAddress;
                while (true)
                {
                    var task = @this.CommitTask;
                    if (@this.TryEnqueue(readOnlySpanBatch, out logicalAddress))
                        break;
                    if (@this.NeedToWait(@this.CommittedUntilAddress, @this.TailAddress))
                    {
                        // Wait for *some* commit - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                        try
                        {
                            await task.WithCancellationAsync(token);
                        }
                        catch when (!token.IsCancellationRequested) { }
                    }
                }

                return logicalAddress;
            }
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
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(long untilAddress = 0, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = CommitTask;
            var tailAddress = untilAddress;
            if (tailAddress == 0) tailAddress = allocator.GetTailAddress();

            if (CommittedUntilAddress >= tailAddress)
                return;

            while (true)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress)
                    task = linkedCommitInfo.NextTask;
                else
                    break;
            }
        }
        #endregion

        #region Commit and CommitAsync

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
        public async ValueTask CommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = CommitTask;
            var tailAddress = CommitInternal();

            while (CommittedUntilAddress < tailAddress)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress)
                    task = linkedCommitInfo.NextTask;
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
        public async ValueTask<Task<LinkedCommitInfo>> CommitAsync(Task<LinkedCommitInfo> prevCommitTask, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (prevCommitTask == null) prevCommitTask = CommitTask;
            var tailAddress = CommitInternal();

            while (CommittedUntilAddress < tailAddress)
            {
                var linkedCommitInfo = await prevCommitTask.WithCancellationAsync(token);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress)
                    prevCommitTask = linkedCommitInfo.NextTask;
                else
                    return linkedCommitInfo.NextTask;
            }

            return prevCommitTask;
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
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(byte[] entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            Task<LinkedCommitInfo> task;

            // Phase 1: wait for commit to memory
            while (true)
            {
                task = CommitTask;
                if (TryEnqueue(entry, out logicalAddress))
                    break;
                if (NeedToWait(CommittedUntilAddress, TailAddress))
                {
                    // Wait for *some* commit - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                    try
                    {
                        await task.WithCancellationAsync(token);
                    }
                    catch when (!token.IsCancellationRequested) { }
                }
            }

            // since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await task.WithCancellationAsync(token);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    task = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log (async) - completes after entry is committed to storage.
        /// Does NOT itself issue flush!
        /// </summary>
        /// <param name="entry">Entry to enqueue</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(ReadOnlyMemory<byte> entry, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            Task<LinkedCommitInfo> task;

            // Phase 1: wait for commit to memory
            while (true)
            {
                task = CommitTask;
                if (TryEnqueue(entry.Span, out logicalAddress))
                    break;
                if (NeedToWait(CommittedUntilAddress, TailAddress))
                {
                    // Wait for *some* commit - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                    try
                    {
                        await task.WithCancellationAsync(token);
                    }
                    catch when (!token.IsCancellationRequested) { }
                }
            }

            // since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await task.WithCancellationAsync(token);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    task = linkedCommitInfo.NextTask;
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
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<long> EnqueueAndWaitForCommitAsync(IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            long logicalAddress;
            Task<LinkedCommitInfo> task;

            // Phase 1: wait for commit to memory
            while (true)
            {
                task = CommitTask;
                if (TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;
                if (NeedToWait(CommittedUntilAddress, TailAddress))
                {
                    // Wait for *some* commit - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                    try
                    {
                        await task.WithCancellationAsync(token);
                    }
                    catch when (!token.IsCancellationRequested) { }
                }
            }

            // since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await task.WithCancellationAsync(token);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    task = linkedCommitInfo.NextTask;
                else
                    break;
            }

            return logicalAddress;
        }
        #endregion

        /// <summary>
        /// Truncate the log until, but not including, untilAddress. **User should ensure
        /// that the provided address is a valid starting address for some record.** The
        /// truncation is not persisted until the next commit.
        /// </summary>
        /// <param name="untilAddress">Until address</param>
        public void TruncateUntil(long untilAddress)
        {
            allocator.ShiftBeginAddress(untilAddress);
        }

        /// <summary>
        /// Truncate the log until the start of the page corresponding to untilAddress. This is 
        /// safer than TruncateUntil, as page starts are always a valid truncation point. The
        /// truncation is not persisted until the next commit.
        /// </summary>
        /// <param name="untilAddress">Until address</param>
        public void TruncateUntilPageStart(long untilAddress)
        {
            allocator.ShiftBeginAddress(untilAddress & ~allocator.PageSizeMask);
        }

        /// <summary>
        /// Pull-based iterator interface for scanning FASTER log
        /// </summary>
        /// <param name="beginAddress">Begin address for scan.</param>
        /// <param name="endAddress">End address for scan (or long.MaxValue for tailing).</param>
        /// <param name="name">Name of iterator, if we need to persist/recover it (default null - do not persist).</param>
        /// <param name="recover">Whether to recover named iterator from latest commit (if exists). If false, iterator starts from beginAddress.</param>
        /// <param name="scanBufferingMode">Use single or double buffering</param>
        /// <returns></returns>
        public FasterLogScanIterator Scan(long beginAddress, long endAddress, string name = null, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
        {
            FasterLogScanIterator iter;
            if (recover && name != null && RecoveredIterators != null && RecoveredIterators.ContainsKey(name))
                iter = new FasterLogScanIterator(this, allocator, RecoveredIterators[name], endAddress, getMemory, scanBufferingMode, epoch, headerSize, name);
            else
                iter = new FasterLogScanIterator(this, allocator, beginAddress, endAddress, getMemory, scanBufferingMode, epoch, headerSize, name);

            if (name != null)
            {
                if (name.Length > 20)
                    throw new FasterException("Max length of iterator name is 20 characters");
                if (PersistedIterators.ContainsKey(name))
                    Debug.WriteLine("Iterator name exists, overwriting");
                PersistedIterators[name] = iter;
            }

            if (Interlocked.Increment(ref logRefCount) == 1)
                throw new FasterException("Cannot scan disposed log instance");
            return iter;
        }

        /// <summary>
        /// Random read record from log, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="estimatedLength">Estimated length of entry, if known</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<(byte[], int)> ReadAsync(long address, int estimatedLength = 0, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
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
            await ctx.completedRead.WaitAsync(token);
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
        private void CommitCallback(CommitInfo commitInfo)
        {
            TaskCompletionSource<LinkedCommitInfo> _commitTcs = default;

            // We can only allow serial monotonic synchronous commit
            lock (this)
            {
                if (CommittedBeginAddress > commitInfo.BeginAddress)
                    commitInfo.BeginAddress = CommittedBeginAddress;
                if (CommittedUntilAddress > commitInfo.FromAddress)
                    commitInfo.FromAddress = CommittedUntilAddress;
                if (CommittedUntilAddress > commitInfo.UntilAddress)
                    commitInfo.UntilAddress = CommittedUntilAddress;

                FasterLogRecoveryInfo info = new FasterLogRecoveryInfo
                {
                    BeginAddress = commitInfo.BeginAddress,
                    FlushedUntilAddress = commitInfo.UntilAddress
                };

                // Take snapshot of persisted iterators
                info.SnapshotIterators(PersistedIterators);

                logCommitManager.Commit(info.BeginAddress, info.FlushedUntilAddress, info.ToByteArray());
                CommittedBeginAddress = info.BeginAddress;
                CommittedUntilAddress = info.FlushedUntilAddress;
                
                // Update completed address for persisted iterators
                info.CommitIterators(PersistedIterators);

                _commitTcs = commitTcs;
                // If task is not faulted, create new task
                // If task is faulted due to commit exception, create new task
                if (commitTcs.Task.Status != TaskStatus.Faulted || commitTcs.Task.Exception.InnerException as CommitFailureException != null)
                {
                    commitTcs = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }
            var lci = new LinkedCommitInfo
            {
                CommitInfo = commitInfo,
                NextTask = commitTcs.Task
            };

            if (commitInfo.ErrorCode == 0)
                _commitTcs?.TrySetResult(lci);
            else
                _commitTcs.TrySetException(new CommitFailureException(lci, $"Commit of address range [{commitInfo.FromAddress}-{commitInfo.UntilAddress}] failed with error code {commitInfo.ErrorCode}"));
        }

        /// <summary>
        /// Restore log
        /// </summary>
        private void Restore(out Dictionary<string, long> recoveredIterators)
        {
            recoveredIterators = null;
            FasterLogRecoveryInfo info = new FasterLogRecoveryInfo();
            var commitInfo = logCommitManager.GetCommitMetadata();

            if (commitInfo == null) return;

            using (var r = new BinaryReader(new MemoryStream(commitInfo)))
            {
                info.Initialize(r);
            }

            var headAddress = info.FlushedUntilAddress - allocator.GetOffsetInPage(info.FlushedUntilAddress);
            if (info.BeginAddress > headAddress)
                headAddress = info.BeginAddress;

            if (headAddress == 0) headAddress = Constants.kFirstValidAddress;

            recoveredIterators = info.Iterators;

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
                    throw new FasterException("Checksum failed for read");
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
            if (allocator.ShiftReadOnlyToTail(out long tailAddress, out _))
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
                // May need to commit begin address and/or iterators
                epoch.Suspend();
                var beginAddress = allocator.BeginAddress;
                if (beginAddress > CommittedBeginAddress || PersistedIterators.Count > 0)
                    CommitCallback(new CommitInfo
                    {
                        BeginAddress = beginAddress,
                        FromAddress = CommittedUntilAddress,
                        UntilAddress = CommittedUntilAddress,
                        ErrorCode = 0
                    });
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

        /// <summary>
        /// Do we need to await a commit to make forward progress?
        /// </summary>
        /// <param name="committedUntilAddress"></param>
        /// <param name="tailAddress"></param>
        /// <returns></returns>
        private bool NeedToWait(long committedUntilAddress, long tailAddress)
        {
            Thread.Yield();
            return
                allocator.GetPage(committedUntilAddress) <=
                (allocator.GetPage(tailAddress) - allocator.BufferSize);
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        private readonly bool disposeLogCommitManager;
        private readonly GetMemory getMemory;
        private readonly int headerSize;
        private readonly LogChecksumType logChecksum;
        private readonly WorkQueueLIFO<CommitInfo> commitQueue;

        internal readonly bool readOnlyMode;

        private TaskCompletionSource<LinkedCommitInfo> commitTcs
            = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<Empty> refreshUncommittedTcs
            = new TaskCompletionSource<Empty>(TaskCreationOptions.RunContinuationsAsynchronously);

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
        /// Log safe read-only address
        /// </summary>
        internal long SafeTailAddress;

        /// <summary>
        /// Dictionary of recovered iterators and their committed until addresses
        /// </summary>
        public Dictionary<string, long> RecoveredIterators { get; private set; }

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
        /// Task notifying log flush completions
        /// </summary>
        internal CompletionEvent FlushEvent => allocator.FlushEvent;

        /// <summary>
        /// Task notifying refresh uncommitted
        /// </summary>
        internal Task<Empty> RefreshUncommittedTask => refreshUncommittedTcs.Task;

        /// <summary>
        /// Table of persisted iterators
        /// </summary>
        internal readonly ConcurrentDictionary<string, FasterLogScanIterator> PersistedIterators
            = new ConcurrentDictionary<string, FasterLogScanIterator>();

        /// <summary>
        /// Version number to track changes to commit metadata (begin address and persisted iterators)
        /// </summary>
        private long commitMetadataVersion;

        /// <summary>
        /// Committed view of commitMetadataVersion
        /// </summary>
        private long persistedCommitMetadataVersion;

        internal Dictionary<string, long> LastPersistedIterators;

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
            : this(logSettings, false)
        {
            this.RecoveredIterators = Restore();
        }

        /// <summary>
        /// Create new log instance asynchronously
        /// </summary>
        /// <param name="logSettings"></param>
        /// <param name="cancellationToken"></param>
        public static async ValueTask<FasterLog> CreateAsync(FasterLogSettings logSettings, CancellationToken cancellationToken = default)
        {
            var fasterLog = new FasterLog(logSettings, false);
            fasterLog.RecoveredIterators = await fasterLog.RestoreAsync(cancellationToken).ConfigureAwait(false);
            return fasterLog;
        }

        private FasterLog(FasterLogSettings logSettings, bool oldCommitManager)
        {
            Debug.Assert(oldCommitManager == false);
            logCommitManager = logSettings.LogCommitManager ??
                new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        logSettings.LogCommitDir ??
                        new FileInfo(logSettings.LogDevice.FileName).Directory.FullName));

            if (logSettings.LogCommitManager == null)
                disposeLogCommitManager = true;

            // Reserve 8 byte checksum in header if requested
            logChecksum = logSettings.LogChecksum;
            headerSize = logChecksum == LogChecksumType.PerEntry ? 12 : 4;
            getMemory = logSettings.GetMemory;
            epoch = new LightEpoch();
            CommittedUntilAddress = Constants.kFirstValidAddress;
            CommittedBeginAddress = Constants.kFirstValidAddress;
            SafeTailAddress = Constants.kFirstValidAddress;
            commitQueue = new WorkQueueLIFO<CommitInfo>(ci => SerialCommitCallbackWorker(ci));
            allocator = new BlittableAllocator<Empty, byte>(
                logSettings.GetLogSettings(), null,
                null, epoch, CommitCallback);
            allocator.Initialize();

            // FasterLog is used as a read-only iterator
            if (logSettings.ReadOnlyMode)
            {
                readOnlyMode = true;
                allocator.HeadAddress = long.MaxValue;
            }

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
            if (disposeLogCommitManager)
                logCommitManager.Dispose();
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
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
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
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
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
            while (!TryEnqueue(readOnlySpanBatch, out logicalAddress))
                Thread.Yield();
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
            var length = entry.Length;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
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
            var length = entry.Length;
            int allocatedLength = headerSize + Align(length);
            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();

            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
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
        }

        private static async ValueTask<long> SlowEnqueueAsync(FasterLog @this, byte[] entry, CancellationToken token)
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(entry, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
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
        }

        private static async ValueTask<long> SlowEnqueueAsync(FasterLog @this, ReadOnlyMemory<byte> entry, CancellationToken token)
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(entry.Span, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            return logicalAddress;
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
        }

        private static async ValueTask<long> SlowEnqueueAsync(FasterLog @this, IReadOnlySpanBatch readOnlySpanBatch, CancellationToken token)
        {
            long logicalAddress;
            while (true)
            {
                var flushEvent = @this.FlushEvent;
                if (@this.TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;
                // Wait for *some* flush - failure can be ignored except if the token was signaled (which the caller should handle correctly)
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
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

            while (CommittedUntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion) Thread.Yield();
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

            while (CommittedUntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion)
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
        public async ValueTask CommitAsync(long version = -1, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = CommitTask;
            var tailAddress = CommitInternal(version);

            while (CommittedUntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion)
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
        public async ValueTask<Task<LinkedCommitInfo>> CommitAsync(Task<LinkedCommitInfo> prevCommitTask, long version = -1, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            if (prevCommitTask == null) prevCommitTask = CommitTask;
            var tailAddress = CommitInternal(version);

            while (CommittedUntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion)
            {
                var linkedCommitInfo = await prevCommitTask.WithCancellationAsync(token).ConfigureAwait(false);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress || persistedCommitMetadataVersion < commitMetadataVersion)
                    prevCommitTask = linkedCommitInfo.NextTask;
                else
                    return linkedCommitInfo.NextTask;
            }

            return prevCommitTask;
        }

        /// <summary>
        /// Trigger a refresh of information about uncommitted part of log (tail address) to ensure visibility 
        /// to uncommitted scan iterators. Will cause SafeTailAddress to reflect the current tail address.
        /// </summary>
        public void RefreshUncommitted(bool spinWait = false)
        {
            RefreshUncommittedInternal(spinWait);
        }

        /// <summary>
        /// Trigger a refresh of information about uncommitted part of log (tail address) to ensure visibility 
        /// to uncommitted scan iterators. Will cause SafeTailAddress to reflect the current tail address.
        /// Async method completes only when we complete the refresh.
        /// </summary>
        /// <returns></returns>
        public async ValueTask RefreshUncommittedAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = RefreshUncommittedTask;
            var tailAddress = RefreshUncommittedInternal();

            while (SafeTailAddress < tailAddress)
            {
                await task.WithCancellationAsync(token).ConfigureAwait(false);
                task = RefreshUncommittedTask;
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
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            while (CommittedUntilAddress < logicalAddress + 1) Thread.Yield();
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
            while (!TryEnqueue(entry, out logicalAddress))
                Thread.Yield();
            while (CommittedUntilAddress < logicalAddress + 1) Thread.Yield();
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
            while (!TryEnqueue(readOnlySpanBatch, out logicalAddress))
                Thread.Yield();
            while (CommittedUntilAddress < logicalAddress + 1) Thread.Yield();
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
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(entry, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
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
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(entry.Span, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
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
            CompletionEvent flushEvent;
            Task<LinkedCommitInfo> commitTask;

            // Phase 1: wait for commit to memory
            while (true)
            {
                flushEvent = FlushEvent;
                commitTask = CommitTask;
                if (TryEnqueue(readOnlySpanBatch, out logicalAddress))
                    break;
                try
                {
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                catch when (!token.IsCancellationRequested) { }
            }

            // Phase 2: wait for commit/flush to storage
            // Since the task object was read before enqueueing, there is no need for the CommittedUntilAddress >= logicalAddress check like in WaitForCommit
            while (true)
            {
                LinkedCommitInfo linkedCommitInfo;
                try
                {
                    linkedCommitInfo = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (CommitFailureException e)
                {
                    linkedCommitInfo = e.LinkedCommitInfo;
                    if (logicalAddress >= linkedCommitInfo.CommitInfo.FromAddress && logicalAddress < linkedCommitInfo.CommitInfo.UntilAddress)
                        throw;
                }
                if (linkedCommitInfo.CommitInfo.UntilAddress < logicalAddress + 1)
                    commitTask = linkedCommitInfo.NextTask;
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
        /// <param name="scanUncommitted">Whether we scan uncommitted data</param>
        /// <returns></returns>
        public FasterLogScanIterator Scan(long beginAddress, long endAddress, string name = null, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false)
        {
            if (readOnlyMode)
            {
                scanBufferingMode = ScanBufferingMode.SinglePageBuffering;

                if (name != null)
                    throw new FasterException("Cannot used named iterators with read-only FasterLog");
                if (scanUncommitted)
                    throw new FasterException("Cannot used scanUncommitted with read-only FasterLog");
            }

            FasterLogScanIterator iter;
            if (recover && name != null && RecoveredIterators != null && RecoveredIterators.ContainsKey(name))
                iter = new FasterLogScanIterator(this, allocator, RecoveredIterators[name], endAddress, getMemory, scanBufferingMode, epoch, headerSize, name, scanUncommitted);
            else
                iter = new FasterLogScanIterator(this, allocator, beginAddress, endAddress, getMemory, scanBufferingMode, epoch, headerSize, name, scanUncommitted);

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
            await ctx.completedRead.WaitAsync(token).ConfigureAwait(false);
            return GetRecordAndFree(ctx.record);
        }

        /// <summary>
        /// Random read record from log as IMemoryOwner&lt;byte&gt;, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="memoryPool">MemoryPool to rent the destination buffer from</param>
        /// <param name="estimatedLength">Estimated length of entry, if known</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<(IMemoryOwner<byte>, int)> ReadAsync(long address, MemoryPool<byte> memoryPool, int estimatedLength = 0, CancellationToken token = default)
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
            await ctx.completedRead.WaitAsync(token).ConfigureAwait(false);
            return GetRecordAsMemoryOwnerAndFree(ctx.record, memoryPool);
        }

        /// <summary>
        /// Random read record from log, at given address
        /// </summary>
        /// <param name="address">Logical address to read from</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async ValueTask<int> ReadRecordLengthAsync(long address, CancellationToken token = default)
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
                allocator.AsyncReadRecordToMemory(address, headerSize, AsyncGetHeaderOnlyFromDiskCallback, ref ctx);
            }
            epoch.Suspend();
            await ctx.completedRead.WaitAsync(token).ConfigureAwait(false);
            
            return GetRecordLengthAndFree(ctx.record);
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
            commitQueue.EnqueueAndTryWork(commitInfo, asTask: true);
        }

        private void SerialCommitCallbackWorker(CommitInfo commitInfo)
        {
            // Check if commit is already covered
            if (CommittedBeginAddress >= BeginAddress &&
                CommittedUntilAddress >= commitInfo.UntilAddress &&
                persistedCommitMetadataVersion >= commitMetadataVersion &&
                commitInfo.ErrorCode == 0)
                return;

            if (commitInfo.ErrorCode == 0)
            {
                // Capture CMV first, so metadata prior to CMV update is visible to commit
                long _localCMV = commitMetadataVersion;

                if (CommittedUntilAddress > commitInfo.FromAddress)
                    commitInfo.FromAddress = CommittedUntilAddress;
                if (CommittedUntilAddress > commitInfo.UntilAddress)
                    commitInfo.UntilAddress = CommittedUntilAddress;

                FasterLogRecoveryInfo info = new FasterLogRecoveryInfo
                {
                    BeginAddress = BeginAddress,
                    FlushedUntilAddress = commitInfo.UntilAddress,
                    Cookie = commitInfo.Cookie
                };

                // Take snapshot of persisted iterators
                info.SnapshotIterators(PersistedIterators);

                logCommitManager.Commit(info.BeginAddress, info.FlushedUntilAddress, info.ToByteArray(), commitInfo.Version);

                LastPersistedIterators = info.Iterators;
                CommittedBeginAddress = info.BeginAddress;
                CommittedUntilAddress = info.FlushedUntilAddress;
                if (_localCMV > persistedCommitMetadataVersion)
                    persistedCommitMetadataVersion = _localCMV;

                // Update completed address for persisted iterators
                info.CommitIterators(PersistedIterators);
            }

            var _commitTcs = commitTcs;
            commitTcs = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
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

        private bool IteratorsChanged()
        {
            var _lastPersistedIterators = LastPersistedIterators;
            if (_lastPersistedIterators == null)
            {
                if (PersistedIterators.Count == 0)
                    return false;
                return true;
            }
            if (_lastPersistedIterators.Count != PersistedIterators.Count)
                return true;
            foreach (var item in _lastPersistedIterators)
            {
                if (PersistedIterators.TryGetValue(item.Key, out var other))
                {
                    if (item.Value != other.requestedCompletedUntilAddress) return true;
                }
                else
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Read-only callback
        /// </summary>
        private void UpdateTailCallback(long tailAddress)
        {
            lock (this)
            {
                if (tailAddress > SafeTailAddress)
                {
                    SafeTailAddress = tailAddress;

                    var _refreshUncommittedTcs = refreshUncommittedTcs;
                    refreshUncommittedTcs = new TaskCompletionSource<Empty>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _refreshUncommittedTcs.SetResult(Empty.Default);
                }
            }
        }

        /// <summary>
        /// Synchronously recover instance to FasterLog's latest commit, when being used as a readonly log iterator
        /// </summary>
        public void RecoverReadOnly()
        {
            if (!readOnlyMode)
                throw new FasterException("This method can only be used with a read-only FasterLog instance used for iteration. Set FasterLogSettings.ReadOnlyMode to true during creation to indicate this.");

            this.Restore();
            SignalWaitingROIterators();
        }

        /// <summary>
        /// Asynchronously recover instance to FasterLog's latest commit, when being used as a readonly log iterator
        /// </summary>
        public async ValueTask RecoverReadOnlyAsync(CancellationToken cancellationToken = default)
        {
            if (!readOnlyMode)
                throw new FasterException("This method can only be used with a read-only FasterLog instance used for iteration. Set FasterLogSettings.ReadOnlyMode to true during creation to indicate this.");

            await this.RestoreAsync(cancellationToken).ConfigureAwait(false);
            SignalWaitingROIterators();
        }

        private void SignalWaitingROIterators()
        {
            // One RecoverReadOnly use case is to allow a FasterLogIterator to continuously read a mirror FasterLog (over the same log storage) of a primary FasterLog.
            // In this scenario, when the iterator arrives at the tail after a previous call to RestoreReadOnly, it will wait asynchronously until more data
            // is committed and read by a subsequent call to RecoverReadOnly. Here, we signal iterators that we have completed recovery.
            var _commitTcs = commitTcs;
            if (commitTcs.Task.Status != TaskStatus.Faulted || commitTcs.Task.Exception.InnerException as CommitFailureException != null)
            {
                commitTcs = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            // Update commit to release pending iterators.
            var lci = new LinkedCommitInfo
            {
                CommitInfo = new CommitInfo { Version = -1, FromAddress = BeginAddress, UntilAddress = FlushedUntilAddress },
                NextTask = commitTcs.Task
            };
            _commitTcs?.TrySetResult(lci);
        }

        /// <summary>
        /// Restore log synchronously
        /// </summary>
        private Dictionary<string, long> Restore()
        {
            foreach (var commitNum in logCommitManager.ListCommits())
            {
                try
                {
                    if (!PrepareToRestoreFromCommit(commitNum, out FasterLogRecoveryInfo info, out long headAddress))
                        return default;

                    if (headAddress > 0)
                        allocator.RestoreHybridLog(info.BeginAddress, headAddress, info.FlushedUntilAddress, info.FlushedUntilAddress);

                    return CompleteRestoreFromCommit(info);
                }
                catch { }
            }
            Debug.WriteLine("Unable to recover using any available commit");
            return null;
        }

        /// <summary>
        /// Restore log asynchronously
        /// </summary>
        private async ValueTask<Dictionary<string, long>> RestoreAsync(CancellationToken cancellationToken)
        {
            foreach (var commitNum in logCommitManager.ListCommits())
            {
                try
                {
                    if (!PrepareToRestoreFromCommit(commitNum, out FasterLogRecoveryInfo info, out long headAddress))
                        return default;

                    if (headAddress > 0)
                        await allocator.RestoreHybridLogAsync(info.BeginAddress, headAddress, info.FlushedUntilAddress, info.FlushedUntilAddress, cancellationToken: cancellationToken).ConfigureAwait(false);

                    return CompleteRestoreFromCommit(info);
                }
                catch { }
            }
            Debug.WriteLine("Unable to recover using any available commit");
            return null;
        }

        private bool PrepareToRestoreFromCommit(long commitNum, out FasterLogRecoveryInfo info, out long headAddress)
        {
            headAddress = 0;
            var commitInfo = logCommitManager.GetCommitMetadata(commitNum);
            if (commitInfo is null)
            {
                info = default;
                return false;
            }

            info = new FasterLogRecoveryInfo();
            using (BinaryReader r = new(new MemoryStream(commitInfo)))
            {
                info.Initialize(r);
            }

            if (!readOnlyMode)
            {
                headAddress = info.FlushedUntilAddress - allocator.GetOffsetInPage(info.FlushedUntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;
            }

            return true;
        }

        private Dictionary<string, long> CompleteRestoreFromCommit(FasterLogRecoveryInfo info)
        {
            CommittedUntilAddress = info.FlushedUntilAddress;
            CommittedBeginAddress = info.BeginAddress;
            SafeTailAddress = info.FlushedUntilAddress;

            // Fix uncommitted addresses in iterators
            var recoveredIterators = info.Iterators;
            if (recoveredIterators != null)
            {
                List<string> keys = recoveredIterators.Keys.ToList();
                foreach (var key in keys)
                    if (recoveredIterators[key] > SafeTailAddress)
                        recoveredIterators[key] = SafeTailAddress;
            }
            return recoveredIterators;
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

            ValidateAllocatedLength(allocatedLength);

            epoch.Resume();
            logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
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

        private unsafe void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            var ctx = (SimpleReadContext)context;

            if (errorCode != 0)
            {
                Trace.TraceError("AsyncGetFromDiskCallback error: {0}", errorCode);
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
        }

        private void AsyncGetHeaderOnlyFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            var ctx = (SimpleReadContext)context;

            if (errorCode != 0)
            {
                Trace.TraceError("AsyncGetFromDiskCallback error: {0}", errorCode);
                ctx.record.Return();
                ctx.record = null;
                ctx.completedRead.Release();
            }
            else
            {
                if (ctx.record.available_bytes < headerSize)
                {
                    Debug.WriteLine("No record header present at address: " + ctx.logicalAddress);
                    ctx.record.Return();
                    ctx.record = null;
                }
                ctx.completedRead.Release();
            }
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

        private (IMemoryOwner<byte>, int) GetRecordAsMemoryOwnerAndFree(SectorAlignedMemory record, MemoryPool<byte> memoryPool)
        {
            if (record == null)
                return (null, 0);

            IMemoryOwner<byte> result;
            int length;
            unsafe
            {
                var ptr = record.GetValidPointer();
                length = GetLength(ptr);
                if (!VerifyChecksum(ptr, length))
                {
                    throw new FasterException("Checksum failed for read");
                }
                result = memoryPool.Rent(length);

                fixed (byte* bp = result.Memory.Span)
                {
                    Buffer.MemoryCopy(ptr + headerSize, bp, length, length);
                }
            }
            
            record.Return();
            return (result, length);
        }

        private int GetRecordLengthAndFree(SectorAlignedMemory record)
        {
            if (record == null)
                return 0;

            int length;
            unsafe
            {
                var ptr = record.GetValidPointer();
                length = GetLength(ptr);

                if (!VerifyChecksum(ptr, length))
                {
                    throw new FasterException("Checksum failed for read");
                }
            }

            record.Return();
            return length;
        }

        private long CommitInternal(bool spinWait = false)
        {
            if (readOnlyMode)
                throw new FasterException("Cannot commit in read-only mode");

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
                if (beginAddress > CommittedBeginAddress || IteratorsChanged())
                {
                    Interlocked.Increment(ref commitMetadataVersion);
                    CommitCallback(new CommitInfo
                    {
                        Version = version,
                        FromAddress = CommittedUntilAddress > beginAddress ? CommittedUntilAddress : beginAddress,
                        UntilAddress = CommittedUntilAddress > beginAddress ? CommittedUntilAddress : beginAddress,
                        ErrorCode = 0
                    });
                }
            }

            return tailAddress;
        }

        private long RefreshUncommittedInternal(bool spinWait = false)
        {
            epoch.Resume();
            var localTail = allocator.GetTailAddress();
            if (SafeTailAddress < localTail)
                epoch.BumpCurrentEpoch(() => UpdateTailCallback(localTail));
            if (spinWait)
            {
                while (SafeTailAddress < localTail)
                {
                    epoch.ProtectAndDrain();
                    Thread.Yield();
                }
            }
            epoch.Suspend();
            return localTail;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateAllocatedLength(int numSlots)
        {
            if (numSlots > allocator.PageSize)
                throw new FasterException("Entry does not fit on page");
        }
    }
}

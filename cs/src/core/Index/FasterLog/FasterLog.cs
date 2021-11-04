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
        internal readonly bool fastCommitMode;

        private TaskCompletionSource<LinkedCommitInfo> commitTcs
            = new TaskCompletionSource<LinkedCommitInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<Empty> refreshUncommittedTcs
            = new TaskCompletionSource<Empty>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Offsets for all currently unprocessed commit records
        private Queue<(long, FasterLogRecoveryInfo)> ongoingCommitRequests;
        private long commitNum;

        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => beginAddress;

        // Here's a soft begin address that is observed by all access at the FasterLog level but not actually on the
        // allocator. This is to make sure that any potential physical deletes only happen after commit.
        private long beginAddress;

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocator.GetTailAddress();

        // Used to track the last commit record and commits that have been issued, to stop commits from committing
        // without any user records
        private long commitCoveredAddress;

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
        /// Recovered Commit Cookie
        /// </summary>
        public byte[] RecoveredCookie;

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
        /// Committed view of commitMetadataVersion
        /// </summary>
        private long persistedCommitNum;

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
        /// <param name="requestedCommitNum"> specific commit number to recover from (or -1 for latest) </param>
        public FasterLog(FasterLogSettings logSettings, long requestedCommitNum = -1)
            : this(logSettings, false)
        {
            Dictionary<string, long> it;
            if (requestedCommitNum == -1)
                RestoreLatest(out it, out RecoveredCookie);
            else
            {
                if (!logCommitManager.PreciseCommitNumRecoverySupport())
                    throw new FasterException("Recovering to a specific commit is not supported for given log setting");
                RestoreSpecificCommit(requestedCommitNum, out it, out RecoveredCookie);
            }

            RecoveredIterators = it;
        }

        /// <summary>
        /// Create new log instance asynchronously
        /// </summary>
        /// <param name="logSettings"></param>
        /// <param name="cancellationToken"></param>
        public static async ValueTask<FasterLog> CreateAsync(FasterLogSettings logSettings, CancellationToken cancellationToken = default)
        {
            var fasterLog = new FasterLog(logSettings, false);
            var (it, cookie) = await fasterLog.RestoreLatestAsync(cancellationToken).ConfigureAwait(false);
            fasterLog.RecoveredIterators = it;
            fasterLog.RecoveredCookie = cookie;
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
            beginAddress = allocator.BeginAddress;

            // FasterLog is used as a read-only iterator
            if (logSettings.ReadOnlyMode)
            {
                readOnlyMode = true;
                allocator.HeadAddress = long.MaxValue;
            }

            fastCommitMode = logSettings.FastCommitMode;

            ongoingCommitRequests = new Queue<(long, FasterLogRecoveryInfo)>();
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
            commitQueue.Dispose();
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

            var observedCommitNum = commitNum;
            while (CommittedUntilAddress < tailAddress || persistedCommitNum < observedCommitNum) Thread.Yield();
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

            var observedCommitNum = commitNum;
            while (CommittedUntilAddress < tailAddress || persistedCommitNum < observedCommitNum)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                task = linkedCommitInfo.NextTask;
            }
        }
        #endregion

        #region Commit and CommitAsync

        /// <summary>
        /// Issue commit request for log (until tail)
        /// </summary>
        /// <param name="spinWait">If true, spin-wait until commit completes. Otherwise, issue commit and return immediately.</param>
        /// <returns> whether there is anything to commit. </returns>

        public void Commit(bool spinWait = false)
        {
            CommitInternal(out _, out _, spinWait, true, null, -1);
        }

        /// <summary>
        /// Issue commit request for log (until tail) with the given commitNum
        /// </summary>
        /// <param name="commitTail"> the tail committed by this call</param>
        /// <param name="actualCommitNum">
        /// a unique, monotonically increasing identifier for the commit that can be used to recover to exactly
        /// his commit
        /// </param>
        /// <param name="spinWait">If true, spin-wait until commit completes. Otherwise, issue commit and return immediately.</param>
        /// <param name="cookie">
        /// a custom piece of metadata to be associated with this commit. If commit is successful, any recovery from
        /// this commit will recover the cookie in RecoveredCookie field. Note that cookies are not stored by FasterLog
        /// itself, so the user is responsible for tracking cookie content and supplying it to every commit call if needed
        /// </param>
        /// /// <param name="proposedCommitNum">
        /// proposal for the identifier to use for this commit, or -1 if the system should pick one. If supplied with
        /// a non -1 value, commit is guaranteed to have the supplied identifier if commit call is successful
        /// </param>
        /// <returns> whether commit is successful </returns>
        public bool CommitStrongly(out long commitTail, out long actualCommitNum, bool spinWait = false, byte[] cookie = null, long proposedCommitNum = -1)
        {
            return CommitInternal(out commitTail, out actualCommitNum, spinWait, false, cookie, proposedCommitNum);
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
            if (!CommitInternal(out var tailAddress, out var actualCommitNum,  false, true, null, -1))
                return;

            while (CommittedUntilAddress < tailAddress || persistedCommitNum < actualCommitNum)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                task = linkedCommitInfo.NextTask;
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

            if (!CommitInternal(out var tailAddress, out var actualCommitNum, false, true, null, -1))
                return prevCommitTask;

            while (CommittedUntilAddress < tailAddress || persistedCommitNum < actualCommitNum)
            {
                var linkedCommitInfo = await prevCommitTask.WithCancellationAsync(token).ConfigureAwait(false);
                if (linkedCommitInfo.CommitInfo.UntilAddress < tailAddress || persistedCommitNum < actualCommitNum)
                    prevCommitTask = linkedCommitInfo.NextTask;
                else
                    return linkedCommitInfo.NextTask;
            }

            return prevCommitTask;
        }
        
        /// <summary>
        /// Issue commit request for log (until tail) with the given commitNum
        /// </summary>
        /// <param name="commitTail"> the tail committed by this call</param>
        /// <param name="actualCommitNum">
        /// a unique, monotonically increasing identifier for the commit that can be used to recover to exactly
        /// his commit
        /// </param>
        /// <param name="spinWait">If true, spin-wait until commit completes. Otherwise, issue commit and return immediately.</param>
        /// <param name="cookie">
        /// a custom piece of metadata to be associated with this commit. If commit is successful, any recovery from
        /// this commit will recover the cookie in RecoveredCookie field. Note that cookies are not stored by FasterLog
        /// itself, so the user is responsible for tracking cookie content and supplying it to every commit call if needed
        /// </param>
        /// /// <param name="proposedCommitNum">
        /// proposal for the identifier to use for this commit, or -1 if the system should pick one. If supplied with
        /// a non -1 value, commit is guaranteed to have the supplied identifier if commit call is successful
        /// </param>
        /// <returns> whether commit is successful </returns>
        public async ValueTask<(bool, long, long)> CommitStronglyAsync(bool spinWait = false, byte[] cookie = null, long proposedCommitNum = -1,  CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            var task = CommitTask;
            if (!CommitInternal(out var commitTail, out var actualCommitNum,  false, false, cookie, proposedCommitNum))
                return ValueTuple.Create(false, commitTail, actualCommitNum);

            while (CommittedUntilAddress < commitTail || persistedCommitNum < actualCommitNum)
            {
                var linkedCommitInfo = await task.WithCancellationAsync(token).ConfigureAwait(false);
                task = linkedCommitInfo.NextTask;
            }

            return ValueTuple.Create(true, commitTail, actualCommitNum);
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
            Utility.MonotonicUpdate(ref beginAddress, untilAddress, out _);
        }

        /// <summary>
        /// Truncate the log until the start of the page corresponding to untilAddress. This is 
        /// safer than TruncateUntil, as page starts are always a valid truncation point. The
        /// truncation is not persisted until the next commit.
        /// </summary>
        /// <param name="untilAddress">Until address</param>
        public void TruncateUntilPageStart(long untilAddress)
        {
            Utility.MonotonicUpdate(ref beginAddress, untilAddress & ~allocator.PageSizeMask, out _);
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
            // Using count is safe as a fast filtering mechanism to reduce number of invocations despite concurrency
            if (ongoingCommitRequests.Count == 0) return;
            commitQueue.EnqueueAndTryWork(commitInfo, asTask: true);
        }

        private unsafe bool TryEnqueueCommitRecord(ref FasterLogRecoveryInfo info)
        {
            var entryBodySize = info.SerializedSize();
            
            int allocatedLength = headerSize + Align(entryBodySize);
            ValidateAllocatedLength(allocatedLength);
            
            epoch.Resume();
            
            var logicalAddress = allocator.TryAllocateRetryNow(allocatedLength);
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }
            // Finish filling in all fields
            info.BeginAddress = BeginAddress;
            info.UntilAddress = logicalAddress + allocatedLength;

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);

            var entryBody = info.ToByteArray();
            fixed (byte* bp = entryBody)
                Buffer.MemoryCopy(bp, (void*)(headerSize + physicalAddress), entryBody.Length, entryBody.Length);
            SetCommitRecordHeader(entryBody.Length, (byte*)physicalAddress);
            epoch.Suspend();
            // Return the commit tail
            return true;
        }

        private bool ShouldCommmitMetadata(ref FasterLogRecoveryInfo info)
        {
            return beginAddress > CommittedBeginAddress || IteratorsChanged(ref info) || info.Cookie != null;
        }
        
        private void CommitMetadataOnly(ref FasterLogRecoveryInfo info, bool spinWait)
        {
            var fromAddress = CommittedUntilAddress > info.BeginAddress ? CommittedUntilAddress : info.BeginAddress;
            var untilAddress = CommittedUntilAddress > info.BeginAddress ? CommittedUntilAddress : info.BeginAddress;
            
            CommitCallback(new CommitInfo
            {
                FromAddress = fromAddress,
                UntilAddress = untilAddress,
                ErrorCode = 0,
            });

            if (spinWait)
            {
                while (info.CommitNum < persistedCommitNum)
                    Thread.Yield();
            }
        }

        private void UpdateCommittedState(FasterLogRecoveryInfo recoveryInfo)
        {
            LastPersistedIterators = recoveryInfo.Iterators;
            CommittedBeginAddress = recoveryInfo.BeginAddress;
            CommittedUntilAddress = recoveryInfo.UntilAddress;
            recoveryInfo.CommitIterators(PersistedIterators);
            Utility.MonotonicUpdate(ref persistedCommitNum, recoveryInfo.CommitNum, out _);
        }

        private void WriteCommitMetadata(FasterLogRecoveryInfo recoveryInfo)
        {
            // TODO(Tianyu): If fast commit, write this in separate thread?
            logCommitManager.Commit(recoveryInfo.BeginAddress, recoveryInfo.UntilAddress,
                recoveryInfo.ToByteArray(), recoveryInfo.CommitNum);
            // If not fast committing, set committed state as we commit metadata explicitly only after metadata commit
            if (!fastCommitMode)
                UpdateCommittedState(recoveryInfo);
            // Issue any potential physical deletes due to shifts in begin address
            allocator.ShiftBeginAddress(recoveryInfo.BeginAddress);
        }

        private void SerialCommitCallbackWorker(CommitInfo commitInfo)
        {
            if (commitInfo.ErrorCode == 0)
            {
                var coveredCommits = new List<FasterLogRecoveryInfo>();
                // Check for the commit records included in this flush
                lock (ongoingCommitRequests)
                {
                    while (ongoingCommitRequests.Count != 0)
                    {
                        var (addr, recoveryInfo) = ongoingCommitRequests.Peek();
                        if (addr > commitInfo.UntilAddress) break;
                        coveredCommits.Add(recoveryInfo);
                        ongoingCommitRequests.Dequeue();
                    }
                }

                // Nothing was committed --- this was probably au auto-flush. Return now without touching any
                // commit task tracking.
                if (coveredCommits.Count == 0) return;

                var latestCommit = coveredCommits[coveredCommits.Count - 1];
                if (fastCommitMode)
                    // In fast commit mode, can safely set committed state to the latest flushed
                    UpdateCommittedState(latestCommit);

                foreach (var recoveryInfo in coveredCommits)
                {
                    // Only write out commit metadata if user cares about this as a distinct recoverable point
                    if (!recoveryInfo.FastForwardAllowed) WriteCommitMetadata(recoveryInfo);
                }

                // We fast-forwarded commits earlier, so write it out if not covered by another commit
                if (latestCommit.FastForwardAllowed) WriteCommitMetadata(latestCommit);
            }

            // TODO(Tianyu): Can invoke earlier in the case of fast commit
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

        private bool IteratorsChanged(ref FasterLogRecoveryInfo info)
        {
            var _lastPersistedIterators = LastPersistedIterators;
            if (_lastPersistedIterators == null)
            {
                return info.Iterators != null &&  info.Iterators.Count != 0;
            }
            if (_lastPersistedIterators.Count != info.Iterators.Count)
                return true;
            foreach (var item in _lastPersistedIterators)
            {
                if (info.Iterators.TryGetValue(item.Key, out var other))
                {
                    if (item.Value != other) return true;
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

        
        // TODO(Tianyu): Will we ever need to recover to a specific commit for read-only cases?
        /// <summary>
        /// Synchronously recover instance to FasterLog's latest commit, when being used as a readonly log iterator
        /// </summary>
        public void RecoverReadOnly()
        {
            if (!readOnlyMode)
                throw new FasterException("This method can only be used with a read-only FasterLog instance used for iteration. Set FasterLogSettings.ReadOnlyMode to true during creation to indicate this.");

            this.RestoreLatest(out _, out _);
            SignalWaitingROIterators();
        }

        /// <summary>
        /// Asynchronously recover instance to FasterLog's latest commit, when being used as a readonly log iterator
        /// </summary>
        public async ValueTask RecoverReadOnlyAsync(CancellationToken cancellationToken = default)
        {
            if (!readOnlyMode)
                throw new FasterException("This method can only be used with a read-only FasterLog instance used for iteration. Set FasterLogSettings.ReadOnlyMode to true during creation to indicate this.");

            await this.RestoreLatestAsync(cancellationToken).ConfigureAwait(false);
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
                CommitInfo = new CommitInfo { FromAddress = BeginAddress, UntilAddress = FlushedUntilAddress },
                NextTask = commitTcs.Task
            };
            _commitTcs?.TrySetResult(lci);
        }

        private bool LoadCommitMetadata(long commitNum, out FasterLogRecoveryInfo info)
        {
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

            if (info.CommitNum == -1)
                info.CommitNum = commitNum;

            return true;
        }

        private void RestoreLatest(out Dictionary<string, long> iterators, out byte[] cookie)
        {
            iterators = null;
            cookie = null;
            FasterLogRecoveryInfo info = new();
            
            foreach (var metadataCommit in logCommitManager.ListCommits())
            {
                try
                {
                    if (LoadCommitMetadata(metadataCommit, out info))
                        break;
                }
                catch { }
            }

            // Only in fast commit mode will we potentially need to recover from an entry in the log
            if (fastCommitMode)
            {
                // Shut up safe guards, I know what I am doing
                CommittedUntilAddress = long.MaxValue;
                beginAddress = info.BeginAddress;
                allocator.HeadAddress = long.MaxValue;
                using var scanIterator = Scan(info.UntilAddress, long.MaxValue, recover: false);
                scanIterator.ScanForwardForCommit(ref info);
            }

            // if until address is 0, that means info is still its default value and we haven't been able to recover
            // from any any commit. Set the log to its start position and return
            if (info.UntilAddress == 0)
            {
                Debug.WriteLine("Unable to recover using any available commit");
                // Reset things to be something normal lol
                allocator.Initialize();
                CommittedUntilAddress = Constants.kFirstValidAddress;
                beginAddress = allocator.BeginAddress;
                if (readOnlyMode)
                    allocator.HeadAddress = long.MaxValue;
                return;
            }

            if (!readOnlyMode)
            {
                var headAddress = info.UntilAddress - allocator.GetOffsetInPage(info.UntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;
                
                allocator.RestoreHybridLog(info.BeginAddress, headAddress, info.UntilAddress, info.UntilAddress);
            }

            iterators = CompleteRestoreFromCommit(info);
            cookie = info.Cookie;
            commitNum = info.CommitNum;
            beginAddress = allocator.BeginAddress;
            if (readOnlyMode)
                allocator.HeadAddress = long.MaxValue;
        }

        private void RestoreSpecificCommit(long requestedCommitNum, out Dictionary<string, long> iterators, out byte[] cookie)
        {
            if (!logCommitManager.PreciseCommitNumRecoverySupport())
                throw new FasterException("Given FasterLog does not support recovery to a precise commit num");
            iterators = null;
            cookie = null;
            FasterLogRecoveryInfo info = new();

            // Find the closest commit metadata with commit num smaller than requested
            long scanStart = 0;
            foreach (var metadataCommit in logCommitManager.ListCommits())
            {
                if (metadataCommit > requestedCommitNum) continue;
                try
                {
                    if (LoadCommitMetadata(metadataCommit, out info))
                    {
                        scanStart = metadataCommit;
                        break;
                    }
                }
                catch { }
            }
            
            // Need to potentially scan log for the entry 
            if (scanStart < requestedCommitNum)
            {
                // If not in fast commit mode, do not scan log
                if (!fastCommitMode)
                    // In the case where precisely requested commit num is not available, can just throw exception
                    throw new FasterException("requested commit num is not available");
                
                // If no exact metadata is found, scan forward to see if we able to find a commit entry
                // Shut up safe guards, I know what I am doing
                CommittedUntilAddress = long.MaxValue;
                beginAddress = info.BeginAddress;
                allocator.HeadAddress = long.MaxValue;
                using var scanIterator = Scan(info.UntilAddress, long.MaxValue, recover: false);
                if (!scanIterator.ScanForwardForCommit(ref info, requestedCommitNum))
                    throw new FasterException("requested commit num is not available");
            }

            // At this point, we should have found the exact commit num requested
            Debug.Assert(info.CommitNum == requestedCommitNum);
            if (!readOnlyMode)
            {
                var headAddress = info.UntilAddress - allocator.GetOffsetInPage(info.UntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;
                allocator.RestoreHybridLog(info.BeginAddress, headAddress, info.UntilAddress, info.UntilAddress);
            }

            iterators = CompleteRestoreFromCommit(info);
            cookie = info.Cookie;
            commitNum = info.CommitNum;
            beginAddress = allocator.BeginAddress;
            if (readOnlyMode)
                allocator.HeadAddress = long.MaxValue;
        }

        /// <summary>
        /// Restore log asynchronously
        /// </summary>
        private async ValueTask<(Dictionary<string, long>, byte[])> RestoreLatestAsync(CancellationToken cancellationToken)
        {
            FasterLogRecoveryInfo info = new();
            
            foreach (var metadataCommit in logCommitManager.ListCommits())
            {
                try
                {
                    if (LoadCommitMetadata(metadataCommit, out info))
                        break;
                }
                catch { }
            }

            // Only in fast commit mode will we potentially need to recover from an entry in the log
            if (fastCommitMode)
            {
                // Shut up safe guards, I know what I am doing
                CommittedUntilAddress = long.MaxValue;
                beginAddress = info.BeginAddress;
                allocator.HeadAddress = long.MaxValue;
                using var scanIterator = Scan(info.UntilAddress, long.MaxValue, recover: false);
                scanIterator.ScanForwardForCommit(ref info);
            }

            // if until address is 0, that means info is still its default value and we haven't been able to recover
            // from any any commit. Set the log to its start position and return
            if (info.UntilAddress == 0)
            {
                Debug.WriteLine("Unable to recover using any available commit");
                // Reset things to be something normal lol
                allocator.Initialize();
                CommittedUntilAddress = Constants.kFirstValidAddress;
                beginAddress = allocator.BeginAddress;
                if (readOnlyMode)
                    allocator.HeadAddress = long.MaxValue;
                return ValueTuple.Create<Dictionary<string, long>, byte[]>(new Dictionary<string, long>(), null);
            }
            
            if (!readOnlyMode)
            {
                var headAddress = info.UntilAddress - allocator.GetOffsetInPage(info.UntilAddress);
                if (info.BeginAddress > headAddress)
                    headAddress = info.BeginAddress;

                if (headAddress == 0)
                    headAddress = Constants.kFirstValidAddress;
                await allocator.RestoreHybridLogAsync(info.BeginAddress, headAddress, info.UntilAddress, info.UntilAddress, cancellationToken : cancellationToken).ConfigureAwait(false);
            }

            var iterators = CompleteRestoreFromCommit(info);
            var cookie = info.Cookie;
            commitNum = info.CommitNum;
            beginAddress = allocator.BeginAddress;
            if (readOnlyMode)
                allocator.HeadAddress = long.MaxValue;
            return ValueTuple.Create(iterators, cookie);
        }

        private Dictionary<string, long> CompleteRestoreFromCommit(FasterLogRecoveryInfo info)
        {
            CommittedUntilAddress = info.UntilAddress;
            CommittedBeginAddress = info.BeginAddress;
            SafeTailAddress = info.UntilAddress;

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

        private bool CommitInternal(out long commitTail, out long actualCommitNum, bool spinWait, bool allowFastForward, byte[] cookie, long proposedCommitNum)
        {
            commitTail = actualCommitNum = 0;
            
            if (readOnlyMode)
                throw new FasterException("Cannot commit in read-only mode");

            if (allowFastForward && (cookie != null || proposedCommitNum != -1))
                throw new FasterException(
                    "Fast forwarding a commit is only allowed when no cookie and not commit num is specified");
            
            var info = new FasterLogRecoveryInfo();
            info.FastForwardAllowed = allowFastForward;

            // This critical section serializes commit record creation / commit content generation and ensures that the
            // long address are sorted in outstandingCommitRecords. Ok because we do not expect heavy contention on the
            // commit code path
            lock (ongoingCommitRequests)
            {
                // Compute regular information about the commit
                info.Cookie = cookie;
                info.SnapshotIterators(PersistedIterators);
                
                if (commitCoveredAddress == TailAddress && !ShouldCommmitMetadata(ref info))
                    // Nothing to commit if no metadata update and no new entries
                    return false;

                
                if (proposedCommitNum == -1)
                    info.CommitNum = actualCommitNum = ++commitNum;
                else if (proposedCommitNum > commitNum)
                    info.CommitNum = actualCommitNum = commitNum = proposedCommitNum;
                else
                    // Invalid commit num
                    return false;

                if (fastCommitMode)
                {
                    // Ok to retry in critical section, any concurrently invoked commit would block, but cannot progress
                    // anyways if no record can be enqueued
                    while (!TryEnqueueCommitRecord(ref info)) {}
                    commitTail = info.UntilAddress;
                }
                else
                {
                    // If not using fastCommitMode, do not need to allocate a commit record. Instead, set the content
                    // of this commit to the current tail and base all commit metadata on this address, even though
                    // perhaps more entries will be flushed as part of this commit
                    info.BeginAddress = BeginAddress;
                    info.UntilAddress = commitTail = TailAddress;
                }
                Utility.MonotonicUpdate(ref commitCoveredAddress, commitTail, out _);

                // Enqueue the commit record's content and offset into the queue so it can be picked up by the next flush
                // At this point, we expect the commit record to be flushed out as a distinct recovery point
                ongoingCommitRequests.Enqueue(ValueTuple.Create(commitTail, info));
            }
            

            // Need to check, however, that a concurrent flush hasn't already advanced flushed address past this
            // commit. If so, need to manually trigger another commit callback in case the one triggered by the flush
            // already finished execution and missed our commit record
            if (commitTail < FlushedUntilAddress)
            {
                CommitMetadataOnly(ref info, spinWait);
                return true;
            }
             
            // Otherwise, move to set read-only tail and flush 
            epoch.Resume();

            if (allocator.ShiftReadOnlyToTail(out _, out _))
            {
                if (spinWait)
                {
                    while (CommittedUntilAddress < commitTail)
                    {
                        epoch.ProtectAndDrain();
                        Thread.Yield();
                    }
                }
            }
            else
            {
                CommitMetadataOnly(ref info, spinWait);
            }
            epoch.Suspend();
            return true;
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
        private unsafe void SetCommitRecordHeader(int length, byte* dest)
        {
            // commit record has negative length field to differentiate from normal records
            if (logChecksum == LogChecksumType.None)
            {
                *(int*)dest = -length;
                return;
            }
            else if (logChecksum == LogChecksumType.PerEntry)
            {
                *(int*)(dest + 8) = -length;
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

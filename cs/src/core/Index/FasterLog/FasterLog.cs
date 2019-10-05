// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
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
        private ILogCommitManager logCommitManager;
        private TaskCompletionSource<long> commitTcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

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
        /// Log commit until address
        /// </summary>
        public long CommittedUntilAddress;

        /// <summary>
        /// Task notifying commit completions
        /// </summary>
        public Task<long> CommitTask => commitTcs.Task;

        /// <summary>
        /// Create new log instance
        /// </summary>
        /// <param name="logSettings"></param>
        public FasterLog(FasterLogSettings logSettings)
        {
            logCommitManager = logSettings.LogCommitManager ?? 
                new LocalLogCommitManager(logSettings.LogCommitFile ??
                logSettings.LogDevice.FileName + ".commit");

            epoch = new LightEpoch();
            CommittedUntilAddress = Constants.kFirstValidAddress;
            allocator = new BlittableAllocator<Empty, byte>(
                logSettings.GetLogSettings(), null, 
                null, epoch, e => Commit(e));
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
        }

        /// <summary>
        /// Append entry to log
        /// </summary>
        /// <param name="entry"></param>
        /// <returns>Logical address of added entry</returns>
        public long Append(ReadOnlySpan<byte> entry)
        {
            long logicalAddress;
            while (!TryAppend(entry, out logicalAddress)) ;
            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log
        /// </summary>
        /// <param name="entry"></param>
        /// <returns>Logical address of added entry</returns>
        public long Append(byte[] entry)
        {
            long logicalAddress;
            while (!TryAppend(entry, out logicalAddress)) ;
            return logicalAddress;
        }

        /// <summary>
        /// Try to append entry to log. If it returns true, we are
        /// done. If it returns false, we need to retry.
        /// </summary>
        /// <param name="entry">Entry to be appended to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryAppend(byte[] entry, out long logicalAddress)
        {
            logicalAddress = 0;

            epoch.Resume();

            var length = entry.Length;
            logicalAddress = allocator.TryAllocate(4 + Align(length));
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = entry)
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);

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
        public unsafe bool TryAppend(ReadOnlySpan<byte> entry, out long logicalAddress)
        {
            logicalAddress = 0;

            epoch.Resume();

            var length = entry.Length;
            logicalAddress = allocator.TryAllocate(4 + Align(length));
            if (logicalAddress == 0)
            {
                epoch.Suspend();
                return false;
            }

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = &entry.GetPinnableReference())
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);

            epoch.Suspend();
            return true;
        }

        /// <summary>
        /// Try to append batch of entries as a single atomic unit. Entire batch
        /// needs to fit on one page.
        /// </summary>
        /// <param name="readOnlySpanBatch">Batch to be appended to log</param>
        /// <param name="logicalAddress">Logical address of first added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public bool TryAppend(IReadOnlySpanBatch readOnlySpanBatch, out long logicalAddress)
        {
            return TryAppend(readOnlySpanBatch, out logicalAddress, out _);
        }

        /// <summary>
        /// Append entry to log (async) - completes after entry is flushed to storage
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> AppendAsync(byte[] entry)
        {
            long logicalAddress;

            // Phase 1: wait for commit to memory
            while (true)
            {
                var task = CommitTask;
                if (TryAppend(entry, out logicalAddress))
                    break;
                await task;
            }

            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                var task = CommitTask;
                if (CommittedUntilAddress < logicalAddress + 4 + entry.Length)
                {
                    await task;
                }
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log (async) - completes after batch is flushed to storage
        /// </summary>
        /// <param name="readOnlySpanBatch"></param>
        /// <returns></returns>
        public async ValueTask<long> AppendAsync(IReadOnlySpanBatch readOnlySpanBatch)
        {
            long logicalAddress;
            int allocatedLength;

            // Phase 1: wait for commit to memory
            while (true)
            {
                var task = CommitTask;
                if (TryAppend(readOnlySpanBatch, out logicalAddress, out allocatedLength))
                    break;
                await task;
            }

            // Phase 2: wait for commit/flush to storage
            while (true)
            {
                var task = CommitTask;
                if (CommittedUntilAddress < logicalAddress + allocatedLength)
                {
                    await task;
                }
                else
                    break;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log in memory (async) - completes after entry is appended
        /// to memory, not necessarily committed to storage.
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> AppendToMemoryAsync(byte[] entry)
        {
            long logicalAddress;

            while (true)
            {
                var task = CommitTask;
                if (TryAppend(entry, out logicalAddress))
                    break;
                await task;
            }

            return logicalAddress;
        }

        /// <summary>
        /// Wait for all prior appends (in memory) to commit to storage. Does not
        /// itself issue a commit, just waits for commit. So you should ensure that
        /// someone else causes the commit to happen.
        /// </summary>
        /// <param name="untilAddress">Address until which we should wait for commit, default 0 for tail of log</param>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(long untilAddress = 0)
        {
            var tailAddress = untilAddress;
            if (tailAddress == 0) tailAddress = allocator.GetTailAddress();

            while (true)
            {
                var task = CommitTask;
                if (CommittedUntilAddress < tailAddress)
                {
                    await task;
                }
                else
                    break;
            }
        }

        /// <summary>
        /// Flush the log until tail
        /// </summary>
        /// <param name="spinWait">If true, wait until flush completes. Otherwise, issue flush and return.</param>
        /// <returns></returns>
        public long FlushAndCommit(bool spinWait = false)
        {
            epoch.Resume();
            allocator.ShiftReadOnlyToTail(out long tailAddress, out _);
            
            if (spinWait)
            {
                while (CommittedUntilAddress < tailAddress)
                {
                    epoch.ProtectAndDrain();
                    Thread.Yield();
                }
            }
            epoch.Suspend();
            return tailAddress;
        }

        /// <summary>
        /// Async flush log until tail
        /// </summary>
        /// <returns></returns>
        public async ValueTask<long> FlushAndCommitAsync()
        {
            var tailAddress = FlushAndCommit();

            while (true)
            {
                var task = CommitTask;
                if (CommittedUntilAddress < tailAddress)
                {
                    await task;
                }
                else
                    break;
            }
            return tailAddress;
       }

        /// <summary>
        /// Truncate the log until, but not including, untilAddress
        /// </summary>
        /// <param name="untilAddress"></param>
        public void TruncateUntil(long untilAddress)
        {
            epoch.Resume();
            allocator.ShiftBeginAddress(untilAddress);
            epoch.Suspend();
        }

        /// <summary>
        /// Pull-based iterator interface for scanning FASTER log
        /// </summary>
        /// <param name="beginAddress">Begin address for scan</param>
        /// <param name="endAddress">End address for scan (or long.MaxValue for tailing)</param>
        /// <param name="getMemory">Delegate to provide user memory where data gets copied to</param>
        /// <param name="scanBufferingMode">Use single or double buffering</param>
        /// <returns></returns>
        public FasterLogScanIterator Scan(long beginAddress, long endAddress, GetMemory getMemory = null, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
        {
            return new FasterLogScanIterator(this, allocator, beginAddress, endAddress, getMemory, scanBufferingMode, epoch);
        }

        /// <summary>
        /// Create and pin epoch entry for this thread - use with ReleaseThread
        /// if you manage the thread.
        /// DO NOT USE WITH ASYNC CODE
        /// </summary>
        public void AcquireThread()
        {
            epoch.Acquire();
        }

        /// <summary>
        /// Dispose epoch entry for this thread. Use with AcquireThread
        /// if you manage the thread.
        /// DO NOT USE WITH ASYNC CODE
        /// </summary>
        public void ReleaseThread()
        {
            epoch.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Align(int length)
        {
            return (length + 3) & ~3;
        }

        /// <summary>
        /// Commit log
        /// </summary>
        private void Commit(long flushAddress)
        {
            FasterLogRecoveryInfo info = new FasterLogRecoveryInfo();
            info.FlushedUntilAddress = flushAddress;
            info.BeginAddress = allocator.BeginAddress;

            var _newCommitTask = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<long> _commitTask;

            // We can only allow serial monotonic synchronous commit
            lock (this)
            {
                if (flushAddress > CommittedUntilAddress)
                {
                    logCommitManager.Commit(flushAddress, info.ToByteArray());
                    CommittedUntilAddress = flushAddress;
                    // info.DebugPrint();
                }

                _commitTask = commitTcs;
                commitTcs = _newCommitTask;
            }
            _commitTask.SetResult(flushAddress);
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
                allocatedLength += Align(readOnlySpanBatch.Get(i).Length) + 4;
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
                *(int*)physicalAddress = entryLength;
                fixed (byte* bp = &span.GetPinnableReference())
                    Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), entryLength, entryLength);
                physicalAddress += Align(entryLength) + 4;
            }

            epoch.Suspend();
            return true;
        }
    }
}

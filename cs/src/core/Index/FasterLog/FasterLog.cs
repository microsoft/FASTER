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
        private ILogCommitManager logCommitManager;

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
        /// Create new log instance
        /// </summary>
        /// <param name="logSettings"></param>
        public FasterLog(FasterLogSettings logSettings)
        {
            logCommitManager = logSettings.LogCommitManager ?? 
                new LocalLogCommitManager(logSettings.LogCommitFile ??
                logSettings.LogDevice.FileName + ".commit");

            epoch = new LightEpoch();
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
        public unsafe long Append(Span<byte> entry)
        {
            epoch.Resume();
            var length = entry.Length;
            var alignedLength = (length + 3) & ~3; // round up to multiple of 4
            BlockAllocate(4 + alignedLength, out long logicalAddress);
            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = &entry.GetPinnableReference())
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);
            epoch.Suspend();
            return logicalAddress;
        }

        /// <summary>
        /// Append entry to log
        /// </summary>
        /// <param name="entry"></param>
        /// <returns>Logical address of added entry</returns>
        public unsafe long Append(byte[] entry)
        {
            epoch.Resume();
            var length = entry.Length;
            var alignedLength = (length + 3) & ~3; // round up to multiple of 4
            BlockAllocate(4 + alignedLength, out long logicalAddress);
            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = entry)
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);
            epoch.Suspend();
            return logicalAddress;
        }

        /// <summary>
        /// Try to append entry to log. If is returns true, we are
        /// done. If it returns false with negative address, user
        /// needs to call TryCompleteAppend to finalize the append.
        /// See TryCompleteAppend for more info.
        /// </summary>
        /// <param name="entry">Entry to be appended to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryAppend(byte[] entry, ref long logicalAddress)
        {
            if (logicalAddress < 0)
                return TryCompleteAppend(entry, ref logicalAddress);

            epoch.Resume();

            var length = entry.Length;
            var alignedLength = (length + 3) & ~3; // round up to multiple of 4

            logicalAddress = allocator.TryAllocate(4 + alignedLength);
            if (logicalAddress <= 0)
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
        /// Try to append entry to log. If is returns true, we are
        /// done. If it returns false with negative address, user
        /// needs to call TryCompleteAppend to finalize the append.
        /// See TryCompleteAppend for more info.
        /// </summary>
        /// <param name="entry">Entry to be appended to log</param>
        /// <param name="logicalAddress">Logical address of added entry</param>
        /// <returns>Whether the append succeeded</returns>
        public unsafe bool TryAppend(Span<byte> entry, ref long logicalAddress)
        {
            if (logicalAddress < 0)
                return TryCompleteAppend(entry, ref logicalAddress);

            epoch.Resume();

            var length = entry.Length;
            var alignedLength = (length + 3) & ~3; // round up to multiple of 4

            logicalAddress = allocator.TryAllocate(4 + alignedLength);
            if (logicalAddress <= 0)
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

        private TaskCompletionSource<long> commitTask = new TaskCompletionSource<long>();
        private int commitTaskWaiters;

        /// <summary>
        /// Append entry to log (async) - completes after entry is flushed to storage
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public async ValueTask<long> AppendAsync(byte[] entry)
        {
            long logicalAddress = 0;
            long commitAddress = CommittedUntilAddress;

            while (true)
            {
                if (TryAppend(entry, ref logicalAddress))
                    break;

                while (true)
                {
                    while (commitTaskWaiters < 0) ;
                    if (Interlocked.Increment(ref commitTaskWaiters) >= 0)
                    {
                        var tcs = commitTask;
                        commitAddress = await tcs.Task;
                        Interlocked.Decrement(ref commitTaskWaiters);
                        break;
                    }

                    Interlocked.Decrement(ref commitTaskWaiters);
                }
            }

            while (commitAddress < logicalAddress + 4 + entry.Length)
            {
                while (true)
                {
                    if (Interlocked.Increment(ref commitTaskWaiters) >= 0)
                    {
                        var tcs = commitTask;
                        Interlocked.Decrement(ref commitTaskWaiters);
                        commitAddress = await tcs.Task;
                        break;
                    }

                    Interlocked.Decrement(ref commitTaskWaiters);
                }
            }

            return logicalAddress;
        }

        /// <summary>
        /// Flush the log until tail
        /// </summary>
        public long FlushAndCommit(bool spinWait = false)
        {
            epoch.Resume();
            allocator.ShiftReadOnlyToTail(out long tailAddress);
            
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
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <returns></returns>
        public FasterLogScanIterator Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
        {
            return new FasterLogScanIterator(allocator, beginAddress, endAddress, scanBufferingMode, epoch);
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

        /// <summary>
        /// Block allocate
        /// </summary>
        /// <param name="recordSize"></param>
        /// <param name="logicalAddress"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BlockAllocate(int recordSize, out long logicalAddress)
        {
            logicalAddress = allocator.Allocate(recordSize);
            if (logicalAddress >= 0) return;

            while (logicalAddress < 0 && -logicalAddress >= allocator.ReadOnlyAddress)
            {
                epoch.ProtectAndDrain();
                allocator.CheckForAllocateComplete(ref logicalAddress);
                if (logicalAddress < 0)
                {
                    Thread.Yield();
                }
            }

            logicalAddress = logicalAddress < 0 ? -logicalAddress : logicalAddress;

            if (logicalAddress < allocator.ReadOnlyAddress)
            {
                Debug.WriteLine("Allocated address is read-only, retrying");
                BlockAllocate(recordSize, out logicalAddress);
            }
        }

        /// <summary>
        /// Try to complete partial allocation. Succeeds when address
        /// turns positive. If failed with negative address, try the
        /// operation. If failed with zero address, user needs to start 
        /// afresh with a new TryAppend operation.
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="logicalAddress"></param>
        /// <returns>Whether operation succeeded</returns>
        private unsafe bool TryCompleteAppend(byte[] entry, ref long logicalAddress)
        {
            epoch.Resume();

            allocator.CheckForAllocateComplete(ref logicalAddress);

            if (logicalAddress < 0)
            {
                epoch.Suspend();
                return false;
            }

            if (logicalAddress < allocator.ReadOnlyAddress)
            {
                logicalAddress = 0;
                epoch.Suspend();
                return false;
            }

            var length = entry.Length;

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = entry)
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);

            epoch.Suspend();
            return true;
        }

        /// <summary>
        /// Try to complete partial allocation. Succeeds when address
        /// turns positive. If failed with negative address, try the
        /// operation. If failed with zero address, user needs to start 
        /// afresh with a new TryAppend operation.
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="logicalAddress"></param>
        /// <returns>Whether operation succeeded</returns>
        private unsafe bool TryCompleteAppend(Span<byte> entry, ref long logicalAddress)
        {
            epoch.Resume();

            allocator.CheckForAllocateComplete(ref logicalAddress);

            if (logicalAddress < 0)
            {
                epoch.Suspend();
                return false;
            }

            if (logicalAddress < allocator.ReadOnlyAddress)
            {
                logicalAddress = 0;
                epoch.Suspend();
                return false;
            }

            var length = entry.Length;

            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = &entry.GetPinnableReference())
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);

            epoch.Suspend();
            return true;
        }

        /// <summary>
        /// Commit log
        /// </summary>
        private void Commit(long flushAddress)
        {
            FasterLogRecoveryInfo info = new FasterLogRecoveryInfo();
            info.FlushedUntilAddress = flushAddress;
            info.BeginAddress = allocator.BeginAddress;

            // We can only allow serial monotonic synchronous commit
            lock (this)
            {
                if (flushAddress > CommittedUntilAddress)
                {
                    logCommitManager.Commit(flushAddress, info.ToByteArray());
                    CommittedUntilAddress = flushAddress;
                    // info.DebugPrint();
                }

                Interlocked.Add(ref commitTaskWaiters, -(1 << 16));
                while (commitTaskWaiters != -(1 << 16)) ;
                var _commitTask = commitTask;
                commitTask = new TaskCompletionSource<long>();
                Interlocked.Add(ref commitTaskWaiters, (1 << 16));

                _commitTask.SetResult(flushAddress);
            }
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

            allocator.RestoreHybridLog(info.FlushedUntilAddress,
                info.FlushedUntilAddress - allocator.GetOffsetInPage(info.FlushedUntilAddress),
                info.BeginAddress);
        }
    }
}

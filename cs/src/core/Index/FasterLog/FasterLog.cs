// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{

    /// <summary>
    /// FASTER log
    /// </summary>
    public class FasterLog : IDisposable
    {
        private readonly BlittableAllocator<Empty, byte> allocator;
        private readonly LightEpoch epoch;

        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => allocator.BeginAddress;

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocator.GetTailAddress();

        /// <summary>
        /// Flushed until address
        /// </summary>
        public long FlushedUntilAddress => allocator.FlushedUntilAddress;

        /// <summary>
        /// Create new log instance
        /// </summary>
        /// <param name="logSettings"></param>
        public FasterLog(FasterLogSettings logSettings)
        {
            epoch = new LightEpoch();
            allocator = new BlittableAllocator<Empty, byte>(logSettings.GetLogSettings(), null, null, epoch);
            allocator.Initialize();
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
            BlockAllocate(4 + length, out long logicalAddress);
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
            BlockAllocate(4 + length, out long logicalAddress);
            var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
            *(int*)physicalAddress = length;
            fixed (byte* bp = entry)
                Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);
            epoch.Suspend();
            return logicalAddress;
        }

        /// <summary>
        /// Append batch of entries to log
        /// </summary>
        /// <param name="entries"></param>
        /// <returns>Logical address of last added entry</returns>
        public unsafe long Append(List<byte[]> entries)
        {
            long logicalAddress = 0;
            epoch.Resume();
            foreach (var entry in entries)
            {
                var length = entry.Length;
                BlockAllocate(4 + length, out logicalAddress);
                var physicalAddress = allocator.GetPhysicalAddress(logicalAddress);
                *(int*)physicalAddress = length;
                fixed (byte* bp = entry)
                    Buffer.MemoryCopy(bp, (void*)(4 + physicalAddress), length, length);
            }
            epoch.Suspend();
            return logicalAddress;
        }

        /// <summary>
        /// Flush the log until tail
        /// </summary>
        public long Flush(bool spinWait = false)
        {
            epoch.Resume();
            allocator.ShiftReadOnlyToTail(out long tailAddress, out SemaphoreSlim _);
            epoch.Suspend();
            if (spinWait)
            {
                while (allocator.FlushedUntilAddress < tailAddress)
                    Thread.Yield();
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
        /// Iterator interface for scanning FASTER log
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
                    Thread.Sleep(10);
                }
            }

            logicalAddress = logicalAddress < 0 ? -logicalAddress : logicalAddress;

            if (logicalAddress < allocator.ReadOnlyAddress)
            {
                Debug.WriteLine("Allocated address is read-only, retrying");
                BlockAllocate(recordSize, out logicalAddress);
            }
        }
    }
}

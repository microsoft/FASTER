// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

#define CALLOC

using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Memory allocator for objects
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MallocFixedPageSize<T> : IDisposable
    {
        private const bool ForceUnpinnedAllocation = false;

        private const int PageSizeBits = 16;
        private const int PageSize = 1 << PageSizeBits;
        private const int PageSizeMask = PageSize - 1;
        private const int LevelSizeBits = 12;
        private const int LevelSize = 1 << LevelSizeBits;

        private T[][] values = new T[LevelSize][];
        private GCHandle[] handles = new GCHandle[LevelSize];
        private IntPtr[] pointers = new IntPtr[LevelSize];

        private readonly int RecordSize;

        private volatile int writeCacheLevel;

        private volatile int count;

        private readonly bool IsPinned;
        private readonly bool ReturnPhysicalAddress;

        private int checkpointCallbackCount;
        private SemaphoreSlim checkpointSemaphore;

        private ConcurrentQueue<long> freeList;

        /// <summary>
        /// Create new instance
        /// </summary>
        /// <param name="returnPhysicalAddress"></param>
        public MallocFixedPageSize(bool returnPhysicalAddress = false)
        {
            freeList = new ConcurrentQueue<long>();

            values[0] = new T[PageSize];

#if !(CALLOC)
            Array.Clear(values[0], 0, PageSize);
#endif
            ReturnPhysicalAddress = returnPhysicalAddress;

            if (ForceUnpinnedAllocation)
            {
                IsPinned = false;
                ReturnPhysicalAddress = false;
            }
            else
            {
                IsPinned = true;
                if (default(T) == null)
                {
                    IsPinned = false;
                    ReturnPhysicalAddress = false;
                }
                else
                {
                    // The surefire way to check if a type is blittable
                    // it to try GCHandle.Alloc with a handle type of Pinned.
                    // If it throws an exception, we know the type is not blittable.
                    try
                    {
                        handles[0] = GCHandle.Alloc(values[0], GCHandleType.Pinned);
                        pointers[0] = handles[0].AddrOfPinnedObject();
                        RecordSize = Marshal.SizeOf(values[0][0]);
                    }
                    catch (Exception)
                    {
                        IsPinned = false;
                        ReturnPhysicalAddress = false;
                    }
                }
            }

            writeCacheLevel = -1;
            Interlocked.MemoryBarrier();

            BulkAllocate(); // null pointer
        }

        /// <summary>
        /// Get physical address
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long address)
        {
            if (ReturnPhysicalAddress)
            {
                return address;
            }
            else
            {
                return
                    (long)pointers[address >> PageSizeBits]
                  + (long)(address & PageSizeMask) * RecordSize;
            }
        }

        /// <summary>
        /// Get object
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref T Get(long index)
        {
            if (this.ReturnPhysicalAddress)
                throw new FasterException("Physical pointer returned by allocator: de-reference pointer to get records instead of calling Get");

            return ref values
                [index >> PageSizeBits]
                [index & PageSizeMask];
        }


        /// <summary>
        /// Set object
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long index, ref T value)
        {
            if (this.ReturnPhysicalAddress)
                throw new FasterException("Physical pointer returned by allocator: de-reference pointer to set records instead of calling Set (otherwise, set ForceUnpinnedAllocation to true)");

            values
                [index >> PageSizeBits]
                [index & PageSizeMask]
                = value;
        }



        /// <summary>
        /// Free object
        /// </summary>
        /// <param name="pointer"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(long pointer)
        {
            if (!ReturnPhysicalAddress)
            {
                values[pointer >> PageSizeBits][pointer & PageSizeMask] = default;
            }

            freeList.Enqueue(pointer);
        }

        private const int kAllocateChunkSize = 16;

        /// <summary>
        /// Warning: cannot mix 'n' match use of
        /// Allocate and BulkAllocate
        /// </summary>
        /// <returns></returns>
        public long BulkAllocate()
        {
            // Determine insertion index.
            // ReSharper disable once CSharpWarnings::CS0420
#pragma warning disable 420
            int index = Interlocked.Add(ref count, kAllocateChunkSize) - kAllocateChunkSize;
#pragma warning restore 420

            int offset = index & PageSizeMask;
            int baseAddr = index >> PageSizeBits;

            // Handle indexes in first batch specially because they do not use write cache.
            if (baseAddr == 0)
            {
                // If index 0, then allocate space for next level.
                if (index == 0)
                {
                    var tmp = new T[PageSize];
#if !(CALLOC)
                    Array.Clear(tmp, 0, PageSize);
#endif

                    if (IsPinned)
                    {
                        handles[1] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                        pointers[1] = handles[1].AddrOfPinnedObject();
                    }
                    values[1] = tmp;
                    Interlocked.MemoryBarrier();
                }

                // Return location.
                if (ReturnPhysicalAddress)
                    return (((long)pointers[0]) + index * RecordSize);
                else
                    return index;
            }

            // See if write cache contains corresponding array.
            var cache = writeCacheLevel;
            T[] array;

            if (cache != -1)
            {
                // Write cache is correct array only if index is within [arrayCapacity, 2*arrayCapacity).
                if (cache == baseAddr)
                {
                    // Return location.
                    if (ReturnPhysicalAddress)
                        return ((long)pointers[baseAddr]) + (long)offset * RecordSize;
                    else
                        return index;
                }
            }

            // Write cache did not work, so get level information from index.
            // int level = GetLevelFromIndex(index);

            // Spin-wait until level has an allocated array.
            var spinner = new SpinWait();
            while (true)
            {
                array = values[baseAddr];
                if (array != null)
                {
                    break;
                }
                spinner.SpinOnce();
            }

            // Perform extra actions if inserting at offset 0 of level.
            if (offset == 0)
            {
                // Update write cache to point to current level.
                writeCacheLevel = baseAddr;
                Interlocked.MemoryBarrier();

                // Allocate for next page
                int newBaseAddr = baseAddr + 1;
                var tmp = new T[PageSize];

#if !(CALLOC)
                    Array.Clear(tmp, 0, PageSize);
#endif

                if (IsPinned)
                {
                    handles[newBaseAddr] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                    pointers[newBaseAddr] = handles[newBaseAddr].AddrOfPinnedObject();
                }
                values[newBaseAddr] = tmp;

                Interlocked.MemoryBarrier();
            }

            // Return location.
            if (ReturnPhysicalAddress)
                return ((long)pointers[baseAddr]) + (long)offset * RecordSize;
            else
                return index;
        }

        /// <summary>
        /// Allocate
        /// </summary>
        /// <returns></returns>
        public long Allocate()
        {
            if (freeList.TryDequeue(out long result))
                return result;

            // Determine insertion index.
            // ReSharper disable once CSharpWarnings::CS0420
#pragma warning disable 420
            int index = Interlocked.Increment(ref count) - 1;
#pragma warning restore 420

            int offset = index & PageSizeMask;
            int baseAddr = index >> PageSizeBits;

            // Handle indexes in first batch specially because they do not use write cache.
            if (baseAddr == 0)
            {
                // If index 0, then allocate space for next level.
                if (index == 0)
                {
                    var tmp = new T[PageSize];

#if !(CALLOC)
                    Array.Clear(tmp, 0, PageSize);
#endif

                    if (IsPinned)
                    {
                        handles[1] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                        pointers[1] = handles[1].AddrOfPinnedObject();
                    }
                    values[1] = tmp;
                    Interlocked.MemoryBarrier();
                }

                // Return location.
                if (ReturnPhysicalAddress)
                    return ((long)pointers[0]) + index * RecordSize;
                else
                    return index;
            }

            // See if write cache contains corresponding array.
            var cache = writeCacheLevel;
            T[] array;

            if (cache != -1)
            {
                // Write cache is correct array only if index is within [arrayCapacity, 2*arrayCapacity).
                if (cache == baseAddr)
                {
                    // Return location.
                    if (ReturnPhysicalAddress)
                        return ((long)pointers[baseAddr]) + (long)offset * RecordSize;
                    else
                        return index;
                }
            }

            // Write cache did not work, so get level information from index.
            // int level = GetLevelFromIndex(index);

            // Spin-wait until level has an allocated array.
            var spinner = new SpinWait();
            while (true)
            {
                array = values[baseAddr];
                if (array != null)
                {
                    break;
                }
                spinner.SpinOnce();
            }

            // Perform extra actions if inserting at offset 0 of level.
            if (offset == 0)
            {
                // Update write cache to point to current level.
                writeCacheLevel = baseAddr;
                Interlocked.MemoryBarrier();

                // Allocate for next page
                int newBaseAddr = baseAddr + 1;
                var tmp = new T[PageSize];

#if !(CALLOC)
                    Array.Clear(tmp, 0, PageSize);
#endif

                if (IsPinned)
                {
                    handles[newBaseAddr] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                    pointers[newBaseAddr] = handles[newBaseAddr].AddrOfPinnedObject();
                }
                values[newBaseAddr] = tmp;

                Interlocked.MemoryBarrier();
            }

            // Return location.
            if (ReturnPhysicalAddress)
                return ((long)pointers[baseAddr]) + (long)offset * RecordSize;
            else
                return index;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (IsPinned && (handles[i].IsAllocated)) handles[i].Free();
                values[i] = null;
            }
            handles = null;
            pointers = null;
            values = null;
            count = 0;
            freeList = null;
        }


        #region Checkpoint

        /// <summary>
        /// Public facing persistence API
        /// </summary>
        /// <param name="device"></param>
        /// <param name="start_offset"></param>
        /// <param name="numBytes"></param>
        public void TakeCheckpoint(IDevice device, ulong start_offset, out ulong numBytes)
        {
            BeginCheckpoint(device, start_offset, out numBytes);
        }

        /// <summary>
        /// Is checkpoint complete
        /// </summary>
        /// <returns></returns>
        public bool IsCheckpointCompleted()
        {
            return checkpointCallbackCount == 0;
        }

        /// <summary>
        /// Is checkpoint completed
        /// </summary>
        /// <returns></returns>
        public async ValueTask IsCheckpointCompletedAsync(CancellationToken token = default)
        {
            await checkpointSemaphore.WaitAsync(token);
            checkpointSemaphore.Release();
        }

        internal unsafe void BeginCheckpoint(IDevice device, ulong offset, out ulong numBytesWritten)
        {
            int localCount = count;
            int recordsCountInLastLevel = localCount & PageSizeMask;
            int numCompleteLevels = localCount >> PageSizeBits;
            int numLevels = numCompleteLevels + (recordsCountInLastLevel > 0 ? 1 : 0);
            checkpointCallbackCount = numLevels;
            checkpointSemaphore = new SemaphoreSlim(0);
            uint alignedPageSize = PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;


            int sectorSize = (int)device.SectorSize;
            numBytesWritten = 0;
            for (int i = 0; i < numLevels; i++)
            {
                OverflowPagesFlushAsyncResult result = default;
                uint writeSize = (uint)((i == numCompleteLevels) ? (lastLevelSize + (sectorSize - 1)) & ~(sectorSize - 1) : alignedPageSize);

                device.WriteAsync(pointers[i], offset + numBytesWritten, writeSize, AsyncFlushCallback, result);
                numBytesWritten += writeSize;
            }
        }

        private unsafe void AsyncFlushCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            try
            {
                if (errorCode != 0)
                {
                    System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Completion Callback error, {0}", ex.Message);
            }
            finally
            {
                if (Interlocked.Decrement(ref checkpointCallbackCount) == 0)
                {
                    checkpointSemaphore.Release();
                }
                Overlapped.Free(overlap);
            }
        }

        /// <summary>
        /// Max valid address
        /// </summary>
        /// <returns></returns>
        public int GetMaxValidAddress()
        {
            return count;
        }

        /// <summary>
        /// Get page size
        /// </summary>
        /// <returns></returns>
        public int GetPageSize()
        {
            return PageSize;
        }
        #endregion

        #region Recover
        /// <summary>
        /// Recover
        /// </summary>
        /// <param name="device"></param>
        /// <param name="buckets"></param>
        /// <param name="numBytes"></param>
        /// <param name="offset"></param>
        public void Recover(IDevice device, ulong offset, int buckets, ulong numBytes)
        {
            BeginRecovery(device, offset, buckets, numBytes, out _);
        }

        /// <summary>
        /// Check if recovery complete
        /// </summary>
        /// <param name="waitUntilComplete"></param>
        /// <returns></returns>
        public bool IsRecoveryCompleted(bool waitUntilComplete = false)
        {
            bool completed = (numLevelsToBeRecovered == 0);
            if (!completed && waitUntilComplete)
            {
                while (numLevelsToBeRecovered != 0)
                {
                    Thread.Sleep(10);
                }
            }
            return completed;
        }

        // Implementation of asynchronous recovery
        private int numLevelsToBeRecovered;

        internal unsafe void BeginRecovery(IDevice device,
                                    ulong offset,
                                    int buckets,
                                    ulong numBytesToRead,
                                    out ulong numBytesRead)
        {
            // Allocate as many records in memory
            while (count < buckets)
            {
                Allocate();
            }

            int numRecords = (int)numBytesToRead / RecordSize;
            int recordsCountInLastLevel = numRecords & PageSizeMask;
            int numCompleteLevels = numRecords >> PageSizeBits;
            int numLevels = numCompleteLevels + (recordsCountInLastLevel > 0 ? 1 : 0);

            numLevelsToBeRecovered = numLevels;

            numBytesRead = 0;
            uint alignedPageSize = (uint)PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;
            for (int i = 0; i < numLevels; i++)
            {
                //read a full page
                uint length = (uint)PageSize * (uint)RecordSize; ;
                OverflowPagesReadAsyncResult result = default;
                device.ReadAsync(offset + numBytesRead, pointers[i], length, AsyncPageReadCallback, result);
                numBytesRead += (i == numCompleteLevels) ? lastLevelSize : alignedPageSize;
            }
        }

        private unsafe void AsyncPageReadCallback(
                                    uint errorCode,
                                    uint numBytes,
                                    NativeOverlapped* overlap)
        {
            try
            {
                if (errorCode != 0)
                {
                    System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Completion Callback error, {0}", ex.Message);
            }
            finally
            {
                Interlocked.Decrement(ref numLevelsToBeRecovered);
                Overlapped.Free(overlap);
            }
        }
        #endregion
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.IO;

namespace FASTER.core
{
    public unsafe class MallocFixedPageSize<T>
    {
        public static bool ForceUnpinnedAllocation = false;

        public static MallocFixedPageSize<T> Instance = new MallocFixedPageSize<T>();
        public static MallocFixedPageSize<T> PhysicalInstance = new MallocFixedPageSize<T>(true);

        protected const int PageSizeBits = 16;
        internal const int PageSize = 1 << PageSizeBits;
        protected const int PageSizeMask = PageSize - 1;
        protected const int LevelSizeBits = 18;
        protected const int LevelSize = 1 << LevelSizeBits;
        protected const int LevelSizeMask = LevelSize - 1;

        protected T[][] values = new T[LevelSize][];
        protected GCHandle[] handles = new GCHandle[LevelSize];
        protected IntPtr[] pointers = new IntPtr[LevelSize];

        protected T[] values0;
        protected GCHandle handles0;
        protected IntPtr pointers0;
        protected readonly int RecordSize;
        protected readonly int AlignedPageSize;

        protected volatile int writeCacheLevel;

        protected volatile int count;

        public readonly bool IsPinned;
        public readonly bool ReturnPhysicalAddress;

        [ThreadStatic]
        public static Queue<FreeItem> freeList;
#if DEBUG
        public ConcurrentBag<Queue<FreeItem>> allQueues = new ConcurrentBag<Queue<FreeItem>>();
#endif
        public MallocFixedPageSize(bool returnPhysicalAddress = false)
        {
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
                    try
                    {
                        handles[0] = GCHandle.Alloc(values[0], GCHandleType.Pinned);
                        pointers[0] = handles[0].AddrOfPinnedObject();
                        handles0 = handles[0];
                        pointers0 = pointers[0];
                        RecordSize = Marshal.SizeOf(values[0][0]);
                        AlignedPageSize = RecordSize * PageSize;
                    }
                    catch (Exception)
                    {
                        IsPinned = false;
                        ReturnPhysicalAddress = false;
                    }
                }
            }

            values0 = values[0];
            writeCacheLevel = -1;
            Interlocked.MemoryBarrier();

            BulkAllocate(); // null pointer
        }

        public void ReInitialize()
        {
            values = new T[LevelSize][];
            handles = new GCHandle[LevelSize];
            pointers = new IntPtr[LevelSize];
            values[0] = new T[PageSize];


#if !(CALLOC)
            Array.Clear(values[0], 0, PageSize);
#endif

            if (IsPinned)
            {
                handles[0] = GCHandle.Alloc(values[0], GCHandleType.Pinned);
                pointers[0] = handles[0].AddrOfPinnedObject();
                handles0 = handles[0];
                pointers0 = pointers[0];
            }

            values0 = values[0];
            writeCacheLevel = -1;
            Interlocked.MemoryBarrier();

            BulkAllocate(); // null pointer
        }

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref T Get(long index)
        {
            if (this.ReturnPhysicalAddress)
                throw new Exception("Physical pointer returned by allocator: de-reference pointer to get records instead of calling Get");

            return ref values
                [index >> PageSizeBits]
                [index & PageSizeMask];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long index, ref T value)
        {
            if (this.ReturnPhysicalAddress)
                throw new Exception("Physical pointer returned by allocator: de-reference pointer to set records instead of calling Set (otherwise, set ForceUnpinnedAllocation to true)");

            values
                [index >> PageSizeBits]
                [index & PageSizeMask]
                = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long index, T value)
        {
            Set(index, ref value);
        }

        //static long _freed = 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FreeAtEpoch(long pointer, int removed_epoch = -1)
        {
            //if (Interlocked.Increment(ref _freed) % 100000 == 0)
            //{
            //    Console.WriteLine("Freed " + _freed);
            //}

            if (!ReturnPhysicalAddress)
            {
                values[pointer >> PageSizeBits][pointer & PageSizeMask] = default(T);
            }
            if (freeList == null) freeList = new Queue<FreeItem>();
            freeList.Enqueue(new FreeItem { removed_item = pointer, removal_epoch = removed_epoch });
        }

#if DEBUG
        public int TotalFreeCount()
        {
            int result = 0;
            var x = allQueues.ToArray();
            foreach (var q in x)
            {
                result += q.Count;
            }
            return result;
        }

        public int TotalUsedPointers()
        {
            return count - TotalFreeCount();
        }
#endif
        public const int kAllocateChunkSize = 16;


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
                    return (((long)pointers0) + index * RecordSize);
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

        //static long _allocated = 0;
        public long Allocate()
        {
            //if (Interlocked.Increment(ref _allocated) % 100000 == 0)
            //{
            //    Console.WriteLine("Allocated " + _allocated);
            //}

            if (freeList == null)
            {
                freeList = new Queue<FreeItem>();
#if DEBUG
                allQueues.Add(freeList);
#endif
            }
            if (freeList.Count > 0)
            {
                if (freeList.Peek().removal_epoch <= LightEpoch.Instance.SafeToReclaimEpoch)
                    return freeList.Dequeue().removed_item;

                //if (freeList.Count % 64 == 0)
                //    LightEpoch.Instance.BumpCurrentEpoch();
            }

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
                    return ((long)pointers0) + index * RecordSize;
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
            values0 = null;
            count = 0;
        }

        public int GetMaxAllocated()
        {
            return count;
        }

        #region Checkpoint

        // Public facing persistence API
        public void TakeCheckpoint(IDevice device, out ulong numBytes)
        {
            begin_checkpoint(device, 0UL, out numBytes);
        }

        public bool IsCheckpointCompleted(bool waitUntilComplete = false)
        {
            bool completed = checkpointEvent.IsSet;
            if (!completed && waitUntilComplete)
            {
                checkpointEvent.Wait();
                return true;
            }
            return completed;
        }

        // Implementation of an asynchronous checkpointing scheme
        protected CountdownEvent checkpointEvent;

        internal void begin_checkpoint(IDevice device, ulong offset, out ulong numBytesWritten)
        {
            int localCount = count;
            int recordsCountInLastLevel = localCount & PageSizeMask;
            int numCompleteLevels = localCount >> PageSizeBits;
            int numLevels = numCompleteLevels + (recordsCountInLastLevel > 0 ? 1 : 0);
            checkpointEvent = new CountdownEvent(numLevels);

            uint alignedPageSize = PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;

            numBytesWritten = 0;
            for (int i = 0; i < numLevels; i++)
            {
                OverflowPagesFlushAsyncResult result = default(OverflowPagesFlushAsyncResult);
                device.WriteAsync(pointers[i], offset + numBytesWritten, alignedPageSize, async_flush_callback, result);
                numBytesWritten += (i == numCompleteLevels) ? lastLevelSize : alignedPageSize;
            }
        }

        private void async_flush_callback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
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
                checkpointEvent.Signal();
            }
        }

        public int GetMaxValidAddress()
        {
            return count;
        }
        #endregion

        #region Recover
        public void Recover(string filename, int buckets, ulong numBytes)
        {
            Recover(new LocalStorageDevice(filename, false, false, true), buckets, numBytes);
        }

        public void Recover(IDevice device, int buckets, ulong numBytes)
        {
            begin_recovery(device, 0UL, buckets, numBytes, out ulong numBytesRead);
        }

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

        internal void begin_recovery(IDevice device,
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
                OverflowPagesReadAsyncResult result = default(OverflowPagesReadAsyncResult);
                device.ReadAsync(offset + numBytesRead, pointers[i], length, async_page_read_callback, result);
                numBytesRead += (i == numCompleteLevels) ? lastLevelSize : alignedPageSize;
            }
        }

        private void async_page_read_callback(
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
            }
        }
        #endregion
    }

    public struct FreeItem
    {
        public long removed_item;
        public int removal_epoch;
    }
}

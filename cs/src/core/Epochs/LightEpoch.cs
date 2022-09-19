// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace FASTER.core
{
    /// <summary>
    /// Epoch protection
    /// </summary>
    public unsafe sealed class LightEpoch
    {
        private const int kCacheLineBytes = 64;

        /// <summary>
        /// Default invalid index entry.
        /// </summary>
        const int kInvalidIndex = 0;

        /// <summary>
        /// Default number of entries in the entries table
        /// </summary>
        static readonly ushort kTableSize = Math.Max((ushort)128, (ushort)(Environment.ProcessorCount * 2));

        /// <summary>
        /// Default drainlist size
        /// </summary>
        const int kDrainListSize = 16;

        /// <summary>
        /// Thread protection status entries.
        /// </summary>
        Entry[] tableRaw;
        Entry* tableAligned;
#if !NET5_0_OR_GREATER
        GCHandle tableHandle;
#endif

        static readonly Entry[] threadIndex;
        static readonly Entry* threadIndexAligned;
#if !NET5_0_OR_GREATER
        static GCHandle threadIndexHandle;
#endif

        /// <summary>
        /// List of action, epoch pairs containing actions to be performed when an epoch becomes safe to reclaim.
        /// Marked volatile to ensure latest value is seen by the last suspended thread.
        /// </summary>
        volatile int drainCount = 0;
        readonly EpochActionPair[] drainList = new EpochActionPair[kDrainListSize];

        /// <summary>
        /// A thread's entry in the epoch table.
        /// </summary>
        [ThreadStatic]
        static int threadEntryIndex;

        /// <summary>
        /// Number of instances using this entry
        /// </summary>
        [ThreadStatic]
        static int threadEntryIndexCount;

        /// <summary>
        /// Managed thread id of this thread
        /// </summary>
        [ThreadStatic]
        static int threadId;

        /// <summary>
        /// Start offset to reserve entry in the epoch table
        /// </summary>
        [ThreadStatic]
        static ushort startOffset1;

        /// <summary>
        /// Alternate start offset to reserve entry in the epoch table (to reduce probing if <see cref="startOffset1"/> slot is already filled)
        /// </summary>
        [ThreadStatic]
        static ushort startOffset2;

        /// <summary>
        /// Global current epoch value
        /// </summary>
        public long CurrentEpoch;

        /// <summary>
        /// Cached value of latest epoch that is safe to reclaim
        /// </summary>
        public long SafeToReclaimEpoch;

        /// <summary>
        /// Local view of current epoch, for an epoch-protected thread
        /// </summary>
        public long LocalCurrentEpoch => (*(tableAligned + threadEntryIndex)).localCurrentEpoch;

        /// <summary>
        /// Static constructor to setup shared cache-aligned space
        /// to store per-entry count of instances using that entry
        /// </summary>
        static LightEpoch()
        {
            long p;

            // Over-allocate to do cache-line alignment
#if NET5_0_OR_GREATER
            threadIndex = GC.AllocateArray<Entry>(kTableSize + 2, true);
            p = (long)Unsafe.AsPointer(ref threadIndex[0]);
#else
            threadIndex = new Entry[kTableSize + 2];
            threadIndexHandle = GCHandle.Alloc(threadIndex, GCHandleType.Pinned);
            p = (long)threadIndexHandle.AddrOfPinnedObject();
#endif
            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
            threadIndexAligned = (Entry*)p2;
        }

        /// <summary>
        /// Instantiate the epoch table
        /// </summary>
        public LightEpoch()
        {
            long p;

#if NET5_0_OR_GREATER
            tableRaw = GC.AllocateArray<Entry>(kTableSize + 2, true);
            p = (long)Unsafe.AsPointer(ref tableRaw[0]);
#else
            // Over-allocate to do cache-line alignment
            tableRaw = new Entry[kTableSize + 2];
            tableHandle = GCHandle.Alloc(tableRaw, GCHandleType.Pinned);
            p = (long)tableHandle.AddrOfPinnedObject();
#endif
            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
            tableAligned = (Entry*)p2;

            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;

            // Mark all epoch table entries as "available"
            for (int i = 0; i < kDrainListSize; i++)
                drainList[i].epoch = long.MaxValue;
            drainCount = 0;
        }

        /// <summary>
        /// Clean up epoch table
        /// </summary>
        public void Dispose()
        {
#if !NET5_0_OR_GREATER
            tableHandle.Free();
#endif
            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;
        }

        /// <summary>
        /// Check whether current epoch instance is protected on this thread
        /// </summary>
        /// <returns>Result of the check</returns>
        public bool ThisInstanceProtected()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex != entry)
            {
                if ((*(tableAligned + entry)).threadId == entry)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Enter the thread into the protected code region
        /// </summary>
        /// <returns>Current epoch</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProtectAndDrain()
        {
            int entry = threadEntryIndex;

            // Protect CurrentEpoch by making an entry for it in the non-static epoch table so ComputeNewSafeToReclaimEpoch() will see it.
            (*(tableAligned + entry)).threadId = threadEntryIndex;
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }
        }

        /// <summary>
        /// Thread suspends its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend()
        {
            Release();
            if (drainCount > 0) SuspendDrain();
        }

        /// <summary>
        /// Thread resumes its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume()
        {
            Acquire();
            ProtectAndDrain();
        }

        /// <summary>
        /// Increment current epoch and associate trigger action
        /// with the prior epoch
        /// </summary>
        /// <param name="onDrain">Trigger action</param>
        /// <returns></returns>
        public void BumpCurrentEpoch(Action onDrain)
        {
            long PriorEpoch = BumpCurrentEpoch() - 1;

            int i = 0;
            while (true)
            {
                if (drainList[i].epoch == long.MaxValue)
                {
                    // This was an empty slot. If it still is, assign this action/epoch to the slot.
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, long.MaxValue) == long.MaxValue)
                    {
                        drainList[i].action = onDrain;
                        drainList[i].epoch = PriorEpoch;
                        Interlocked.Increment(ref drainCount);
                        break;
                    }
                }
                else
                {
                    var triggerEpoch = drainList[i].epoch;

                    if (triggerEpoch <= SafeToReclaimEpoch)
                    {
                        // This was a slot with an epoch that was safe to reclaim. If it still is, execute its trigger, then assign this action/epoch to the slot.
                        if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, triggerEpoch) == triggerEpoch)
                        {
                            var triggerAction = drainList[i].action;
                            drainList[i].action = onDrain;
                            drainList[i].epoch = PriorEpoch;
                            triggerAction();
                            break;
                        }
                    }
                }

                if (++i == kDrainListSize)
                {
                    // We are at the end of the drain list and found no empty or reclaimable slot. ProtectAndDrain, which should clear one or more slots.
                    ProtectAndDrain();
                    i = 0;
                    Thread.Yield();
                }
            }

            // Now ProtectAndDrain, which may execute the action we just added.
            ProtectAndDrain();
        }

        /// <summary>
        /// Mechanism for threads to mark some activity as completed until
        /// some version by this thread
        /// </summary>
        /// <param name="markerIdx">ID of activity</param>
        /// <param name="version">Version</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Mark(int markerIdx, long version)
        {
            Debug.Assert(markerIdx < 6);

            (*(tableAligned + threadEntryIndex)).markers[markerIdx] = version;
        }

        /// <summary>
        /// Check if all active threads have completed the some
        /// activity until given version.
        /// </summary>
        /// <param name="markerIdx">ID of activity</param>
        /// <param name="version">Version</param>
        /// <returns>Whether complete</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CheckIsComplete(int markerIdx, long version)
        {
            Debug.Assert(markerIdx < 6);

            // check if all threads have reported complete
            for (int index = 1; index <= kTableSize; ++index)
            {
                long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                long fc_version = (*(tableAligned + index)).markers[markerIdx];
                if (0 != entry_epoch)
                {
                    if ((fc_version != version) && (entry_epoch < long.MaxValue))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Increment global current epoch
        /// </summary>
        /// <returns></returns>
        long BumpCurrentEpoch()
        {
            long nextEpoch = Interlocked.Increment(ref CurrentEpoch);

            if (drainCount > 0)
                Drain(nextEpoch);

            return nextEpoch;
        }

        /// <summary>
        /// Looks at all threads and return the latest safe epoch
        /// </summary>
        /// <param name="currentEpoch">Current epoch</param>
        /// <returns>Safe epoch</returns>
        long ComputeNewSafeToReclaimEpoch(long currentEpoch)
        {
            long oldestOngoingCall = currentEpoch;

            for (int index = 1; index <= kTableSize; ++index)
            {
                long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                if (0 != entry_epoch)
                {
                    if (entry_epoch < oldestOngoingCall)
                    {
                        oldestOngoingCall = entry_epoch;
                    }
                }
            }

            // The latest safe epoch is the one just before the earliest unsafe epoch.
            SafeToReclaimEpoch = oldestOngoingCall - 1;
            return SafeToReclaimEpoch;
        }

        /// <summary>
        /// Take care of pending drains after epoch suspend
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SuspendDrain()
        {
            while (drainCount > 0)
            {
                // Barrier ensures we see the latest epoch table entries. Ensures
                // that the last suspended thread drains all pending actions.
                Thread.MemoryBarrier();
                for (int index = 1; index <= kTableSize; ++index)
                {
                    long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                    if (0 != entry_epoch)
                    {
                        return;
                    }
                }
                Resume();
                Release();
            }
        }

        /// <summary>
        /// Check and invoke trigger actions that are ready
        /// </summary>
        /// <param name="nextEpoch">Next epoch</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Drain(long nextEpoch)
        {
            ComputeNewSafeToReclaimEpoch(nextEpoch);

            for (int i = 0; i < kDrainListSize; i++)
            {
                var trigger_epoch = drainList[i].epoch;

                if (trigger_epoch <= SafeToReclaimEpoch)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, trigger_epoch) == trigger_epoch)
                    {
                        // Store off the trigger action, then set epoch to int.MaxValue to mark this slot as "available for use".
                        var trigger_action = drainList[i].action;
                        drainList[i].action = null;
                        drainList[i].epoch = long.MaxValue;
                        Interlocked.Decrement(ref drainCount);

                        // Execute the action
                        trigger_action();
                        if (drainCount == 0) break;
                    }
                }
            }
        }

        /// <summary>
        /// Thread acquires its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Acquire()
        {
            if (threadEntryIndex == kInvalidIndex)
                threadEntryIndex = ReserveEntryForThread();

            Debug.Assert((*(tableAligned + threadEntryIndex)).localCurrentEpoch == 0,
                "Trying to acquire protected epoch. Make sure you do not re-enter FASTER from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // This corresponds to AnyInstanceProtected(). We do not mark "ThisInstanceProtected" until ProtectAndDrain().
            threadEntryIndexCount++;
        }

        /// <summary>
        /// Thread releases its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Release()
        {
            int entry = threadEntryIndex;

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch != 0,
                "Trying to release unprotected epoch. Make sure you do not re-enter FASTER from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // Clear "ThisInstanceProtected()" (non-static epoch table)
            (*(tableAligned + entry)).localCurrentEpoch = 0;
            (*(tableAligned + entry)).threadId = 0;

            // Decrement "AnyInstanceProtected()" (static thread table)
            threadEntryIndexCount--;
            if (threadEntryIndexCount == 0)
            {
                (threadIndexAligned + threadEntryIndex)->threadId = 0;
                threadEntryIndex = kInvalidIndex;
            }
        }

        /// <summary>
        /// Reserve entry for thread. This method relies on the fact that no
        /// thread will ever have ID 0.
        /// </summary>
        /// <returns>Reserved entry</returns>
        static int ReserveEntry()
        {
            while (true)
            {
                // Try to acquire entry
                if (0 == (threadIndexAligned + startOffset1)->threadId)
                {
                    if (0 == Interlocked.CompareExchange(
                        ref (threadIndexAligned + startOffset1)->threadId,
                        threadId, 0))
                        return startOffset1;
                }

                if (startOffset2 > 0)
                {
                    // Try alternate entry
                    startOffset1 = startOffset2;
                    startOffset2 = 0;
                }
                else startOffset1++; // Probe next sequential entry
                if (startOffset1 > kTableSize)
                {
                    startOffset1 -= kTableSize;
                    Thread.Yield();
                }
            }
        }

        /// <summary>
        /// A 32-bit murmur3 implementation.
        /// </summary>
        /// <param name="h"></param>
        /// <returns></returns>
        private static int Murmur3(int h)
        {
            uint a = (uint)h;
            a ^= a >> 16;
            a *= 0x85ebca6b;
            a ^= a >> 13;
            a *= 0xc2b2ae35;
            a ^= a >> 16;
            return (int)a;
        }

        /// <summary>
        /// Allocate a new entry in epoch table. This is called 
        /// once for a thread.
        /// </summary>
        /// <returns>Reserved entry</returns>
        static int ReserveEntryForThread()
        {
            if (threadId == 0) // run once per thread for performance
            {
                threadId = Environment.CurrentManagedThreadId;
                uint code = (uint)Murmur3(threadId);
                startOffset1 = (ushort)(1 + (code % kTableSize));
                startOffset2 = (ushort)(1 + ((code >> 16) % kTableSize));
            }
            return ReserveEntry();
        }

        /// <summary>
        /// Epoch table entry (cache line size).
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = kCacheLineBytes)]
        struct Entry
        {
            /// <summary>
            /// Thread-local value of epoch
            /// </summary>
            [FieldOffset(0)]
            public long localCurrentEpoch;

            /// <summary>
            /// ID of thread associated with this entry.
            /// </summary>
            [FieldOffset(8)]
            public int threadId;

            [FieldOffset(12)]
            public int reentrant;

            [FieldOffset(16)]
            public fixed long markers[6];

            public override string ToString() => $"lce = {localCurrentEpoch}, tid = {threadId}, re-ent {reentrant}";
        }
        struct EpochActionPair
        {
            public long epoch;
            public Action action;

            public override string ToString() => $"epoch = {epoch}, action = {(action is null ? "n/a" : action.Method.ToString())}";
        }
    }
}
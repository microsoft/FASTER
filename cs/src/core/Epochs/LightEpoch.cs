// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define COUNT_ACTIVE_THREADS

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace FASTER.core
{

    public unsafe class LightEpoch
    {
        public static LightEpoch Instance = new LightEpoch();

        /// <summary>
        /// Default invalid index entry.
        /// </summary>
        public const int kInvalidIndex = 0;
        /// <summary>
        /// Default number of entries in the entries table
        /// </summary>
        private const int kTableSize = 128;
        /// <summary>
        /// Default drainlist size
        /// </summary>
        private const int kDrainListSize = 16;

        /// <summary>
        /// Thread protection status entries. Threads lock entries the
        /// first time the call Protect() (see reserveEntryForThread()).
        /// See documentation for the fields to specifics of how threads
        /// use their Entries to guarantee memory-stability.
        /// </summary>
        private Entry[] tableRaw;
        private GCHandle tableHandle;
        private Entry* tableAligned;

        /// <summary>
        /// List of action, epoch pairs containing actions to performed when an epoch becomes safe to reclaim.
        /// </summary>
        private int drainCount = 0;
        private EpochActionPair[] drainList = new EpochActionPair[kDrainListSize];

        /// <summary>
        /// The number of entries in tableAligned. Currently, this is fixed after
        /// Initialize() and never changes or grows. If tableAligned runs out
        /// of entries, then the current implementation will deadlock threads.
        /// </summary>
        private int numEntries;

        /// <summary>
        /// A thread's entry in the epoch table.
        /// </summary>
        [ThreadStatic]
        public static int threadEntryIndex;

        public int CurrentNumThreads;

        /**
         * A notion of time for objects that are removed from data structures.
         * Objects in data structures are timestamped with this Epoch just after
         * they have been (sequentially consistently) "unlinked" from a structure.
         * Threads also use this Epoch to mark their entry into a protected region
         * (also in sequentially consistent way). While a thread operates in this
         * region "unlinked" items that they may be accessing will not be reclaimed.
         */
        public int CurrentEpoch;

        /**
         * Caches the most recent result of ComputeNewSafeToReclaimEpoch() so
         * that fast decisions about whether an object can be reused or not
         * (in IsSafeToReclaim()). Effectively, this is periodically computed
         * by taking the minimum of the protected Epochs in #m_epochTable and
         * #m_currentEpoch.
         */
        public int SafeToReclaimEpoch;

        public LightEpoch(int size = kTableSize)
        {
            Initialize(size);
        }

        ~LightEpoch()
        {
            Uninitialize();
        }

        unsafe void Initialize(int size)
        {
            CurrentNumThreads = 0;
            numEntries = size;

            // over-allocate to do cache-line alignment
            tableRaw = new Entry[size + 2];
            tableHandle = GCHandle.Alloc(tableRaw, GCHandleType.Pinned);
            long p = (long)tableHandle.AddrOfPinnedObject();
            
            // force the pointer to align to 64-byte boundaries
            long p2 = (p + (Constants.kCacheLineBytes - 1)) & ~(Constants.kCacheLineBytes - 1);
            tableAligned = (Entry*)p2;

            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;

            for (int i = 0; i < kDrainListSize; i++)
                drainList[i].epoch = int.MaxValue;
            drainCount = 0;
        }

        void Uninitialize()
        {
            tableHandle.Free();
            tableAligned = null;
            tableRaw = null;

            numEntries = 0;
            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;
        } 

        /// <summary>
        /// Enter the thread into the protected code region, which guarantees
        /// pointer stability for records in client data structures. After this
        /// call, accesses to protected data structure items are guaranteed to be
        /// safe, even if the item is concurrently removed from the structure.
        ///
        /// Behavior is undefined if Protect() is called from an already
        /// protected thread. Upon creation, threads are unprotected.
        /// </summary>
        /// <param name="current_epoch">
        /// A sequentially consistent snapshot of the current
        /// global epoch. It is okay that this may be stale by the time it
        /// actually gets entered into the table.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Protect()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex == entry)
            {
                entry = ReserveEntryForThread();
                threadEntryIndex = entry;
            }

            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;
            return (*(tableAligned + entry)).localCurrentEpoch;
        }

        public bool IsProtected()
        {
            return (kInvalidIndex != threadEntryIndex);
        }

        public int GetThreadEpoch()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex == entry)
            {
                entry = ReserveEntryForThread();
                threadEntryIndex = entry;
            }
            return (*(tableAligned + entry)).localCurrentEpoch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ProtectAndDrain()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex == entry)
            {
                entry = ReserveEntryForThread();
                threadEntryIndex = entry;
            }
    
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }

            return (*(tableAligned + entry)).localCurrentEpoch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReentrantProtect()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex == entry)
            {
                entry = ReserveEntryForThread();
                threadEntryIndex = entry;
            }

            if ((*(tableAligned + entry)).localCurrentEpoch != 0)
                return (*(tableAligned + entry)).localCurrentEpoch;

            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;
            (*(tableAligned + entry)).reentrant++;
            return (*(tableAligned + entry)).localCurrentEpoch;
        }

        /// <summary>
        /// Exit the thread from the protected code region. The thread must
        /// promise not to access pointers to elements in the protected data
        /// structures beyond this call.
        ///
        /// Behavior is undefined if Unprotect() is called from an already
        /// unprotected thread.
        /// </summary>
        /// <param name="current_epoch"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unprotect()
        {
            (*(tableAligned+threadEntryIndex)).localCurrentEpoch = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReentrantUnprotect()
        {
            if (--((*(tableAligned + threadEntryIndex)).reentrant) == 0)
                (*(tableAligned + threadEntryIndex)).localCurrentEpoch = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain(int nextEpoch)
        {
            ComputeNewSafeToReclaimEpoch(nextEpoch);

            for (int i = 0; i < kDrainListSize; i++)
            {
                var trigger_epoch = drainList[i].epoch;

                if (trigger_epoch <= SafeToReclaimEpoch)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, int.MaxValue - 1, trigger_epoch) == trigger_epoch)
                    {
                        var trigger_action = drainList[i].action;
                        drainList[i].action = null;
                        drainList[i].epoch = int.MaxValue;
                        trigger_action();
                        if (Interlocked.Decrement(ref drainCount) == 0) break;
                    }
                }
            }
        }

        public void Release()
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex == entry)
            {
                return;
            }

#if COUNT_ACTIVE_THREADS
            Interlocked.Decrement(ref CurrentNumThreads);
#endif
            threadEntryIndex = kInvalidIndex;
            (*(tableAligned + entry)).localCurrentEpoch = 0;
            (*(tableAligned + entry)).threadId = 0;
        }

        /// <summary>
        /// Increment the current epoch; this should be called "occasionally" to
        /// ensure that items removed from client data structures can eventually be
        /// removed. Roughly, items removed from data structures cannot be reclaimed
        /// until the epoch in which they were removed ends and all threads that may
        /// have operated in the protected region during that Epoch have exited the
        /// protected region. As a result, the current epoch should be bumped whenever
        /// enough items have been removed from data structures that they represent
        /// a significant amount of memory. Bumping the epoch unnecessarily may impact
        /// performance, since it is an atomic operation and invalidates a read-hot
        /// object in the cache of all of the cores.
        /// </summary>
        public int BumpCurrentEpoch()
        {
            int nextEpoch = Interlocked.Add(ref CurrentEpoch, 1);
            
            if (drainCount > 0)
            {
                Drain(nextEpoch);
            }

            return nextEpoch;
        }

        public int BumpCurrentEpoch(Action onDrain)
        {
            int PriorEpoch = BumpCurrentEpoch() - 1;

            int i = 0, j = 0;
            while (true)
            {
                if (drainList[i].epoch == int.MaxValue)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, int.MaxValue-1, int.MaxValue) == int.MaxValue)
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
                        if (Interlocked.CompareExchange(ref drainList[i].epoch, int.MaxValue - 1, triggerEpoch) == triggerEpoch)
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
                    i = 0;
                    if (++j == 500)
                    {
                        j = 0;
                        Thread.Sleep(1);
                        Console.Write("."); // "Slowdown: Unable to add trigger to epoch");
                    }
                }
            }

            ProtectAndDrain();

            return PriorEpoch + 1;
        }

        /// <summary>
        /// Looks at all of the threads in the protected region and \a currentEpoch
        /// and returns the latest Epoch that is guaranteed to be safe for reclamation.
        /// That is, all items removed and tagged with a lower Epoch than returned by
        /// this call may be safely reused.
        /// </summary>
        /// <param name="currentEpoch"></param>
        /// <returns></returns>
        public int ComputeNewSafeToReclaimEpoch(int currentEpoch)
        {
            int oldestOngoingCall = currentEpoch;

            for (int index = 1; index <= numEntries; ++index)
            {
                int entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                if (0 != entry_epoch && entry_epoch < oldestOngoingCall)
                {
                    oldestOngoingCall = entry_epoch;
                }
            }

            // The latest safe epoch is the one just before 
            //the earlier unsafe one.
            SafeToReclaimEpoch = oldestOngoingCall - 1;
            return SafeToReclaimEpoch;
        }

        public void SpinWaitForSafeToReclaim(int currentEpoch, int safeToReclaimEpoch)
        {
            do
            {
                ComputeNewSafeToReclaimEpoch(currentEpoch);
            }
            while (SafeToReclaimEpoch < safeToReclaimEpoch);
        }

        public bool IsSafeToReclaim(long epoch)
        {
            return (epoch <= SafeToReclaimEpoch);
        }

        /// <summary>
        /// Does the heavy lifting of reserveEntryForThread() and is really just
        /// split out for easy unit testing. This method relies on the fact that no
        /// thread will ever have ID 0.
        ///
        /// http://msdn.microsoft.com/en-us/library/windows/desktop/ms686746(v=vs.85).asp
        /// </summary>
        /// <param name="startIndex"></param>
        /// <param name="threadId"></param>
        /// <returns></returns>
        private int ReserveEntry(int startIndex, int threadId)
        {
            int current_iteration = 0;
            for (; ; )
            {
                // Reserve an entry in the table.
                for (int i = 0; i < numEntries; ++i)
                {
                    int index_to_test = 1 + ((startIndex + i) & (numEntries - 1));
                    if (0 == (*(tableAligned + index_to_test)).threadId)
                    {
                        bool success =
                            (0 == Interlocked.CompareExchange(
                            ref (*(tableAligned + index_to_test)).threadId,
                            threadId, 0));

                        if (success)
                        {
#if COUNT_ACTIVE_THREADS
                            Interlocked.Increment(ref CurrentNumThreads);
#endif
                            return (int)index_to_test;
                        }
                    }
                    ++current_iteration;
                }

                if (current_iteration > (numEntries * 3))
                {
                    throw new Exception("unable to make progress reserving an epoch entry.");
                }
            }
        }

        /// <summary>
        /// Allocate a new Entry to track a thread's protected/unprotected status and
        /// return the index to it. This should only be called once for a thread.
        /// </summary>
        /// <returns></returns>
        private int ReserveEntryForThread()
        {
            int threadId = (int)Native32.GetCurrentThreadId();
            int startIndex = Utility.Murmur3(threadId);
            return ReserveEntry(startIndex, threadId);
        }

        /// <summary>
        /// An entry tracks the protected/unprotected state of a single
        /// thread. Threads (conservatively) the Epoch when they entered
        /// the protected region, and more loosely when they left.
        /// Threads compete for entries and atomically lock them using a
        /// compare-and-swap on the #m_threadId member.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Constants.kCacheLineBytes)]
        private struct Entry
        {

            /// <summary>
            /// Threads record a snapshot of the global epoch during Protect().
            /// Threads reset this to 0 during Unprotect().
            /// It is safe that this value may actually lag the real current
            /// epoch by the time it is actually stored. This value is set
            /// with a sequentially-consistent store, which guarantees that
            /// it precedes any pointers that were removed (with sequential
            /// consistency) from data structures before the thread entered
            /// the epoch. This is critical to ensuring that a thread entering
            /// a protected region can never see a pointer to a data item that
            /// was already "unlinked" from a protected data structure. If an
            /// item is "unlinked" while this field is non-zero, then the thread
            /// associated with this entry may be able to access the unlinked
            /// memory still. This is safe, because the value stored here must
            /// be less than the epoch value associated with the deleted item
            /// (by sequential consistency, the snapshot of the epoch taken
            /// during the removal operation must have happened before the
            /// snapshot taken just before this field was updated during
            /// Protect()), which will prevent its reuse until this (and all
            /// other threads that could access the item) have called
            /// Unprotect().
            /// </summary>
            [FieldOffset(0)]
            public int localCurrentEpoch;

            /// <summary>
            /// ID of the thread associated with this entry. Entries are
            /// locked by threads using atomic compare-and-swap. See
            /// reserveEntry() for details.
            /// </summary>
            [FieldOffset(4)]
            public int threadId;

            [FieldOffset(8)]
            public int reentrant;

            [FieldOffset(12)]
            public fixed int markers[13];
        };

        private struct EpochActionPair
        {
            public long epoch;
            public Action action;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MarkAndCheckIsComplete(int markerIdx, int version)
        {
            int entry = threadEntryIndex;
            if (kInvalidIndex == entry)
            {
                Debug.WriteLine("New Thread entered during CPR");
                Debug.Assert(false);
            }

            (*(tableAligned + entry)).markers[markerIdx] = version;

            // check if all threads have reported complete
            for (int index = 1; index <= numEntries; ++index)
            {
                int entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                int fc_version = (*(tableAligned + index)).markers[markerIdx];
                if (0 != entry_epoch)
                {
                    if (fc_version != version)
                    {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}

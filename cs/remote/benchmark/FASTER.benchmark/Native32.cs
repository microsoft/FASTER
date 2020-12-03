// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.benchmark
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Interop with WINAPI for file I/O, threading, and NUMA functions.
    /// </summary>
    public static unsafe class Native32
    {
        #region Thread and NUMA functions
        [DllImport("kernel32.dll")]
        private static extern IntPtr GetCurrentThread();
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint GetActiveProcessorCount(uint count);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern ushort GetActiveProcessorGroupCount();
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int SetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY GroupAffinity, ref GROUP_AFFINITY PreviousGroupAffinity);
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int GetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY PreviousGroupAffinity);

        private static readonly uint ALL_PROCESSOR_GROUPS = 0xffff;

        [System.Runtime.InteropServices.StructLayoutAttribute(System.Runtime.InteropServices.LayoutKind.Sequential)]
        private struct GROUP_AFFINITY
        {
            public ulong Mask;
            public uint Group;
            public uint Reserved1;
            public uint Reserved2;
            public uint Reserved3;
        }

        /// <summary>
        /// Accepts thread id = 0, 1, 2, ... and sprays them round-robin
        /// across all cores (viewed as a flat space). On NUMA machines,
        /// this gives us [socket, core] ordering of affinitization. That is, 
        /// if there are N cores per socket, then thread indices of 0 to N-1 map
        /// to the range [socket 0, core 0] to [socket 0, core N-1].
        /// </summary>
        /// <param name="threadIdx">Index of thread (from 0 onwards)</param>
        public static void AffinitizeThreadRoundRobin(uint threadIdx)
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            ushort nrOfProcessorGroups = GetActiveProcessorGroupCount();
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;

            GROUP_AFFINITY groupAffinityThread = default;
            GROUP_AFFINITY oldAffinityThread = default;

            IntPtr thread = GetCurrentThread();
            GetThreadGroupAffinity(thread, ref groupAffinityThread);

            threadIdx %= nrOfProcessors;

            groupAffinityThread.Mask = (ulong)1L << ((int)(threadIdx % (int)nrOfProcsPerGroup));
            groupAffinityThread.Group = (uint)(threadIdx / nrOfProcsPerGroup);

            if (SetThreadGroupAffinity(thread, ref groupAffinityThread, ref oldAffinityThread) == 0)
            {
                throw new Exception("Unable to affinitize thread");
            }
        }

        /// <summary>
        /// Accepts thread id = 0, 1, 2, ... and sprays them round-robin
        /// across all cores (viewed as a flat space). On NUMA machines,
        /// this gives us [core, socket] ordering of affinitization. That is, 
        /// if there are N cores per socket, then thread indices of 0 to N-1 map
        /// to the range [socket 0, core 0] to [socket N-1, core 0].
        /// </summary>
        /// <param name="threadIdx">Index of thread (from 0 onwards)</param>
        /// <param name="nrOfProcessorGroups">Number of NUMA sockets</param>
        public static void AffinitizeThreadShardedNuma(uint threadIdx, ushort nrOfProcessorGroups)
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;

            threadIdx = nrOfProcsPerGroup * (threadIdx % nrOfProcessorGroups) + (threadIdx / nrOfProcessorGroups);
            AffinitizeThreadRoundRobin(threadIdx);
            return;
        }
        #endregion
    }
}

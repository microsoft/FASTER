// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.


namespace FASTER.core
{
    using System;
    using System.Runtime.InteropServices;
    using System.Security;
    using Microsoft.Win32.SafeHandles;
    using System.Threading;

    /// <summary>
    /// Interop with WINAPI for file I/O, threading, and NUMA functions.
    /// </summary>
    public static unsafe class Native32
    {
        #region io constants and flags

        public const uint INFINITE = unchecked((uint)-1);

        public const int ERROR_IO_PENDING = 997;
        public const uint ERROR_IO_INCOMPLETE = 996;
        public const uint ERROR_NOACCESS = 998;
        public const uint ERROR_HANDLE_EOF = 38;

        public const int ERROR_FILE_NOT_FOUND = 0x2;
        public const int ERROR_PATH_NOT_FOUND = 0x3;
        public const int ERROR_INVALID_DRIVE = 0x15;


        public const uint FILE_BEGIN = 0;
        public const uint FILE_CURRENT = 1;
        public const uint FILE_END = 2;

        public const uint FORMAT_MESSAGE_ALLOCATE_BUFFER = 0x00000100;
        public const uint FORMAT_MESSAGE_IGNORE_INSERTS = 0x00000200;
        public const uint FORMAT_MESSAGE_FROM_SYSTEM = 0x00001000;

        public const uint INVALID_HANDLE_VALUE = unchecked((uint)-1);

        public const uint GENERIC_READ = 0x80000000;
        public const uint GENERIC_WRITE = 0x40000000;
        public const uint GENERIC_EXECUTE = 0x20000000;
        public const uint GENERIC_ALL = 0x10000000;

        public const uint READ_CONTROL = 0x00020000;
        public const uint FILE_READ_ATTRIBUTES = 0x0080;
        public const uint FILE_READ_DATA = 0x0001;
        public const uint FILE_READ_EA = 0x0008;
        public const uint STANDARD_RIGHTS_READ = READ_CONTROL;
        public const uint FILE_APPEND_DATA = 0x0004;
        public const uint FILE_WRITE_ATTRIBUTES = 0x0100;
        public const uint FILE_WRITE_DATA = 0x0002;
        public const uint FILE_WRITE_EA = 0x0010;
        public const uint STANDARD_RIGHTS_WRITE = READ_CONTROL;

        public const uint FILE_GENERIC_READ =
            FILE_READ_ATTRIBUTES
            | FILE_READ_DATA
            | FILE_READ_EA
            | STANDARD_RIGHTS_READ;
        public const uint FILE_GENERIC_WRITE =
            FILE_WRITE_ATTRIBUTES
            | FILE_WRITE_DATA
            | FILE_WRITE_EA
            | STANDARD_RIGHTS_WRITE
            | FILE_APPEND_DATA;

        public const uint FILE_SHARE_DELETE = 0x00000004;
        public const uint FILE_SHARE_READ = 0x00000001;
        public const uint FILE_SHARE_WRITE = 0x00000002;

        public const uint CREATE_ALWAYS = 2;
        public const uint CREATE_NEW = 1;
        public const uint OPEN_ALWAYS = 4;
        public const uint OPEN_EXISTING = 3;
        public const uint TRUNCATE_EXISTING = 5;

        public const uint FILE_FLAG_DELETE_ON_CLOSE = 0x04000000;
        public const uint FILE_FLAG_NO_BUFFERING = 0x20000000;
        public const uint FILE_FLAG_OPEN_NO_RECALL = 0x00100000;
        public const uint FILE_FLAG_OVERLAPPED = 0x40000000;
        public const uint FILE_FLAG_RANDOM_ACCESS = 0x10000000;
        public const uint FILE_FLAG_SEQUENTIAL_SCAN = 0x08000000;
        public const uint FILE_FLAG_WRITE_THROUGH = 0x80000000;
        public const uint FILE_ATTRIBUTE_ENCRYPTED = 0x4000;

        /// <summary>
        /// Represents additional options for creating unbuffered overlapped file stream.
        /// </summary>
        [Flags]
        public enum UnbufferedFileOptions : uint
        {
            None = 0,
            WriteThrough = 0x80000000,
            DeleteOnClose = 0x04000000,
            OpenReparsePoint = 0x00200000,
            Overlapped = 0x40000000,
        }

        #endregion

        #region io functions

        [DllImport("Kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern SafeFileHandle CreateFileW(
            [In] string lpFileName,
            [In] UInt32 dwDesiredAccess,
            [In] UInt32 dwShareMode,
            [In] IntPtr lpSecurityAttributes,
            [In] UInt32 dwCreationDisposition,
            [In] UInt32 dwFlagsAndAttributes,
            [In] IntPtr hTemplateFile);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern void CloseHandle(
            [In] SafeHandle handle);

        [DllImport("Kernel32.dll", SetLastError = true)]
        public static extern bool ReadFile(
            [In] SafeFileHandle hFile,
            [Out] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToRead,
            [Out] out UInt32 lpNumberOfBytesRead,
            [In] NativeOverlapped* lpOverlapped);

        [DllImport("Kernel32.dll", SetLastError = true)]
        public static extern bool WriteFile(
            [In] SafeFileHandle hFile,
            [In] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToWrite,
            [Out] out UInt32 lpNumberOfBytesWritten,
            [In] NativeOverlapped* lpOverlapped);

        [DllImport("Kernel32.dll", SetLastError = true)]
        public static extern bool GetOverlappedResult(
            [In] SafeFileHandle hFile,
            [In] NativeOverlapped* lpOverlapped,
            [Out] out UInt32 lpNumberOfBytesTransferred,
            [In] bool bWait);

        [DllImport("adv-file-ops.dll", SetLastError = true)]
        public static extern bool CreateAndSetFileSize(ref string filename, Int64 file_size);

        [DllImport("adv-file-ops.dll", SetLastError = true)]
        public static extern bool EnableProcessPrivileges();

        [DllImport("adv-file-ops.dll", SetLastError = true)]
        public static extern bool EnableVolumePrivileges(ref string filename, SafeFileHandle hFile);

        [DllImport("adv-file-ops.dll", SetLastError = true)]
        public static extern bool SetFileSize(SafeFileHandle hFile, Int64 file_size);

        public enum EMoveMethod : uint
        {
            Begin = 0,
            Current = 1,
            End = 2
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint SetFilePointer(
              [In] SafeFileHandle hFile,
              [In] int lDistanceToMove,
              [In, Out] ref int lpDistanceToMoveHigh,
              [In] EMoveMethod dwMoveMethod);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint SetFilePointerEx(
              [In] SafeFileHandle hFile,
              [In] long lDistanceToMove,
              [In, Out] IntPtr lpDistanceToMoveHigh,
              [In] EMoveMethod dwMoveMethod);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool SetEndOfFile(
            [In] SafeFileHandle hFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr CreateIoCompletionPort(
            [In] SafeFileHandle fileHandle,
            [In] IntPtr existingCompletionPort,
            [In] UInt32 completionKey,
            [In] UInt32 numberOfConcurrentThreads);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern UInt32 GetLastError();

        [DllImport("kernel32.dll", SetLastError = true)]
        public static unsafe extern bool GetQueuedCompletionStatus(
            [In] IntPtr completionPort,
            [Out] out UInt32 ptrBytesTransferred,
            [Out] out UInt32 ptrCompletionKey,
            [Out] NativeOverlapped** lpOverlapped,
            [In] UInt32 dwMilliseconds);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool PostQueuedCompletionStatus(
            [In] IntPtr completionPort,
            [In] UInt32 bytesTrasferred,
            [In] UInt32 completionKey,
            [In] IntPtr lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern bool GetDiskFreeSpace(string lpRootPathName,
           out uint lpSectorsPerCluster,
           out uint lpBytesPerSector,
           out uint lpNumberOfFreeClusters,
           out uint lpTotalNumberOfClusters);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool DeleteFileW([MarshalAs(UnmanagedType.LPWStr)]string lpFileName);
        #endregion

        #region thread and numa functions
        [DllImport("kernel32.dll")]
        public static extern IntPtr GetCurrentThread();
        [DllImport("kernel32")]
        public static extern uint GetCurrentThreadId();
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint GetCurrentProcessorNumber();
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint GetActiveProcessorCount(uint count);
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern ushort GetActiveProcessorGroupCount();

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern int SetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY GroupAffinity, ref GROUP_AFFINITY PreviousGroupAffinity);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern int GetThreadGroupAffinity(IntPtr hThread, ref GROUP_AFFINITY PreviousGroupAffinity);

        public static uint ALL_PROCESSOR_GROUPS = 0xffff;

        [System.Runtime.InteropServices.StructLayoutAttribute(System.Runtime.InteropServices.LayoutKind.Sequential)]
        public struct GROUP_AFFINITY
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

            GROUP_AFFINITY groupAffinityThread = default(GROUP_AFFINITY);
            GROUP_AFFINITY oldAffinityThread = default(GROUP_AFFINITY);

            IntPtr thread = GetCurrentThread();
            GetThreadGroupAffinity(thread, ref groupAffinityThread);

            threadIdx = threadIdx % nrOfProcessors;

            groupAffinityThread.Mask = (ulong)1L << ((int)(threadIdx % (int)nrOfProcsPerGroup));
            groupAffinityThread.Group = (uint)(threadIdx / nrOfProcsPerGroup);

            if (SetThreadGroupAffinity(thread, ref groupAffinityThread, ref oldAffinityThread) == 0)
            {
                Console.WriteLine("Unable to set group affinity");
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
        public static void AffinitizeThreadShardedTwoNuma(uint threadIdx)
        {
            uint nrOfProcessors = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
            ushort nrOfProcessorGroups = 2; // GetActiveProcessorGroupCount();
            uint nrOfProcsPerGroup = nrOfProcessors / nrOfProcessorGroups;

            threadIdx = threadIdx % 2 == 0 ? threadIdx / 2 : (nrOfProcsPerGroup + (threadIdx - 1) / 2);
            AffinitizeThreadRoundRobin(threadIdx);
            return;
        }
        #endregion
    }

    /// <summary>
    /// Methods to perform high-resolution low-overhead timing
    /// </summary>
    public static class HiResTimer
    {
        private const string lib = "kernel32.dll";
        [DllImport(lib)]
        [SuppressUnmanagedCodeSecurity]
        public static extern int QueryPerformanceCounter(ref Int64 count);

        [DllImport(lib)]
        [SuppressUnmanagedCodeSecurity]
        public static extern int QueryPerformanceFrequency(ref Int64 frequency);

        [DllImport(lib)]
        [SuppressUnmanagedCodeSecurity]
        private static extern void GetSystemTimePreciseAsFileTime(out long filetime);

        [DllImport(lib)]
        [SuppressUnmanagedCodeSecurity]
        private static extern void GetSystemTimeAsFileTime(out long filetime);

        [DllImport("readtsc.dll")]
        [SuppressUnmanagedCodeSecurity]
        public static extern ulong Rdtsc();

        public static long Freq;

        public static long EstimateCPUFrequency()
        {
            long oldCps = 0, cps = 0;
            ulong startC, endC;
            long accuracy = 500; // wait for consecutive measurements to get within 300 clock cycles

            int i = 0;
            while (i < 5)
            {
                GetSystemTimeAsFileTime(out long startT);
                startC = Rdtsc();

                while (true)
                {
                    GetSystemTimeAsFileTime(out long endT);
                    endC = Rdtsc();

                    if (endT - startT >= 10000000)
                    {
                        cps = (long)(10000000 * (endC - startC) / (double)(endT - startT));
                        break;
                    }
                }


                if ((oldCps > (cps - accuracy)) && (oldCps < (cps + accuracy)))
                {
                    Freq = cps;
                    return cps;
                }
                oldCps = cps;
                i++;
            }
            Freq = cps;
            return cps;
        }
    }
}

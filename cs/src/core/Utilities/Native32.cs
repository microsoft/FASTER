// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    using System;
    using System.Runtime.InteropServices;
    using System.Security;
    using Microsoft.Win32.SafeHandles;
    using System.Threading;
    using System.IO;

    /// <summary>
    /// Interop with WINAPI for file I/O, threading, and NUMA functions.
    /// </summary>
    public static unsafe class Native32
    {
        #region Native structs
        [StructLayout(LayoutKind.Sequential)]
        private struct LUID
        {
            public uint lp;
            public int hp;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct LUID_AND_ATTRIBUTES
        {
            public LUID Luid;
            public uint Attributes;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct TOKEN_PRIVILEGES
        {
            public uint PrivilegeCount;
            public LUID_AND_ATTRIBUTES Privileges;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct MARK_HANDLE_INFO
        {
            public uint UsnSourceInfo;
            public IntPtr VolumeHandle;
            public uint HandleInfo;
        }
        #endregion

        #region io constants and flags
        internal const int ERROR_IO_PENDING = 997;
        internal const uint GENERIC_READ = 0x80000000;
        internal const uint GENERIC_WRITE = 0x40000000;
        internal const uint FILE_FLAG_DELETE_ON_CLOSE = 0x04000000;
        internal const uint FILE_FLAG_NO_BUFFERING = 0x20000000;
        internal const uint FILE_FLAG_OVERLAPPED = 0x40000000;

        internal const uint FILE_SHARE_DELETE = 0x00000004;
        #endregion

        #region io functions

        [DllImport("Kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern SafeFileHandle CreateFileW(
            [In] string lpFileName,
            [In] UInt32 dwDesiredAccess,
            [In] UInt32 dwShareMode,
            [In] IntPtr lpSecurityAttributes,
            [In] UInt32 dwCreationDisposition,
            [In] UInt32 dwFlagsAndAttributes,
            [In] IntPtr hTemplateFile);

        [DllImport("Kernel32.dll", SetLastError = true)]
        internal static extern bool ReadFile(
            [In] SafeFileHandle hFile,
            [Out] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToRead,
            [Out] out UInt32 lpNumberOfBytesRead,
            [In] NativeOverlapped* lpOverlapped);

        [DllImport("Kernel32.dll", SetLastError = true)]
        internal static extern bool WriteFile(
            [In] SafeFileHandle hFile,
            [In] IntPtr lpBuffer,
            [In] UInt32 nNumberOfBytesToWrite,
            [Out] out UInt32 lpNumberOfBytesWritten,
            [In] NativeOverlapped* lpOverlapped);


        internal enum EMoveMethod : uint
        {
            Begin = 0,
            Current = 1,
            End = 2
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern uint SetFilePointer(
              [In] SafeFileHandle hFile,
              [In] int lDistanceToMove,
              [In, Out] ref int lpDistanceToMoveHigh,
              [In] EMoveMethod dwMoveMethod);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool SetEndOfFile(
            [In] SafeFileHandle hFile);


        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern bool GetDiskFreeSpace(string lpRootPathName,
           out uint lpSectorsPerCluster,
           out uint lpBytesPerSector,
           out uint lpNumberOfFreeClusters,
           out uint lpTotalNumberOfClusters);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool DeleteFileW([MarshalAs(UnmanagedType.LPWStr)]string lpFileName);
#endregion

        #region Thread and NUMA functions
        [DllImport("kernel32.dll")]
        private static extern IntPtr GetCurrentThread();
        [DllImport("kernel32")]
        internal static extern uint GetCurrentThreadId();
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint GetCurrentProcessorNumber();
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

            GROUP_AFFINITY groupAffinityThread = default(GROUP_AFFINITY);
            GROUP_AFFINITY oldAffinityThread = default(GROUP_AFFINITY);

            IntPtr thread = GetCurrentThread();
            GetThreadGroupAffinity(thread, ref groupAffinityThread);

            threadIdx = threadIdx % nrOfProcessors;

            groupAffinityThread.Mask = (ulong)1L << ((int)(threadIdx % (int)nrOfProcsPerGroup));
            groupAffinityThread.Group = (uint)(threadIdx / nrOfProcsPerGroup);

            if (SetThreadGroupAffinity(thread, ref groupAffinityThread, ref oldAffinityThread) == 0)
            {
                throw new FasterException("Unable to affinitize thread");
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

        #region Advanced file ops
        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool LookupPrivilegeValue(string lpSystemName, string lpName, ref LUID lpLuid);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GetCurrentProcess();

        [DllImport("advapi32", SetLastError = true)]
        private static extern bool OpenProcessToken(IntPtr ProcessHandle, uint DesiredAccess, out IntPtr TokenHandle);

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool AdjustTokenPrivileges(IntPtr tokenhandle, int disableprivs, ref TOKEN_PRIVILEGES Newstate, int BufferLengthInBytes, int PreviousState, int ReturnLengthInBytes);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr hObject);

        [DllImport("Kernel32.dll", SetLastError = true)]
        private static extern bool DeviceIoControl(SafeFileHandle hDevice, uint IoControlCode, void* InBuffer, int nInBufferSize, IntPtr OutBuffer, int nOutBufferSize, ref uint pBytesReturned, IntPtr Overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetFilePointerEx(SafeFileHandle hFile, long liDistanceToMove, out long lpNewFilePointer, uint dwMoveMethod);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetFileValidData(SafeFileHandle hFile, long ValidDataLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern SafeFileHandle CreateFile(string filename, uint access, uint share, IntPtr securityAttributes, uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

        /// <summary>
        /// Enable privilege for process
        /// </summary>
        /// <returns></returns>
        public static bool EnableProcessPrivileges()
        {
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return false;
#endif

            TOKEN_PRIVILEGES token_privileges = default(TOKEN_PRIVILEGES);
            token_privileges.PrivilegeCount = 1;
            token_privileges.Privileges.Attributes = 0x2;

            if (!LookupPrivilegeValue(null, "SeManageVolumePrivilege",
                ref token_privileges.Privileges.Luid)) return false;

            if (!OpenProcessToken(GetCurrentProcess(), 0x20, out IntPtr token)) return false;

            if (!AdjustTokenPrivileges(token, 0, ref token_privileges, 0, 0, 0)) return false;
            if (Marshal.GetLastWin32Error() != 0) return false;
            CloseHandle(token);
            return true;
        }

        private static uint CTL_CODE(uint DeviceType, uint Function, uint Method, uint Access)
        {
            return (((DeviceType) << 16) | ((Access) << 14) | ((Function) << 2) | (Method));
        }

        internal static bool EnableVolumePrivileges(string filename, SafeFileHandle handle)
        {
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return false;
#endif

            string volume_string = "\\\\.\\" + filename.Substring(0, 2);

            uint fileCreation = unchecked((uint)FileMode.Open);

            SafeFileHandle volume_handle = CreateFile(volume_string, 0, 0, IntPtr.Zero, fileCreation,
                0x80, IntPtr.Zero);
            if (volume_handle == null)
            {
                return false;
            }

            MARK_HANDLE_INFO mhi;
            mhi.UsnSourceInfo = 0x1;
            mhi.VolumeHandle = volume_handle.DangerousGetHandle();
            mhi.HandleInfo = 0x1;

            uint bytes_returned = 0;
            bool result = DeviceIoControl(handle, CTL_CODE(0x9, 63, 0, 0),
                (void*)&mhi, sizeof(MARK_HANDLE_INFO), IntPtr.Zero,
                0, ref bytes_returned, IntPtr.Zero);

            if (!result)
            {
                return false;
            }

            volume_handle.Close();
            return true;
        }

        /// <summary>
        /// Set file size
        /// </summary>
        /// <param name="file_handle"></param>
        /// <param name="file_size"></param>
        /// <returns></returns>
        public static bool SetFileSize(SafeFileHandle file_handle, long file_size)
        {
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return false;
#endif

            if (!SetFilePointerEx(file_handle, file_size, out long newFilePtr, 0))
            {
                return false;
            }

            // Set a fixed file length
            if (!SetEndOfFile(file_handle))
            {
                return false;
            }

            if (!SetFileValidData(file_handle, file_size))
            {
                return false;
            }

            return true;
        }

        internal static int MakeHRFromErrorCode(int errorCode)
        {
            return unchecked(((int)0x80070000) | errorCode);
        }
        #endregion
    }
}

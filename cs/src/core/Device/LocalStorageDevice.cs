// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Local storage device
    /// </summary>
    public class LocalStorageDevice : StorageDeviceBase
    {
        private readonly bool preallocateFile;
        private readonly bool deleteOnClose;
        private readonly ConcurrentDictionary<int, SafeFileHandle> logHandles;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        public LocalStorageDevice(string filename, bool preallocateFile = false, bool deleteOnClose = false)
            : base(filename, GetSectorSize(filename))
        {
            Native32.EnableProcessPrivileges();
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            logHandles = new ConcurrentDictionary<int, SafeFileHandle>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, 
                                     IntPtr destinationAddress, 
                                     uint readLength, 
                                     IOCompletionCallback callback, 
                                     IAsyncResult asyncResult)
        {
            var logHandle = GetOrAddHandle(segmentId);

            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);
            ovNative->OffsetLow = unchecked((int)((ulong)sourceAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)(((ulong)sourceAddress >> 32) & 0xFFFFFFFF));

            bool result = Native32.ReadFile(logHandle,
                                            destinationAddress,
                                            readLength,
                                            out uint bytesRead,
                                            ovNative);

            if (!result)
            {
                int error = Marshal.GetLastWin32Error();
                if (error != Native32.ERROR_IO_PENDING)
                {
                    Overlapped.Unpack(ovNative);
                    Overlapped.Free(ovNative);
                    throw new Exception("Error reading from log file: " + error);
                }
            }
            else
            {
                // On synchronous completion, issue callback directly
                callback(0, bytesRead, ovNative);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceAddress"></param>
        /// <param name="segmentId"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="numBytesToWrite"></param>
        /// <param name="callback"></param>
        /// <param name="asyncResult"></param>
        public override unsafe void WriteAsync(IntPtr sourceAddress, 
                                      int segmentId,
                                      ulong destinationAddress, 
                                      uint numBytesToWrite, 
                                      IOCompletionCallback callback, 
                                      IAsyncResult asyncResult)
        {
            var logHandle = GetOrAddHandle(segmentId);
            
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);
            ovNative->OffsetLow = unchecked((int)(destinationAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)((destinationAddress >> 32) & 0xFFFFFFFF));

            bool result = Native32.WriteFile(logHandle,
                                    sourceAddress,
                                    numBytesToWrite,
                                    out uint bytesWritten,
                                    ovNative);

            if (!result)
            {
                int error = Marshal.GetLastWin32Error();
                if (error != Native32.ERROR_IO_PENDING)
                {
                    Overlapped.Unpack(ovNative);
                    Overlapped.Free(ovNative);
                    throw new Exception("Error writing to log file: " + error);
                }
            }
            else
            {
                // On synchronous completion, issue callback directly
                callback(0, bytesWritten, ovNative);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="fromSegment"></param>
        /// <param name="toSegment"></param>
        public override void DeleteSegmentRange(int fromSegment, int toSegment)
        {
            for (int i=fromSegment; i<toSegment; i++)
            {
                if (logHandles.TryRemove(i, out SafeFileHandle logHandle))
                {
                    logHandle.Dispose();
                    Native32.DeleteFileW(GetSegmentName(i));
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Close()
        {
            foreach (var logHandle in logHandles.Values)
                logHandle.Dispose();
        }


        private string GetSegmentName(int segmentId)
        {
            return FileName + "." + segmentId;
        }

        private static uint GetSectorSize(string filename)
        {
            if (!Native32.GetDiskFreeSpace(filename.Substring(0, 3),
                                        out uint lpSectorsPerCluster,
                                        out uint _sectorSize,
                                        out uint lpNumberOfFreeClusters,
                                        out uint lpTotalNumberOfClusters))
            {
                Debug.WriteLine("Unable to retrieve information for disk " + filename.Substring(0, 3) + " - check if the disk is available and you have specified the full path with drive name. Assuming sector size of 512 bytes.");
                _sectorSize = 512;
            }
            return _sectorSize;
        }

        private SafeFileHandle CreateHandle(int segmentId)
        {
            uint fileAccess = Native32.GENERIC_READ | Native32.GENERIC_WRITE;
            uint fileShare = unchecked(((uint)FileShare.ReadWrite & ~(uint)FileShare.Inheritable));
            uint fileCreation = unchecked((uint)FileMode.OpenOrCreate);
            uint fileFlags = Native32.FILE_FLAG_OVERLAPPED;

            fileFlags = fileFlags | Native32.FILE_FLAG_NO_BUFFERING;
            if (deleteOnClose)
                fileFlags = fileFlags | Native32.FILE_FLAG_DELETE_ON_CLOSE;

            var logHandle = Native32.CreateFileW(
                GetSegmentName(segmentId),
                fileAccess, fileShare,
                IntPtr.Zero, fileCreation,
                fileFlags, IntPtr.Zero);

            if (preallocateFile)
                SetFileSize(FileName, logHandle, segmentSize);

            try
            {
                ThreadPool.BindHandle(logHandle);
            }
            catch (Exception e)
            {
                throw new Exception("Error binding log handle for " + GetSegmentName(segmentId) + ": " + e.ToString());
            }
            return logHandle;
        }

        private SafeFileHandle GetOrAddHandle(int _segmentId)
        {
            return logHandles.GetOrAdd(_segmentId, segmentId => CreateHandle(segmentId));
        }

        /// Sets file size to the specified value.
        /// Does not reset file seek pointer to original location.
        private bool SetFileSize(string filename, SafeFileHandle logHandle, long size)
        {
            if (segmentSize <= 0)
                return false;

            if (Native32.EnableVolumePrivileges(filename, logHandle))
            {
                return Native32.SetFileSize(logHandle, size);
            }

            int lodist = (int)size;
            int hidist = (int)(size >> 32);
            Native32.SetFilePointer(logHandle, lodist, ref hidist, Native32.EMoveMethod.Begin);
            if (!Native32.SetEndOfFile(logHandle)) return false;
            return true;
        }
    }
}

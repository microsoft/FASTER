// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Local storage device
    /// </summary>
    public unsafe class LocalStorageDevice : StorageDeviceBase
    {
        private readonly bool preallocateFile;
        private readonly bool deleteOnClose;
        private readonly bool disableFileBuffering;
        private readonly SafeConcurrentDictionary<int, SafeFileHandle> logHandles;

        private readonly ConcurrentQueue<SimpleAsyncResult> results;
        private static uint sectorSize = 0;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        public LocalStorageDevice(string filename,
                                  bool preallocateFile = false,
                                  bool deleteOnClose = false,
                                  bool disableFileBuffering = true,
                                  long capacity = Devices.CAPACITY_UNSPECIFIED,
                                  bool recoverDevice = false)
            : this(filename, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, initialLogFileHandles: null)
        {
        }

        void _callback(uint errorCode, uint numBytes, NativeOverlapped* pOVERLAP)
        {
            Interlocked.Decrement(ref numPending);
            var result = (SimpleAsyncResult)Overlapped.Unpack(pOVERLAP).AsyncResult;
            result.callback(errorCode, numBytes, result.context);
            results.Enqueue(result);
        }

        /// <inheritdoc />
        public override bool Throttle() => numPending > 120;

        /// <summary>
        /// Constructor with more options for derived classes
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        /// <param name="initialLogFileHandles">Optional set of preloaded safe file handles, which can speed up hydration of preexisting log file handles</param>
        protected internal LocalStorageDevice(string filename,
                                      bool preallocateFile = false,
                                      bool deleteOnClose = false,
                                      bool disableFileBuffering = true,
                                      long capacity = Devices.CAPACITY_UNSPECIFIED,
                                      bool recoverDevice = false,
                                      IEnumerable<KeyValuePair<int, SafeFileHandle>> initialLogFileHandles = null)
                : base(filename, GetSectorSize(filename), capacity)
        {
            Native32.EnableProcessPrivileges();
            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.results = new ConcurrentQueue<SimpleAsyncResult>();

            logHandles = initialLogFileHandles != null
                ? new SafeConcurrentDictionary<int, SafeFileHandle>(initialLogFileHandles)
                : new SafeConcurrentDictionary<int, SafeFileHandle>();
            if (recoverDevice)
                RecoverFiles();
        }

        private void RecoverFiles()
        {
            FileInfo fi = new FileInfo(FileName); // may not exist
            DirectoryInfo di = fi.Directory;
            if (!di.Exists) return;

            string bareName = fi.Name;

            List<int> segids = new List<int>();
            foreach (System.IO.FileInfo item in di.GetFiles(bareName + "*"))
            {
                segids.Add(int.Parse(item.Name.Replace(bareName, "").Replace(".", "")));
            }
            segids.Sort();

            int prevSegmentId = -1;
            foreach (int segmentId in segids)
            {
                if (segmentId != prevSegmentId + 1)
                {
                    startSegment = segmentId;
                }
                else
                {
                    endSegment = segmentId;
                }
                prevSegmentId = segmentId;
            }
            // No need to populate map because logHandles use Open or create on files.
        }

        /// <summary>
        /// Async read
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="sourceAddress"></param>
        /// <param name="destinationAddress"></param>
        /// <param name="readLength"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public override void ReadAsync(int segmentId, ulong sourceAddress, 
                                     IntPtr destinationAddress, 
                                     uint readLength, 
                                     DeviceIOCompletionCallback callback, 
                                     object context)
        {
            if (!results.TryDequeue(out SimpleAsyncResult result))
            {
                result = new SimpleAsyncResult();
                result.overlapped = new Overlapped(0, 0, IntPtr.Zero, result);
                result.nativeOverlapped = result.overlapped.UnsafePack(_callback, IntPtr.Zero);
            }

            result.context = context;
            result.callback = callback;
            var ovNative = result.nativeOverlapped;

            ovNative->OffsetLow = unchecked((int)((ulong)sourceAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)(((ulong)sourceAddress >> 32) & 0xFFFFFFFF));

            try
            {
                var logHandle = GetOrAddHandle(segmentId);

                Interlocked.Increment(ref numPending);

                bool _result = Native32.ReadFile(logHandle,
                                                destinationAddress,
                                                readLength,
                                                out uint bytesRead,
                                                ovNative);

                if (!_result)
                {
                    int error = Marshal.GetLastWin32Error();
                    if (error != Native32.ERROR_IO_PENDING)
                    {
                        throw new IOException("Error reading from log file", error);
                    }
                }
            }
            catch (IOException e)
            {
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                results.Enqueue(result);
            }
            catch
             {
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
                results.Enqueue(result);
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
        /// <param name="context"></param>
        public override unsafe void WriteAsync(IntPtr sourceAddress, 
                                      int segmentId,
                                      ulong destinationAddress, 
                                      uint numBytesToWrite, 
                                      DeviceIOCompletionCallback callback, 
                                      object context)
        {
            HandleCapacity(segmentId);

            if (!results.TryDequeue(out SimpleAsyncResult result))
            {
                result = new SimpleAsyncResult();
                result.overlapped = new Overlapped(0, 0, IntPtr.Zero, result);
                result.nativeOverlapped = result.overlapped.UnsafePack(_callback, IntPtr.Zero);
            }

            result.context = context;
            result.callback = callback;
            var ovNative = result.nativeOverlapped;

            ovNative->OffsetLow = unchecked((int)(destinationAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)((destinationAddress >> 32) & 0xFFFFFFFF));

            try
            {
                var logHandle = GetOrAddHandle(segmentId);

                Interlocked.Increment(ref numPending);

                bool _result = Native32.WriteFile(logHandle,
                                        sourceAddress,
                                        numBytesToWrite,
                                        out uint bytesWritten,
                                        ovNative);

                if (!_result)
                {
                    int error = Marshal.GetLastWin32Error();
                    if (error != Native32.ERROR_IO_PENDING)
                    {
                        throw new IOException("Error writing to log file", error);
                    }
                }
            }
            catch (IOException e)
            {
                Interlocked.Decrement(ref numPending);
                callback((uint)(e.HResult & 0x0000FFFF), 0, context);
                results.Enqueue(result);
            }
            catch
            {
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
                results.Enqueue(result);
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            if (logHandles.TryRemove(segment, out SafeFileHandle logHandle))
            {
                logHandle.Dispose();
                Native32.DeleteFileW(GetSegmentName(segment));
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            RemoveSegment(segment);
            callback(result);
        }

        /// <summary>
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            foreach (var logHandle in logHandles.Values)
                logHandle.Dispose();

            while (results.TryDequeue(out var entry))
            {
                Overlapped.Free(entry.nativeOverlapped);
            }
        }

        /// <summary>
        /// Creates a SafeFileHandle for the specified segment. This can be used by derived classes to prepopulate logHandles in the constructor.
        /// </summary>
        protected internal static SafeFileHandle CreateHandle(int segmentId, bool disableFileBuffering, bool deleteOnClose, bool preallocateFile, long segmentSize, string fileName)
        {
            uint fileAccess = Native32.GENERIC_READ | Native32.GENERIC_WRITE;
            uint fileShare = unchecked(((uint)FileShare.ReadWrite & ~(uint)FileShare.Inheritable));
            uint fileCreation = unchecked((uint)FileMode.OpenOrCreate);
            uint fileFlags = Native32.FILE_FLAG_OVERLAPPED;

            if (String.IsNullOrEmpty(fileName))
            {
                fileAccess = Native32.GENERIC_READ;
                //fileShare = unchecked(((uint)FileShare.Read & ~(uint)FileShare.Inheritable));
                fileCreation = unchecked((uint)FileMode.Open);
            }

            if (disableFileBuffering)
            {
                fileFlags = fileFlags | Native32.FILE_FLAG_NO_BUFFERING;
            }

            if (deleteOnClose)
            {
                fileFlags = fileFlags | Native32.FILE_FLAG_DELETE_ON_CLOSE;

                // FILE_SHARE_DELETE allows multiple FASTER instances to share a single log directory and each can specify deleteOnClose.
                // This will allow the files to persist until all handles across all instances have been closed.
                fileShare = fileShare | Native32.FILE_SHARE_DELETE;
            }

            var logHandle = Native32.CreateFileW(
                GetSegmentName(fileName, segmentId),
                fileAccess, fileShare,
                IntPtr.Zero, fileCreation,
                fileFlags, IntPtr.Zero);

            if (logHandle.IsInvalid)
            {
                var error = Marshal.GetLastWin32Error();
                throw new IOException($"Error creating log file for {GetSegmentName(fileName, segmentId)}, error: {error}", Native32.MakeHRFromErrorCode(error));
            }

            if (preallocateFile && segmentSize != -1)
                SetFileSize(fileName, logHandle, segmentSize);

            try
            {
                ThreadPool.BindHandle(logHandle);
            }
            catch (Exception e)
            {
                throw new FasterException("Error binding log handle for " + GetSegmentName(fileName, segmentId) + ": " + e.ToString());
            }
            return logHandle;
        }

        /// <summary>
        /// Static method to construct segment name
        /// </summary>
        protected static string GetSegmentName(string fileName, int segmentId)
        {
            return fileName + "." + segmentId;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="segmentId"></param>
        /// <returns></returns>
        protected string GetSegmentName(int segmentId) => GetSegmentName(FileName, segmentId);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="_segmentId"></param>
        /// <returns></returns>
        // Can be used to pre-load handles, e.g., after a checkpoint
        protected SafeFileHandle GetOrAddHandle(int _segmentId)
        {
            if (logHandles.TryGetValue(_segmentId, out SafeFileHandle h))
            {
                return h;
            }
            return logHandles.GetOrAdd(_segmentId, segmentId => CreateHandle(segmentId));
        }

        private SafeFileHandle CreateHandle(int segmentId)
            => CreateHandle(segmentId, this.disableFileBuffering, this.deleteOnClose, this.preallocateFile, this.segmentSize, this.FileName);

        private static uint GetSectorSize(string filename)
        {
            if (sectorSize > 0) return sectorSize;

            /* Get true physical sector size if we want to use it - commented for now as we prefer to use logical sector size (smaller) */

            /*
            SafeFileHandle safeFileHandle = CreateHandle(0, true, true, false, 0, filename + ".tmp");
            if (!safeFileHandle.IsInvalid)
            {
                if (Native32.GetFileInformationByHandleEx(safeFileHandle, Native32.FILE_INFO_BY_HANDLE_CLASS.FileStorageInfo, out Native32.FILE_STORAGE_INFO info, (uint)sizeof(Native32.FILE_STORAGE_INFO)))
                {
                    sectorSize = info.PhysicalBytesPerSectorForAtomicity;
                }
                safeFileHandle.Dispose();
            }
            if (sectorSize > 0) return sectorSize;
            Debug.WriteLine($"Unable to retrieve sector size information for temporary handle {filename + ".tmp"}, trying disk level information");
            */

            if (!Native32.GetDiskFreeSpace(filename.Substring(0, 3), out _, out sectorSize, out _, out _))
            {
                Debug.WriteLine("Unable to retrieve information for disk " + filename.Substring(0, 3) + " - check if the disk is available and you have specified the full path with drive name. Assuming sector size of 512 bytes.");
                sectorSize = 512;
            }
            return sectorSize;
        }

        /// Sets file size to the specified value.
        /// Does not reset file seek pointer to original location.
        private static bool SetFileSize(string filename, SafeFileHandle logHandle, long size)
        {
            if (size <= 0)
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

    unsafe sealed class SimpleAsyncResult : IAsyncResult
    {
        public DeviceIOCompletionCallback callback;
        public object context;
        public Overlapped overlapped;
        public NativeOverlapped* nativeOverlapped;

        public object AsyncState => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();

        public bool IsCompleted => throw new NotImplementedException();
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

#if DOTNETCORE
using Mono.Unix.Native;
#endif

namespace FASTER.core
{
    /// <summary>
    /// Managed device using .NET streams
    /// </summary>
    public sealed class ManagedLocalStorageDevice : StorageDeviceBase
    {
        private readonly bool preallocateFile;
        private readonly bool deleteOnClose;
        private readonly ConcurrentDictionary<int, (FixedPool<Stream>, FixedPool<Stream>)> logHandles;
        private readonly SectorAlignedBufferPool pool;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit</param>
        /// <param name="recoverDevice">Whether to recover device metadata from existing files</param>
        public ManagedLocalStorageDevice(string filename, bool preallocateFile = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED, bool recoverDevice = false)
            : base(filename, GetSectorSize(filename), capacity)
        {
            pool = new SectorAlignedBufferPool(1, 1);

            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            logHandles = new ConcurrentDictionary<int, (FixedPool<Stream>, FixedPool<Stream>)>();
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
            foreach (FileInfo item in di.GetFiles(bareName + "*"))
            {
                segids.Add(Int32.Parse(item.Name.Replace(bareName, "").Replace(".", "")));
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
        /// Read async
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
                                     DeviceIOCompletionCallback callback,
                                     IAsyncResult asyncResult)
        {
            SectorAlignedMemory memory = null;
            Stream logReadHandle = null;
            int offset = -1;
            FixedPool<Stream> streampool = null;

            memory = pool.Get((int)readLength);
            streampool = GetOrAddHandle(segmentId).Item1;
            (logReadHandle, offset) = streampool.Get();

            logReadHandle.Seek((long)sourceAddress, SeekOrigin.Begin);
            logReadHandle.ReadAsync(memory.buffer, 0, (int)readLength)
                .ContinueWith(t =>
                {
                    uint errorCode = 0;
                    if (!t.IsFaulted)
                    {
                        fixed (void* source = memory.buffer)
                        {
                            Buffer.MemoryCopy(source, (void*)destinationAddress, readLength, readLength);
                        }
                    }
                    else
                    {
                        if (t.Exception.InnerException is IOException)
                        {
                            var e = t.Exception.InnerException as IOException;
                            errorCode = (uint)(e.HResult & 0x0000FFFF);
                        }
                        else
                        {
                            errorCode = uint.MaxValue;
                        }
                    }
                    memory.Return();
                    // Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
                    callback(errorCode, (uint)t.Result, asyncResult); // ov.UnsafePack(callback, IntPtr.Zero));
                }
                );
            if (offset >= 0)
                streampool?.Return(offset);
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
            SectorAlignedMemory memory = null;
            Stream logWriteHandle = null;
            int offset = -1;
            FixedPool<Stream> streampool = null;

            memory = pool.Get((int)numBytesToWrite);
            streampool = GetOrAddHandle(segmentId).Item2;
            (logWriteHandle, offset) = streampool.Get();

            fixed (void* destination = memory.buffer)
            {
                Buffer.MemoryCopy((void*)sourceAddress, destination, numBytesToWrite, numBytesToWrite);
            }

            logWriteHandle.Seek((long)destinationAddress, SeekOrigin.Begin);
            logWriteHandle.WriteAsync(memory.buffer, 0, (int)numBytesToWrite)
                .ContinueWith(t =>
                {
                    uint errorCode = 0;
                    if (t.IsFaulted)
                    {
                        if (t.Exception.InnerException is IOException)
                        {
                            var e = t.Exception.InnerException as IOException;
                            errorCode = (uint)(e.HResult & 0x0000FFFF);
                        }
                        else
                        {
                            errorCode = uint.MaxValue;
                        }
                    }
                    memory.Return();

                    // Sequentialize all writes on non-windows
#if DOTNETCORE
                    if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        ((FileStream)logWriteHandle).Flush(true);
                        if (offset >= 0) streampool?.Return(offset);
                    }
#endif

                    Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
                    callback(errorCode, numBytesToWrite, ov.UnsafePack(callback, IntPtr.Zero));
                }
                );

#if DOTNETCORE
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                if (offset >= 0) streampool?.Return(offset);
#else
            if (offset >= 0) streampool?.Return(offset);
#endif
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            if (logHandles.TryRemove(segment, out (FixedPool<Stream>, FixedPool<Stream>) logHandle))
            {
                logHandle.Item1.Dispose();
                logHandle.Item2.Dispose();
                File.Delete(GetSegmentName(segment));
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
        /// 
        /// </summary>
        public override void Close()
        {
            foreach (var logHandle in logHandles.Values)
            {
                logHandle.Item1.Dispose();
                logHandle.Item2.Dispose();
            }
            pool.Free();
        }


        private string GetSegmentName(int segmentId)
        {
            return FileName + "." + segmentId;
        }

        private static uint GetSectorSize(string filename)
        {
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Debug.WriteLine("Assuming 512 byte sector alignment for disk with file " + filename);
                return 512;
            }
#endif
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

        private Stream CreateReadHandle(int segmentId)
        {
            const int FILE_FLAG_NO_BUFFERING = 0x20000000;
            FileOptions fo =
                (FileOptions)FILE_FLAG_NO_BUFFERING |
                FileOptions.WriteThrough | 
                FileOptions.Asynchronous |
                FileOptions.None;
            if (deleteOnClose)
                fo |= FileOptions.DeleteOnClose;

            var logReadHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Read, FileShare.ReadWrite, 512, fo);

#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Syscall.fcntl((int)logReadHandle.SafeFileHandle.DangerousGetHandle(), FcntlCommand.F_NOCACHE, 1);
            }
#endif

            return logReadHandle;
        }

        private Stream CreateWriteHandle(int segmentId)
        {
            const int FILE_FLAG_NO_BUFFERING = 0x20000000;
            FileOptions fo =
                (FileOptions)FILE_FLAG_NO_BUFFERING |
                FileOptions.WriteThrough | 
                FileOptions.Asynchronous |
                FileOptions.None;
            if (deleteOnClose)
                fo |= FileOptions.DeleteOnClose;

            var logWriteHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Write, FileShare.ReadWrite, 512, fo);

#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Syscall.fcntl((int)logWriteHandle.SafeFileHandle.DangerousGetHandle(), FcntlCommand.F_NOCACHE, 1);
            }
#endif

            if (preallocateFile && segmentSize != -1)
                SetFileSize(logWriteHandle, segmentSize);

            return logWriteHandle;
        }

        private (FixedPool<Stream>, FixedPool<Stream>) GetOrAddHandle(int _segmentId)
        {
#pragma warning disable IDE0067 // Dispose objects before losing scope
            return logHandles.GetOrAdd(_segmentId,
            (new FixedPool<Stream>(8, () => CreateReadHandle(_segmentId)),
             new FixedPool<Stream>(8, () => CreateWriteHandle(_segmentId))));
#pragma warning restore IDE0067 // Dispose objects before losing scope
        }

        /// <summary>
        /// Sets file size to the specified value.
        /// Does not reset file seek pointer to original location.
        /// </summary>
        /// <param name="logHandle"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private bool SetFileSize(Stream logHandle, long size)
        {
            logHandle.SetLength(size);
            return true;
        }
    }
}

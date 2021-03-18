// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Managed device using .NET streams
    /// </summary>
    public sealed class ManagedLocalStorageDevice : StorageDeviceBase
    {
#if NETSTANDARD
        // IsOSPlatform leads to a string comparison, cache the result once and reuse.
        private static bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#else
        private static bool IsWindows = true;
#endif

        private readonly bool preallocateFile;
        private readonly bool deleteOnClose;
        private readonly SafeConcurrentDictionary<int, (FixedPool<Stream>, FixedPool<Stream>)> logHandles;
        private readonly SectorAlignedBufferPool pool;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;

        private bool _disposed;

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
            ThrottleLimit = 120;

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            this._disposed = false;
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            logHandles = new SafeConcurrentDictionary<int, (FixedPool<Stream>, FixedPool<Stream>)>();
            if (recoverDevice)
                RecoverFiles();
        }

        /// <inheritdoc />
        public override bool Throttle() => numPending > ThrottleLimit;

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
        /// <param name="context"></param>
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            _ = Task.Run(async () =>
            {
                Stream logReadHandle = null;
                int offset = -1;
                FixedPool<Stream> streampool = null;
                uint errorCode = 0;
                int numBytes = 0;
                Task<int> readTask;
#if NETSTANDARD2_1
                UnmanagedMemoryManager<byte> umm = default;
#else
                SectorAlignedMemory memory = default;
#endif

                try
                {
                    Interlocked.Increment(ref numPending);
                    streampool = GetOrAddHandle(segmentId).Item1;
                    (logReadHandle, offset) = streampool.Get();
                    logReadHandle.Seek((long)sourceAddress, SeekOrigin.Begin);
#if NETSTANDARD2_1
                    unsafe
                    {
                        umm = new UnmanagedMemoryManager<byte>((byte*)destinationAddress, (int)readLength);
                    }

                    // FileStream.WriteAsync is not thread-safe hence need a lock here
                    lock (this)
                    {
                        readTask = logReadHandle.ReadAsync(umm.Memory).AsTask();
#else
                    memory = pool.Get((int)readLength);
                    lock (this)
                    {
                        readTask = logReadHandle.ReadAsync(memory.buffer, 0, (int)readLength);
#endif
                    }
                    if (IsWindows)
                    {
                        // If non-netstandard (==windows-only), or netstandard+windows, we can return to the pool immediately.
                        // For netstandard+linux we will return after the operation completes.
                        streampool?.Return(offset);
                    }
                }
                catch
                {
                    Interlocked.Decrement(ref numPending);
#if !NETSTANDARD2_1
                    memory?.Return();
#endif
                    streampool?.Return(offset);
                    streampool?.DisposeIfNeeded(offset, logReadHandle);
                    callback(uint.MaxValue, 0, context);
                    return;
                }
                
                try
                {
                    numBytes = await readTask;

                    // Sequentialize all reads from same handle on non-windows
                    if (!IsWindows)
                    {
                        streampool?.Return(offset);
                    }
                    streampool?.DisposeIfNeeded(offset, logReadHandle);
                }
                catch (Exception ex)
                {
                    if (ex.InnerException != null &&
                        ex.InnerException is IOException ioex)
                    {
                        errorCode = (uint)(ioex.HResult & 0x0000FFFF);
                    }
                    else
                    {
                        errorCode = uint.MaxValue;
                    }

                    numBytes = 0;
                }
                finally
                {
                    Interlocked.Decrement(ref numPending);
#if !NETSTANDARD2_1
                    unsafe
                    {
                        fixed (void* source = memory.buffer)
                            Buffer.MemoryCopy(source, (void*)destinationAddress, numBytes, numBytes);
                    }
                    memory?.Return();
#endif
                    callback(errorCode, (uint)numBytes, context);
                }
            });
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
        public override void WriteAsync(IntPtr sourceAddress,
                                      int segmentId,
                                      ulong destinationAddress,
                                      uint numBytesToWrite,
                                      DeviceIOCompletionCallback callback,
                                      object context)
        {
            _ = Task.Run(async () =>
            {
                Stream logWriteHandle = null;
                int offset = -1;
                FixedPool<Stream> streampool = null;
                uint errorCode = 0;
                Task writeTask;
#if NETSTANDARD2_1
                UnmanagedMemoryManager<byte> umm = default;
#else
            SectorAlignedMemory memory = default;
#endif

                HandleCapacity(segmentId);

                try
                {
                    Interlocked.Increment(ref numPending);
                    streampool = GetOrAddHandle(segmentId).Item2;
                    (logWriteHandle, offset) = streampool.Get();
                    logWriteHandle.Seek((long)destinationAddress, SeekOrigin.Begin);
#if NETSTANDARD2_1
                    unsafe
                    {
                        umm = new UnmanagedMemoryManager<byte>((byte*)sourceAddress, (int)numBytesToWrite);
                    }

                    // FileStream.WriteAsync is not thread-safe hence need a lock here
                    lock (this)
                    {
                        writeTask = logWriteHandle.WriteAsync(umm.Memory).AsTask();
#else
                    memory = pool.Get((int)numBytesToWrite);
                    unsafe
                    {
                        fixed (void* destination = memory.buffer)
                        {
                            Buffer.MemoryCopy((void*)sourceAddress, destination, numBytesToWrite, numBytesToWrite);
                        }
                    }
                    lock (this)
                    {
                        writeTask = logWriteHandle.WriteAsync(memory.buffer, 0, (int)numBytesToWrite);
#endif
                    }
                    if (IsWindows)
                    {
                        // If non-netstandard, or netstandard+windows, we can return to the pool immediately.
                        // For netstandard+linux we will return after the operation completes.
                        streampool?.Return(offset);
                    }
                }
                catch
                {
                    Interlocked.Decrement(ref numPending);
#if !NETSTANDARD2_1
                    memory?.Return();
#endif
                    streampool?.Return(offset);
                    streampool?.DisposeIfNeeded(offset, logWriteHandle);
                    callback(uint.MaxValue, 0, context);
                    return;
                }

                try
                {
                    await writeTask;

                    // Sequentialize all writes to same handle on non-windows
                    if (!IsWindows)
                    {
                        ((FileStream)logWriteHandle).Flush(true);
                        streampool?.Return(offset);
                    }
                    streampool?.DisposeIfNeeded(offset, logWriteHandle);
                }
                catch (Exception ex)
                {
                    if (ex.InnerException != null &&
                        ex.InnerException is IOException ioex)
                    {
                        errorCode = (uint)(ioex.HResult & 0x0000FFFF);
                    }
                    else
                    {
                        errorCode = uint.MaxValue;
                    }

                    numBytesToWrite = 0;
                }
                finally
                {
                    Interlocked.Decrement(ref numPending);
#if !NETSTANDARD2_1
                    memory?.Return();
#endif
                    callback(errorCode, numBytesToWrite, context);
                }
            });
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
        /// Close device
        /// </summary>
        public override void Dispose()
        {
            _disposed = true;
            foreach (var entry in logHandles)
            {
                entry.Value.Item1.Dispose();
                entry.Value.Item2.Dispose();
                if (deleteOnClose)
                    File.Delete(GetSegmentName(entry.Key));
            }
            pool.Free();
        }


        private string GetSegmentName(int segmentId)
        {
            return FileName + "." + segmentId;
        }

        private static uint GetSectorSize(string filename)
        {
#if NETSTANDARD
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

            var logReadHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Read, FileShare.ReadWrite, 512, fo);

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

            var logWriteHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.Write, FileShare.ReadWrite, 512, fo);

            if (preallocateFile && segmentSize != -1)
                SetFileSize(logWriteHandle, segmentSize);

            return logWriteHandle;
        }

        private (FixedPool<Stream>, FixedPool<Stream>) AddHandle(int _segmentId)
        {
            return (new FixedPool<Stream>(8, () => CreateReadHandle(_segmentId)), new FixedPool<Stream>(8, () => CreateWriteHandle(_segmentId)));
        }

        private (FixedPool<Stream>, FixedPool<Stream>) GetOrAddHandle(int _segmentId)
        {
            if (logHandles.TryGetValue(_segmentId, out var h))
            {
                return h;
            }
            var result = logHandles.GetOrAdd(_segmentId, e => AddHandle(e));

            if (_disposed)
            {
                // If disposed, dispose the fixed pools and return the (disposed) result
                foreach (var entry in logHandles)
                {
                    entry.Value.Item1.Dispose();
                    entry.Value.Item2.Dispose();
                    if (deleteOnClose)
                        File.Delete(GetSegmentName(entry.Key));
                }
            }
            return result;
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

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
        private readonly SafeConcurrentDictionary<int, (FixedPool<Stream>, FixedPool<Stream>)> logHandles;
        private readonly SectorAlignedBufferPool pool;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;

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

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            logHandles = new SafeConcurrentDictionary<int, (FixedPool<Stream>, FixedPool<Stream>)>();
            if (recoverDevice)
                RecoverFiles();
        }

        /// <inheritdoc />
        public override bool Throttle() => numPending > 120;

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
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            Stream logReadHandle = null;
            int offset = -1;
            FixedPool<Stream> streampool = null;

            streampool = GetOrAddHandle(segmentId).Item1;
            (logReadHandle, offset) = streampool.Get();

            logReadHandle.Seek((long)sourceAddress, SeekOrigin.Begin);
            Interlocked.Increment(ref numPending);

#if NETSTANDARD21
            var umm = new UnmanagedMemoryManager<byte>((byte*)destinationAddress, (int)readLength);
            logReadHandle.ReadAsync(umm.Memory).AsTask()
#else
            SectorAlignedMemory memory = pool.Get((int)readLength);
            logReadHandle.ReadAsync(memory.buffer, 0, (int)readLength)
#endif
                .ContinueWith(t =>
                {
                    Interlocked.Decrement(ref numPending);

                    uint errorCode = 0;
                    if (!t.IsFaulted)
                    {
#if !NETSTANDARD21
                        fixed (void* source = memory.buffer)
                        {
                            Buffer.MemoryCopy(source, (void*)destinationAddress, readLength, readLength);
                        }
#endif
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
#if !NETSTANDARD21
                    memory.Return();
#endif

                    // Sequentialize all reads from same handle on non-windows
#if DOTNETCORE
                    if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        if (offset >= 0) streampool?.Return(offset);
                    }
#endif

                    callback(errorCode, (uint)t.Result, context);
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

            Stream logWriteHandle = null;
            int offset = -1;
            FixedPool<Stream> streampool = null;

            streampool = GetOrAddHandle(segmentId).Item2;
            (logWriteHandle, offset) = streampool.Get();

            logWriteHandle.Seek((long)destinationAddress, SeekOrigin.Begin);
            Interlocked.Increment(ref numPending);

#if NETSTANDARD21
            var umm = new UnmanagedMemoryManager<byte>((byte*)sourceAddress, (int)numBytesToWrite);
            logWriteHandle.WriteAsync(umm.Memory).AsTask()
#else
            SectorAlignedMemory memory = pool.Get((int)numBytesToWrite);
            fixed (void* destination = memory.buffer)
            {
                Buffer.MemoryCopy((void*)sourceAddress, destination, numBytesToWrite, numBytesToWrite);
            }
            logWriteHandle.WriteAsync(memory.buffer, 0, (int)numBytesToWrite)
#endif
                .ContinueWith(t =>
                {
                    Interlocked.Decrement(ref numPending);

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
#if !NETSTANDARD21
                    memory.Return();
#endif

                    // Sequentialize all writes to same handle on non-windows
#if DOTNETCORE
                    if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        ((FileStream)logWriteHandle).Flush(true);
                        if (offset >= 0) streampool?.Return(offset);
                    }
#endif

                    callback(errorCode, numBytesToWrite, context);
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

        private (FixedPool<Stream>, FixedPool<Stream>) AddHandle(int _segmentId)
        {
            return (new FixedPool<Stream>(8, () => CreateReadHandle(_segmentId)), new FixedPool<Stream>(8, () => CreateWriteHandle(_segmentId)));
        }

        private (FixedPool<Stream>, FixedPool<Stream>) GetOrAddHandle(int _segmentId)
        {
            return logHandles.GetOrAdd(_segmentId, e => AddHandle(e));
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


#if NETSTANDARD21
    /// <summary>
    /// A MemoryManager over a raw pointer
    /// </summary>
    /// <remarks>The pointer is assumed to be fully unmanaged, or externally pinned - no attempt will be made to pin this data</remarks>
    internal sealed unsafe class UnmanagedMemoryManager<T> : MemoryManager<T>
        where T : unmanaged
    {
        private readonly T* _pointer;
        private readonly int _length;

        /// <summary>
        /// Create a new UnmanagedMemoryManager instance at the given pointer and size
        /// </summary>
        /// <remarks>It is assumed that the span provided is already unmanaged or externally pinned</remarks>
        public UnmanagedMemoryManager(Span<T> span)
        {
            fixed (T* ptr = &MemoryMarshal.GetReference(span))
            {
                _pointer = ptr;
                _length = span.Length;
            }
        }
        /// <summary>
        /// Create a new UnmanagedMemoryManager instance at the given pointer and size
        /// </summary>
        public UnmanagedMemoryManager(T* pointer, int length)
        {
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
            _pointer = pointer;
            _length = length;
        }
        /// <summary>
        /// Obtains a span that represents the region
        /// </summary>
        public override Span<T> GetSpan() => new Span<T>(_pointer, _length);

        /// <summary>
        /// Provides access to a pointer that represents the data (note: no actual pin occurs)
        /// </summary>
        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if (elementIndex < 0 || elementIndex >= _length)
                throw new ArgumentOutOfRangeException(nameof(elementIndex));
            return new MemoryHandle(_pointer + elementIndex);
        }
        /// <summary>
        /// Has no effect
        /// </summary>
        public override void Unpin() { }

        /// <summary>
        /// Releases all resources associated with this object
        /// </summary>
        protected override void Dispose(bool disposing) { }
    }
#endif

}

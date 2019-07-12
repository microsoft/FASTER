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
    /// Managed device using .NET streams
    /// </summary>
    public class ManagedLocalStorageDevice : StorageDeviceBase
    {
        private readonly bool preallocateFile;
        private readonly bool deleteOnClose;
        private readonly ConcurrentDictionary<int, Stream> logHandles;
        private SectorAlignedBufferPool pool;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="preallocateFile"></param>
        /// <param name="deleteOnClose"></param>
        /// <param name="capacity">The maximal number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        public ManagedLocalStorageDevice(string filename, bool preallocateFile = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED)
            : base(filename, GetSectorSize(filename), capacity)
        {
            pool = new SectorAlignedBufferPool(1, 1);

            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            logHandles = new ConcurrentDictionary<int, Stream>();
        }


        class ReadCallbackWrapper
        {
            readonly IOCompletionCallback callback;
            readonly IAsyncResult asyncResult;
            SectorAlignedMemory memory;
            readonly IntPtr destinationAddress;
            readonly uint readLength;

            public ReadCallbackWrapper(IOCompletionCallback callback, IAsyncResult asyncResult, SectorAlignedMemory memory, IntPtr destinationAddress, uint readLength)
            {
                this.callback = callback;
                this.asyncResult = asyncResult;
                this.memory = memory;
                this.destinationAddress = destinationAddress;
                this.readLength = readLength;
            }

            public unsafe void Callback(IAsyncResult result)
            {
                fixed (void* source = memory.buffer)
                {
                    Buffer.MemoryCopy(source, (void*)destinationAddress, readLength, readLength);
                }
                memory.Return();
                Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
                callback(0, 0, ov.UnsafePack(callback, IntPtr.Zero));
            }
        }

        class WriteCallbackWrapper
        {
            readonly IOCompletionCallback callback;
            readonly IAsyncResult asyncResult;
            SectorAlignedMemory memory;

            public WriteCallbackWrapper(IOCompletionCallback callback, IAsyncResult asyncResult, SectorAlignedMemory memory)
            {
                this.callback = callback;
                this.asyncResult = asyncResult;
                this.memory = memory;
            }

            public unsafe void Callback(IAsyncResult result)
            {
                memory.Return();
                Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
                callback(0, 0, ov.UnsafePack(callback, IntPtr.Zero));
            }
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
            var memory = pool.Get((int)readLength);
            logHandle.Seek((long)sourceAddress, SeekOrigin.Begin);
            logHandle.BeginRead(memory.buffer, 0, (int)readLength,
                new ReadCallbackWrapper(callback, asyncResult, memory, destinationAddress, readLength).Callback, null);
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
            var memory = pool.Get((int)numBytesToWrite);

            fixed (void* destination = memory.buffer)
            {
                Buffer.MemoryCopy((void*)sourceAddress, destination, numBytesToWrite, numBytesToWrite);
            }
            logHandle.Seek((long)destinationAddress, SeekOrigin.Begin);
            logHandle.BeginWrite(memory.buffer, 0, (int)numBytesToWrite,
                new WriteCallbackWrapper(callback, asyncResult, memory).Callback, null);
        }

        public override void RemoveSegment(int segment)
        {
            if (logHandles.TryRemove(segment, out Stream logHandle))
            {
                logHandle.Dispose();
                File.Delete(GetSegmentName(segment));
            }
        }

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
                logHandle.Dispose();
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

        private Stream CreateHandle(int segmentId)
        {
            FileOptions fo = FileOptions.WriteThrough;
            fo |= FileOptions.Asynchronous;
            if (deleteOnClose)
                fo |= FileOptions.DeleteOnClose;

            var logHandle = new FileStream(
                GetSegmentName(segmentId), FileMode.OpenOrCreate,
                FileAccess.ReadWrite, FileShare.ReadWrite, 4096, fo);
                
            if (preallocateFile && segmentSize != -1)
                SetFileSize(FileName, logHandle, segmentSize);

            return logHandle;
        }

        private Stream GetOrAddHandle(int _segmentId)
        {
            return logHandles.GetOrAdd(_segmentId, segmentId => CreateHandle(segmentId));
        }

        /// <summary>
        /// Sets file size to the specified value.
        /// Does not reset file seek pointer to original location.
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="logHandle"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private bool SetFileSize(string filename, Stream logHandle, long size)
        {
            logHandle.SetLength(size);
            return true;
        }
    }
}

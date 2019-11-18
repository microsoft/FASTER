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

#if DOTNETCORE
using Mono.Unix.Native;
#endif

namespace FASTER.core
{

    /// <summary>
    /// Managed device using .NET streams
    /// </summary>
    public class ManagedLocalStorageDevice : StorageDeviceBase
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




        class ReadCallbackWrapper
        {
            uint errorCode;
            readonly Stream logHandle;
            readonly IOCompletionCallback callback;
            readonly IAsyncResult asyncResult;
            readonly SectorAlignedMemory memory;
            readonly IntPtr destinationAddress;
            readonly uint readLength;

            public ReadCallbackWrapper(Stream logHandle, IOCompletionCallback callback, IAsyncResult asyncResult, SectorAlignedMemory memory, IntPtr destinationAddress, uint readLength, uint errorCode)
            {
                this.logHandle = logHandle;
                this.callback = callback;
                this.asyncResult = asyncResult;
                this.memory = memory;
                this.destinationAddress = destinationAddress;
                this.readLength = readLength;
                this.errorCode = errorCode;
            }

            public unsafe void Callback(IAsyncResult result)
            {
                if (errorCode == 0)
                {
                    try
                    {
                        logHandle.EndRead(result);
                        fixed (void* source = memory.buffer)
                        {
                            Buffer.MemoryCopy(source, (void*)destinationAddress, readLength, readLength);
                        }
                    }
                    catch (IOException e)
                    {
                        errorCode = (uint)(e.HResult & 0x0000FFFF);
                    }
                    catch
                    {
                        // Non-IO exception; assign error code of max value
                        errorCode = uint.MaxValue;
                    }
                }

                memory.Return();
                Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
                callback(errorCode, 0, ov.UnsafePack(callback, IntPtr.Zero));
            }
        }

        class WriteCallbackWrapper
        {
            readonly Stream logHandle;
            readonly IOCompletionCallback callback;
            readonly IAsyncResult asyncResult;
            readonly SectorAlignedMemory memory;
            uint errorCode;

            public WriteCallbackWrapper(Stream logHandle, IOCompletionCallback callback, IAsyncResult asyncResult, SectorAlignedMemory memory, uint errorCode)
            {
                this.callback = callback;
                this.asyncResult = asyncResult;
                this.memory = memory;
                this.logHandle = logHandle;
                this.errorCode = errorCode;
            }

            public unsafe void Callback(IAsyncResult result)
            {
                if (errorCode == 0)
                {
                    try
                    {

/*                        logHandle.EndWrite(result);
#if DOTNETCORE
                        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        {
                            ((FileStream)logHandle).Flush(true);
                        }
#endif
*/
                    }
                    catch (IOException e)
                    {
                        errorCode = (uint)(e.HResult & 0x0000FFFF);
                    }
                    catch
                    {
                        // Non-IO exception; assign error code of max value
                        errorCode = uint.MaxValue;
                    }
                }

                memory.Return();
                Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
                callback(errorCode, 0, ov.UnsafePack(callback, IntPtr.Zero));
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
            ReadCallbackWrapper wrapper = null;
            SectorAlignedMemory memory = null;
            Stream logReadHandle = null;
            int offset = -1;
            FixedPool<Stream> streampool = null;

            try
            {
                memory = pool.Get((int)readLength);
                streampool = GetOrAddHandle(segmentId).Item1;
                (logReadHandle, offset) = streampool.Get();
                wrapper = new ReadCallbackWrapper(logReadHandle, callback, asyncResult, memory, destinationAddress, readLength, 0);
                lock (logReadHandle)
                {
                    logReadHandle.Seek((long)sourceAddress, SeekOrigin.Begin);
                    logReadHandle.BeginRead(memory.buffer, 0, (int)readLength, wrapper.Callback, null);
                }
            }
            catch (IOException e)
            {
                wrapper = new ReadCallbackWrapper(logReadHandle, callback, asyncResult, memory, destinationAddress, readLength, (uint)(e.HResult & 0x0000FFFF));
                wrapper.Callback(asyncResult);
            }
            catch
            {
                wrapper = new ReadCallbackWrapper(logReadHandle, callback, asyncResult, memory, destinationAddress, readLength, uint.MaxValue);
                wrapper.Callback(asyncResult);
            }
            finally
            {
                if (offset >= 0)
                    streampool?.Return(offset);
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
            WriteCallbackWrapper wrapper = null;
            SectorAlignedMemory memory = null;
            Stream logWriteHandle = null;
            int offset = -1;
            FixedPool<Stream> streampool = null;

            try
            {
                memory = pool.Get((int)numBytesToWrite);
                streampool = GetOrAddHandle(segmentId).Item2;
                (logWriteHandle, offset) = streampool.Get();
                wrapper = new WriteCallbackWrapper(logWriteHandle, callback, asyncResult, memory, 0);

                fixed (void* destination = memory.buffer)
                {
                    Buffer.MemoryCopy((void*)sourceAddress, destination, numBytesToWrite, numBytesToWrite);
                }

                // lock (logWriteHandle)
                {
                    logWriteHandle.Seek((long)destinationAddress, SeekOrigin.Begin);
                    logWriteHandle.Write(memory.buffer, 0, (int)numBytesToWrite);
                    ((FileStream)logWriteHandle).Flush(true);
                }
                wrapper.Callback(asyncResult);
            }
            catch (IOException e)
            {
                wrapper = new WriteCallbackWrapper(logWriteHandle, callback, asyncResult, memory, (uint)(e.HResult & 0x0000FFFF));
                wrapper.Callback(asyncResult);
            }
            catch
            {
                wrapper = new WriteCallbackWrapper(logWriteHandle, callback, asyncResult, memory, uint.MaxValue);
                wrapper.Callback(asyncResult);
            }
            finally
            {
                if (offset >= 0)
                    streampool?.Return(offset);
            }
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
                //(FileOptions)FILE_FLAG_NO_BUFFERING |
                //FileOptions.WriteThrough | 
                //FileOptions.Asynchronous |
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
                //(FileOptions)FILE_FLAG_NO_BUFFERING |
                //FileOptions.WriteThrough | 
                //FileOptions.Asynchronous |
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
            (new FixedPool<Stream>(1, () => CreateReadHandle(_segmentId)),
             new FixedPool<Stream>(1, () => CreateWriteHandle(_segmentId))));
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

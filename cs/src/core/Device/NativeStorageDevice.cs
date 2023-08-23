// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    struct NativeResult
    {
        public DeviceIOCompletionCallback callback;
        public object context;
    }

    /// <summary>
    /// Native version of local storage device
    /// </summary>
    public unsafe class NativeStorageDevice : StorageDeviceBase
    {
        const int MaxResults = 1 << 12;

        private static uint sectorSize = 512;
        private readonly ConcurrentQueue<int> freeResults = new();
        NativeResult[] results;

        /// <summary>
        /// Number of pending reads on device
        /// </summary>
        private int numPending = 0;

        #region Native storage interface
        /// <summary>
        /// Async callback delegate
        /// </summary>
        public delegate void AsyncIOCallback(IntPtr context, int result, ulong bytesTransferred);
        readonly IntPtr nativeDevice;

        [DllImport("native_device", EntryPoint = "NativeDevice_Create", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr NativeDevice_Create(string file, bool enablePrivileges, bool unbuffered, bool delete_on_close);

        [DllImport("native_device", EntryPoint = "NativeDevice_Destroy", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_Destroy(IntPtr device);

        [DllImport("native_device", EntryPoint = "NativeDevice_sector_size", CallingConvention = CallingConvention.Cdecl)]
        static extern uint NativeDevice_sector_size(IntPtr device);

        [DllImport("native_device", EntryPoint = "NativeDevice_ReadAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_ReadAsync(IntPtr device, ulong source, IntPtr dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport("native_device", EntryPoint = "NativeDevice_WriteAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_WriteAsync(IntPtr device, IntPtr source, ulong dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport("native_device", EntryPoint = "NativeDevice_CreateDir", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_CreateDir(IntPtr device, string dir);

        [DllImport("native_device", EntryPoint = "NativeDevice_TryComplete", CallingConvention = CallingConvention.Cdecl)]
        static extern bool NativeDevice_TryComplete(IntPtr device);

        [DllImport("native_device", EntryPoint = "NativeDevice_QueueRun", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_QueueRun(IntPtr device, int timeout_secs);
        #endregion

        readonly AsyncIOCallback _callbackDelegate;
        readonly CancellationTokenSource completionThreadToken = new();
        int numCompletionThreads;

        void _callback(IntPtr context, int errorCode, ulong numBytes)
        {
            Interlocked.Decrement(ref numPending);
            var result = results[(int)context];
            result.callback((uint)errorCode, (uint)numBytes, result.context);
            freeResults.Enqueue((int)context);
        }

        /// <inheritdoc />
        public override bool Throttle() => numPending > ThrottleLimit;

        /// <summary>
        /// Constructor with more options for derived classes
        /// </summary>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="numCompletionThreads">Number of IO completion threads</param>
        protected internal NativeStorageDevice(string filename,
                                      bool deleteOnClose = false,
                                      bool disableFileBuffering = true,
                                      long capacity = Devices.CAPACITY_UNSPECIFIED, int numCompletionThreads = 1)
                : base(filename, GetSectorSize(filename), capacity)
        {
            _callbackDelegate = _callback;

            if (filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new FasterException($"Path {filename} is too long");

            ThrottleLimit = 120;

            string path = new FileInfo(filename).Directory.FullName;
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            this.nativeDevice = NativeDevice_Create(filename, false, disableFileBuffering, deleteOnClose);
            this.results = new NativeResult[MaxResults];
            this.numCompletionThreads = numCompletionThreads;
            for (int i = 0; i < numCompletionThreads; i++)
            {
                var thread = new Thread(CompletionWorker)
                {
                    IsBackground = true
                };
                thread.Start();
            }
        }

        /// <inheritdoc />
        public override void Reset()
        {
        }

        int resultOffset;

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
            int offset;
            while (!freeResults.TryDequeue(out offset))
            {
                if (resultOffset < MaxResults)
                {
                    offset = Interlocked.Increment(ref resultOffset) - 1;
                    if (offset < MaxResults) break;
                }
                Thread.Yield();
            }
            ref var result = ref results[offset];
            result.context = context;
            result.callback = callback;

            try
            {
                Interlocked.Increment(ref numPending);
                int _result = NativeDevice_ReadAsync(nativeDevice, ((ulong)segmentId << segmentSizeBits) | sourceAddress, destinationAddress, readLength, _callbackDelegate, (IntPtr)offset);
                    
                if (_result != 0)
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
                freeResults.Enqueue(offset);
            }
            catch
            {
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
                freeResults.Enqueue(offset);
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
            int offset;
            while (!freeResults.TryDequeue(out offset))
            {
                if (resultOffset < MaxResults)
                {
                    offset = Interlocked.Increment(ref resultOffset) - 1;
                    if (offset < MaxResults) break;
                }
                Thread.Yield();
            }
            ref var result = ref results[offset];
            result.context = context;
            result.callback = callback;

            try
            {
                Interlocked.Increment(ref numPending);
                int _result = NativeDevice_WriteAsync(nativeDevice, sourceAddress, ((ulong)segmentId << segmentSizeBits) | destinationAddress, numBytesToWrite, _callbackDelegate, (IntPtr)offset);

                if (_result != 0)
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
            }
            catch
            {
                Interlocked.Decrement(ref numPending);
                callback(uint.MaxValue, 0, context);
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
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
            completionThreadToken.Cancel();
            NativeDevice_Destroy(nativeDevice);
            while (Interlocked.CompareExchange(ref numCompletionThreads, int.MinValue, 0) != 0) Thread.Yield();
            completionThreadToken.Dispose();
        }

        /// <inheritdoc/>
        public override bool TryComplete()
        {
            return NativeDevice_TryComplete(nativeDevice);
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            return segmentSize;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="segmentId"></param>
        /// <returns></returns>
        protected string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        private static uint GetSectorSize(string filename)
        {
            return sectorSize;
        }

        void CompletionWorker()
        {
            try
            {
                while (true)
                {
                    if (completionThreadToken.IsCancellationRequested) break;
                    NativeDevice_QueueRun(nativeDevice, 5);
                    Thread.Yield();
                }
            }
            finally
            {
                Interlocked.Decrement(ref numCompletionThreads);
            }
        }
    }
}

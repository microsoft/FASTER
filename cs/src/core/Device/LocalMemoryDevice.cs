// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    class IORequestLocalMemory {
        public readonly bool isRead;
        public readonly int segmentId;
        public readonly ulong deviceAddress;
        public readonly IntPtr appAddress;
        public readonly uint bytes;
        public readonly DeviceIOCompletionCallback callback;
        public readonly object asyncResult;

        public IORequestLocalMemory(bool isRead_, int segmentId_, ulong deviceAddress_, IntPtr appAddress_, uint bytes_, DeviceIOCompletionCallback callback_, object asyncResult_)
        {
            isRead = isRead_;
            segmentId = segmentId_;
            deviceAddress = deviceAddress_;
            appAddress = appAddress_;
            bytes = bytes_;
            callback = callback_;
            asyncResult = asyncResult_;
        }
    }

    /// <summary>
    /// Local storage device
    /// </summary>
    public sealed class LocalMemoryDevice : StorageDeviceBase
    {
        private readonly List<byte[]> ram_segments = new List<byte[]>();
        private readonly int num_segments;
        private readonly ConcurrentQueue<IORequestLocalMemory>[] ioQueue;
        private readonly Thread[] ioProcessors;
        private readonly int queueDepth;
        private bool terminated;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="sz_segment">The size of each segment</param>
        /// <param name="queueDepth_">The depth of the IO queue</param>
        /// <param name="parallelism">Number of IO processing threads</param>
        public LocalMemoryDevice(long capacity, long sz_segment, int queueDepth_, int parallelism)
            :base("/userspace/ram/storage", GetSectorSize("/userspace/ram/storage"), capacity)
        {
            if (capacity == Devices.CAPACITY_UNSPECIFIED) throw new Exception("Local memory device must have a capacity!");
            Console.WriteLine("LocalMemoryDevice: creating a " + capacity + " size local memory device.");
            num_segments = (int) (capacity / sz_segment);
            Console.WriteLine("LocalMemoryDevice: # of segments = " + num_segments);
            for (int i = 0; i < num_segments; i++)
            {
                byte[] new_segment = new byte[sz_segment];
                ram_segments.Add(new_segment);
            }
            terminated = false;
            ioQueue = new ConcurrentQueue<IORequestLocalMemory>[parallelism];
            ioProcessors = new Thread[parallelism];
            for (int i = 0; i != parallelism; i++)
            {
                var x = i;
                ioQueue[x] = new ConcurrentQueue<IORequestLocalMemory>();
                ioProcessors[i] = new Thread(() => this.ProcessIOQueue(ioQueue[x]));
                ioProcessors[i].Start();
            }

            queueDepth = queueDepth_;

            Console.WriteLine("LocalMemoryDevice: " + ram_segments.Count + " in-memory segments are created, each with " + sz_segment);
        }

        private unsafe void ProcessIOQueue(ConcurrentQueue<IORequestLocalMemory> q)
        {
            while (terminated == false) {
                if (q.TryDequeue(out IORequestLocalMemory req))
                {
                    if (req.isRead)
                    {
                        fixed (void* src_addr = &ram_segments[req.segmentId % num_segments][req.deviceAddress])
                        {
                            Buffer.MemoryCopy(src_addr, req.appAddress.ToPointer(), req.bytes, req.bytes);
                        }

                        req.callback(0, req.bytes, req.asyncResult);
                    }
                    else
                    {
                        fixed (void* dst_addr = &(ram_segments[req.segmentId % num_segments][req.deviceAddress]))
                        {
                            Buffer.MemoryCopy(req.appAddress.ToPointer(), dst_addr, req.bytes, req.bytes);
                        }
                        req.callback(0, req.bytes, req.asyncResult);
                    }
                }
                else
                    Thread.Yield();
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
                                     DeviceIOCompletionCallback callback,
                                     object asyncResult)
        {
            var q = ioQueue[segmentId % ioQueue.Length];
            while (q.Count >= queueDepth) { }
            IORequestLocalMemory req = new IORequestLocalMemory(true, segmentId, sourceAddress, destinationAddress, readLength, callback, asyncResult);
            q.Enqueue(req);
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
                                      DeviceIOCompletionCallback callback,
                                      object asyncResult)
        {
            var q = ioQueue[segmentId % ioQueue.Length];
            while (q.Count >= queueDepth) { }
            IORequestLocalMemory req = new IORequestLocalMemory(false, segmentId, destinationAddress, sourceAddress, numBytesToWrite, callback, asyncResult);
            q.Enqueue(req);
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            Array.Clear(ram_segments[segment % num_segments], 0, ram_segments[segment % num_segments].Length);
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

        // It may be somewhat inefficient to use the default async calls from the base class when the underlying
        // method is inherently synchronous. But just for delete (which is called infrequently and off the 
        // critical path) such inefficiency is probably negligible.

        /// <summary>
        /// Close device
        /// </summary>
        public override void Close()
        {
            foreach (var q in ioQueue)
                while (q.Count != 0) { }
            terminated = true;
            for (int i = 0; i != ioProcessors.Length; i++)
            {
                ioProcessors[i].Join();
            }
            ram_segments.Clear();
        }


        private static uint GetSectorSize(string filename)
        {
            return 512;
        }
    }
}

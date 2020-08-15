// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    unsafe struct IORequestLocalMemory {
        public void* srcAddress;
        public void* dstAddress;
        public uint bytes;
        public DeviceIOCompletionCallback callback;
        public object context;
    }

    /// <summary>
    /// Local storage device
    /// </summary>
    public unsafe sealed class LocalMemoryDevice : StorageDeviceBase
    {
        readonly byte*[] ram_segments;
        readonly GCHandle[] ram_segment_handles;

        private readonly int num_segments;
        private readonly ConcurrentQueue<IORequestLocalMemory>[] ioQueue;
        private readonly Thread[] ioProcessors;
        private readonly int parallelism;
        private bool terminated;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="sz_segment">The size of each segment</param>
        /// <param name="parallelism">Number of IO processing threads</param>
        /// <param name="sector_size">Sector size for device (default 64)</param>
        public LocalMemoryDevice(long capacity, long sz_segment, int parallelism, uint sector_size = 64)
            :base("/userspace/ram/storage", sector_size, capacity)
        {
            if (capacity == Devices.CAPACITY_UNSPECIFIED) throw new Exception("Local memory device must have a capacity!");
            Console.WriteLine("LocalMemoryDevice: Creating a " + capacity + " sized local memory device.");
            num_segments = (int) (capacity / sz_segment);
            
            ram_segments = new byte*[num_segments];
            ram_segment_handles = new GCHandle[num_segments];

            for (int i = 0; i < num_segments; i++)
            {
                var new_segment = new byte[sz_segment];
                
                ram_segment_handles[i] = GCHandle.Alloc(new_segment, GCHandleType.Pinned);
                ram_segments[i] = (byte*)(long)ram_segment_handles[i].AddrOfPinnedObject();
            }
            terminated = false;
            ioQueue = new ConcurrentQueue<IORequestLocalMemory>[parallelism];
            this.parallelism = parallelism;
            ioProcessors = new Thread[parallelism];
            for (int i = 0; i != parallelism; i++)
            {
                var x = i;
                ioQueue[x] = new ConcurrentQueue<IORequestLocalMemory>();
                ioProcessors[i] = new Thread(() => this.ProcessIOQueue(ioQueue[x]));
                ioProcessors[i].Start();
            }

            Console.WriteLine("LocalMemoryDevice: " + ram_segments.Length + " pinned in-memory segments created, each with " + sz_segment + " bytes");
        }

        private void ProcessIOQueue(ConcurrentQueue<IORequestLocalMemory> q)
        {
            while (terminated == false) {
                while (q.TryDequeue(out IORequestLocalMemory req))
                {
                    Buffer.MemoryCopy(req.srcAddress, req.dstAddress, req.bytes, req.bytes);
                    req.callback(0, req.bytes, req.context);
                }
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
        /// <param name="context"></param>
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            var q = ioQueue[segmentId % parallelism];
            var req = new IORequestLocalMemory
            {
                srcAddress = ram_segments[segmentId] + sourceAddress,
                dstAddress = (void*)destinationAddress,
                bytes = readLength,
                callback = callback,
                context = context
            };
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
        /// <param name="context"></param>
        public override void WriteAsync(IntPtr sourceAddress,
                                      int segmentId,
                                      ulong destinationAddress,
                                      uint numBytesToWrite,
                                      DeviceIOCompletionCallback callback,
                                      object context)
        {
            var q = ioQueue[segmentId % parallelism];
            var req = new IORequestLocalMemory
            {
                srcAddress = (void*)sourceAddress,
                dstAddress = ram_segments[segmentId] + destinationAddress,
                bytes = numBytesToWrite,
                callback = callback,
                context = context
            };
            q.Enqueue(req);
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
        public override void Close()
        {
            foreach (var q in ioQueue)
                while (q.Count != 0) { }
            terminated = true;
            for (int i = 0; i != ioProcessors.Length; i++)
            {
                ioProcessors[i].Join();
            }
            for (int i = 0; i < ram_segment_handles.Length; i++)
            {
                ram_segment_handles[i].Free();
                ram_segments[i] = null;
            }
        }
    }
}

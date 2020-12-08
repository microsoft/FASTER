// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Sector aligned memory allocator
    /// </summary>
    public unsafe sealed class SectorAlignedMemory
    {
        /// <summary>
        /// Actual buffer
        /// </summary>
        public byte[] buffer;

        /// <summary>
        /// Handle
        /// </summary>
        internal GCHandle handle;

        /// <summary>
        /// Offset
        /// </summary>
        public int offset;

        /// <summary>
        /// Aligned pointer
        /// </summary>
        public byte* aligned_pointer;

        /// <summary>
        /// Valid offset
        /// </summary>
        public int valid_offset;

        /// <summary>
        /// Required bytes
        /// </summary>
        public int required_bytes;

        /// <summary>
        /// Available bytes
        /// </summary>
        public int available_bytes;

        internal int level;
        internal SectorAlignedBufferPool pool;

        /// <summary>
        /// Default constructor
        /// </summary>
        public SectorAlignedMemory() { }

        /// <summary>
        /// Create new instance of SectorAlignedMemory
        /// </summary>
        /// <param name="numRecords"></param>
        /// <param name="sectorSize"></param>
        public SectorAlignedMemory(int numRecords, int sectorSize)
        {
            int recordSize = 1;
            int requiredSize = sectorSize + (((numRecords) * recordSize + (sectorSize - 1)) & ~(sectorSize - 1));

            buffer = new byte[requiredSize];
            handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            aligned_pointer = (byte*)(((long)handle.AddrOfPinnedObject() + (sectorSize - 1)) & ~(sectorSize - 1));
            offset = (int)((long)aligned_pointer - (long)handle.AddrOfPinnedObject());
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            handle.Free();
            buffer = null;
        }

        /// <summary>
        /// Return
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return()
        {
            pool.Return(this);
        }

        /// <summary>
        /// Get valid pointer
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* GetValidPointer()
        {
            return aligned_pointer + valid_offset;
        }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{0} {1} {2} {3} {4}", (long)aligned_pointer, offset, valid_offset, required_bytes, available_bytes);
        }
    }

    /// <summary>
    /// SectorAlignedBufferPool is a pool of memory. 
    /// Internally, it is organized as an array of concurrent queues where each concurrent
    /// queue represents a memory of size in particular range. queue[i] contains memory 
    /// segments each of size (2^i * sectorSize).
    /// </summary>
    public sealed class SectorAlignedBufferPool
    {
        /// <summary>
        /// Disable buffer pool
        /// </summary>
        public static bool Disabled = false;

        private const int levels = 32;
        private readonly int recordSize;
        private readonly int sectorSize;
        private readonly ConcurrentQueue<SectorAlignedMemory>[] queue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="recordSize">Record size. May be 1 if allocations of different lengths will be made</param>
        /// <param name="sectorSize">Sector size, e.g. from log device</param>
        public SectorAlignedBufferPool(int recordSize, int sectorSize)
        {
            queue = new ConcurrentQueue<SectorAlignedMemory>[levels];
            this.recordSize = recordSize;
            this.sectorSize = sectorSize;
        }

        /// <summary>
        /// Return
        /// </summary>
        /// <param name="page"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(SectorAlignedMemory page)
        {
            Debug.Assert(queue[page.level] != null);
            page.available_bytes = 0;
            page.required_bytes = 0;
            page.valid_offset = 0;
            Array.Clear(page.buffer, 0, page.buffer.Length);
            if (!Disabled)
                queue[page.level].Enqueue(page);
            else
            {
                page.handle.Free();
                page.buffer = null;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Position(int v)
        {
            if (v == 1) return 0;
            v--;

            int r = 0; // r will be lg(v)
            while (true) // unroll for more speed...
            {
                v >>= 1;
                if (v == 0) break;
                r++;
            }
            return r + 1;
        }

        /// <summary>
        /// Get buffer
        /// </summary>
        /// <param name="numRecords"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe SectorAlignedMemory Get(int numRecords)
        {
            int requiredSize = sectorSize + (((numRecords) * recordSize + (sectorSize - 1)) & ~(sectorSize - 1));
            int index = Position(requiredSize / sectorSize);
            if (queue[index] == null)
            {
                var localPool = new ConcurrentQueue<SectorAlignedMemory>();
                Interlocked.CompareExchange(ref queue[index], localPool, null);
            }

            if (!Disabled && queue[index].TryDequeue(out SectorAlignedMemory page))
            {
                return page;
            }

            page = new SectorAlignedMemory
            {
                level = index,
                buffer = new byte[sectorSize * (1 << index)]
            };
            page.handle = GCHandle.Alloc(page.buffer, GCHandleType.Pinned);
            page.aligned_pointer = (byte*)(((long)page.handle.AddrOfPinnedObject() + (sectorSize - 1)) & ~(sectorSize - 1));
            page.offset = (int) ((long)page.aligned_pointer - (long)page.handle.AddrOfPinnedObject());
            page.pool = this;
            return page;
        }

        /// <summary>
        /// Free buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free()
        {
            for (int i = 0; i < levels; i++)
            {
                if (queue[i] == null) continue;
                while (queue[i].TryDequeue(out SectorAlignedMemory result))
                {
                    result.handle.Free();
                    result.buffer = null;
                }
            }
        }

        /// <summary>
        /// Print pool contents
        /// </summary>
        public void Print()
        {
            for (int i = 0; i < levels; i++)
            {
                if (queue[i] == null) continue;
                foreach (var item in queue[i])
                {
                    Console.WriteLine("  " + item.ToString());
                }
            }
        }
    }
}

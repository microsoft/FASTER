// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Sector aligned memory allocator
    /// </summary>
    public unsafe struct SectorAlignedMemory
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
        internal NativeSectorAlignedBufferPool pool;

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
    }

    /// <summary>
    /// NativeSectorAlignedBufferPool is a pool of memory. 
    /// Internally, it is organized as an array of concurrent queues where each concurrent
    /// queue represents a memory of size in particular range. queue[i] contains memory 
    /// segments each of size (2^i * sectorSize).
    /// </summary>
    public class NativeSectorAlignedBufferPool
    {
        private const int levels = 32;
        private readonly int recordSize;
        private readonly int sectorSize;
        private readonly ConcurrentQueue<SectorAlignedMemory>[] queue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="recordSize"></param>
        /// <param name="sectorSize"></param>
        public NativeSectorAlignedBufferPool(int recordSize, int sectorSize)
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
        public unsafe void Return(SectorAlignedMemory page)
        {
            Debug.Assert(queue[page.level] != null);
            queue[page.level].Enqueue(page);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Position(int v)
        {
            if (v == 1) return 0;
            v--;

            int r = 0; // r will be lg(v)
            while (true) // unroll for more speed...
            {
                v = v >> 1;
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

            if (queue[index].TryDequeue(out SectorAlignedMemory page))
            {
                return page;
            }

            page.level = index;
            page.buffer = new byte[sectorSize * (1 << index)];
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
    }
}

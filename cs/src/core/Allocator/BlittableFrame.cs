// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// A frame is an in-memory circular buffer of log pages
    /// </summary>
    internal sealed class BlittableFrame : IDisposable
    {
        public readonly int frameSize, pageSize, sectorSize;
        public readonly byte[][] frame;
#if !NET5_0_OR_GREATER
        public readonly GCHandle[] handles;
#endif
        public readonly long[] pointers;

        public BlittableFrame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            frame = new byte[frameSize][];
#if !NET5_0_OR_GREATER
            handles = new GCHandle[frameSize];
#endif
            pointers = new long[frameSize];
        }

        public unsafe void Allocate(int index)
        {
            var adjustedSize = pageSize + 2 * sectorSize;

#if NET5_0_OR_GREATER
            byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
            long p = (long)Unsafe.AsPointer(ref tmp[0]);
#else
            byte[] tmp = new byte[adjustedSize];
            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
#endif
            Array.Clear(tmp, 0, adjustedSize);
            pointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
            frame[index] = tmp;
        }

        public void Clear(int pageIndex)
        {
            Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
        }

        public long GetPhysicalAddress(long frameNumber, long offset)
        {
            return pointers[frameNumber % frameSize] + offset;
        }

        public void Dispose()
        {
#if !NET5_0_OR_GREATER
            for (int i = 0; i < frameSize; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
            }
#endif
        }
    }
}



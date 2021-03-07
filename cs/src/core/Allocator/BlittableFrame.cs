// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
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
        public GCHandle[] handles;
        public long[] pointers;

        public BlittableFrame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            frame = new byte[frameSize][];
            handles = new GCHandle[frameSize];
            pointers = new long[frameSize];
        }

        public void Allocate(int index)
        {
            var adjustedSize = pageSize + 2 * sectorSize;
            byte[] tmp = new byte[adjustedSize];
            Array.Clear(tmp, 0, adjustedSize);

            handles[index] = GCHandle.Alloc(tmp, GCHandleType.Pinned);
            long p = (long)handles[index].AddrOfPinnedObject();
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
            for (int i = 0; i < frameSize; i++)
            {
                if (handles[i] != default(GCHandle))
                    handles[i].Free();
                frame[i] = null;
                pointers[i] = 0;
            }
        }
    }
}



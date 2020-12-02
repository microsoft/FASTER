// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Threading;

namespace FASTER.core
{
    internal struct AsyncGetFromDiskResult<TContext>
    {
        public TContext context;
    }

    internal unsafe struct HashIndexPageAsyncFlushResult
    {
        public int chunkIndex;
        public SectorAlignedMemory mem;
    }

    internal unsafe struct HashIndexPageAsyncReadResult
    {
        public int chunkIndex;
    }

    internal struct OverflowPagesFlushAsyncResult
    {
        public SectorAlignedMemory mem;
    }

    internal struct OverflowPagesReadAsyncResult
    {
    }
}

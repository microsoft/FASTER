// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;

namespace FASTER.core
{
    /// <summary>
    /// Async IO context for PMM
    /// </summary>
    public unsafe struct AsyncIOContext
    {
        /// <summary>
        /// Id
        /// </summary>
        public long id;

        /// <summary>
        /// Key pointer
        /// </summary>
        public IntPtr key;

        /// <summary>
        /// Logical address
        /// </summary>
        public long logicalAddress;

        /// <summary>
        /// Record buffer
        /// </summary>
        public SectorAlignedMemory record;

        /// <summary>
        /// Object buffer
        /// </summary>
        public SectorAlignedMemory objBuffer;

        /// <summary>
        /// Callback queue
        /// </summary>
        public BlockingCollection<AsyncIOContext> callbackQueue;
    }
}

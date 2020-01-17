// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Async IO context for PMM
    /// </summary>
    public unsafe struct AsyncIOContext<Key, Value>
    {
        /// <summary>
        /// Id
        /// </summary>
        public long id;

        /// <summary>
        /// Key
        /// </summary>
        public IHeapContainer<Key> request_key;

        /// <summary>
        /// Retrieved key
        /// </summary>
        public Key key;

        /// <summary>
        /// Retrieved value
        /// </summary>
        public Value value;

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
        public AsyncQueue<AsyncIOContext<Key, Value>> callbackQueue;

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // Do not dispose request_key as it is a shallow copy
            // of the key in pendingContext
            record.Return();
        }
    }

    internal class SimpleReadContext : IAsyncResult
    {
        public long logicalAddress;
        public SectorAlignedMemory record;
        public SemaphoreSlim completedRead;

        public object AsyncState => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();

        public bool IsCompleted => throw new NotImplementedException();
    }
}

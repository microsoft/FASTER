// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

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
        /// Async Operation ValueTask backer
        /// </summary>
        public TaskCompletionSource<AsyncIOContext<Key, Value>> asyncOperation;

        /// <summary>
        /// Indicates whether this is a default instance with no pending operation
        /// </summary>
        public bool IsDefault() => this.callbackQueue is null && this.asyncOperation is null;

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

    internal sealed class SimpleReadContext
    {
        public long logicalAddress;
        public SectorAlignedMemory record;
        public SemaphoreSlim completedRead;
    }
}

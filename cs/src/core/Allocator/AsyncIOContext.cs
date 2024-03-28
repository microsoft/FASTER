// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
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
        /// Minimum Logical address to resolve Key in
        /// </summary>
        public long minAddress;

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
        /// Synchronous completion event
        /// </summary>
        internal AsyncIOContextCompletionEvent<Key, Value> completionEvent;

        /// <summary>
        /// Indicates whether this is a default instance with no pending operation
        /// </summary>
        public readonly bool IsDefault() => this.callbackQueue is null && this.asyncOperation is null && this.completionEvent is null;

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // Do not dispose request_key as it is a shallow copy of the key in pendingContext
            record?.Return();
            record = null;
        }
    }

    // Wrapper class so we can communicate back the context.record even if it has to retry due to incomplete records.
    internal class AsyncIOContextCompletionEvent<Key, Value> : IDisposable
    {
        internal SemaphoreSlim semaphore;
        internal Exception exception;
        internal AsyncIOContext<Key, Value> request;

        internal AsyncIOContextCompletionEvent()
        {
            this.semaphore = new SemaphoreSlim(0);
            this.request.id = -1;
            this.request.minAddress = Constants.kInvalidAddress;
            this.request.completionEvent = this;
        }

        internal void Prepare(IHeapContainer<Key> request_key, long logicalAddress)
        {
            this.request.Dispose();
            this.request.request_key = request_key;
            this.request.logicalAddress = logicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(ref AsyncIOContext<Key, Value> ctx)
        {
            this.request.Dispose();
            this.request = ctx;
            this.exception = null;
            this.semaphore.Release(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetException(Exception ex)
        {
            this.request.Dispose();
            this.request = default;
            this.exception = ex;
            this.semaphore.Release(1);
        }

        internal void Wait(CancellationToken token = default) => this.semaphore.Wait(token);

        /// <inheritdoc/>
        public void Dispose()
        {
            this.request.Dispose();
            this.semaphore?.Dispose();
        }
    }

    internal sealed class SimpleReadContext
    {
        public long logicalAddress;
        public SectorAlignedMemory record;
        public SemaphoreSlim completedRead;
    }
}

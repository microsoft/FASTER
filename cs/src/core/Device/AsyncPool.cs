// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Asynchronous pool of fixed pre-filled capacity
    /// Supports sync get (TryGet) for fast path
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AsyncPool<T> : IDisposable where T : IDisposable
    {
        readonly int size;
        private readonly Func<T> creator;
        readonly SemaphoreSlim handleAvailable;
        readonly ConcurrentQueue<T> itemQueue;
        bool disposed = false;
        int disposedCount = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="size"></param>
        /// <param name="creator"></param>
        public AsyncPool(int size, Func<T> creator)
        {
            this.size = size;
            this.creator = creator;
            this.handleAvailable = new SemaphoreSlim(initialCount: 1, maxCount: size);
            this.itemQueue = new ConcurrentQueue<T>();
            this.itemQueue.Enqueue(creator());
        }

        /// <summary>
        /// Get item
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask<T> GetAsync(CancellationToken token = default)
        {
            for (; ; )
            {
                await handleAvailable.WaitAsync(token);

                if (disposed)
                    throw new FasterException("Getting handle in disposed device");

                if (!itemQueue.TryDequeue(out T item))
                {
                    item = creator();
                }

                return item;
            }
        }

        /// <summary>
        /// Try get item
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryGet(out T item)
        {
            if (disposed)
            {
                item = default;
                return false;
            }

            if (!handleAvailable.Wait(0))
            {
                item = default;
                return false;
            }

            if (!itemQueue.TryDequeue(out item))
            {
                handleAvailable.Release();
                item = default;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Return item to pool
        /// </summary>
        /// <param name="item"></param>
        public void Return(T item)
        {
            if (this.disposed)
            {
                item.Dispose();
            }
            else
            {
                itemQueue.Enqueue(item);
                handleAvailable.Release();
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            disposed = true;

            while (itemQueue.TryDequeue(out var item))
            {
                item.Dispose();
            }
        }
    }
}

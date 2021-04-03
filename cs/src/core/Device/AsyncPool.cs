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
    class AsyncPool<T> : IDisposable where T : IDisposable
    {
        readonly int size;
        bool disposed = false;
        SemaphoreSlim handleAvailable;
        ConcurrentQueue<T> itemQueue;
        int disposedCount = 0;

        public AsyncPool(int size, Func<T> creator)
        {
            this.size = 1;
            this.handleAvailable = new SemaphoreSlim(size);
            this.itemQueue = new ConcurrentQueue<T>();
            for (int i = 0; i < size; i++)
                itemQueue.Enqueue(creator());
        }

        public async Task<T> GetAsync(CancellationToken token = default)
        {
            for (; ; )
            {
                if (disposed)
                    throw new FasterException("Getting handle in disposed device");

                await handleAvailable.WaitAsync(token);
                if (itemQueue.TryDequeue(out T item))
                    return item;
            }
        }

        public bool TryGet(out T item)
        {
            if (disposed)
            {
                item = default;
                return false;
            }
            return itemQueue.TryDequeue(out item);
        }

        public void Return(T item)
        {
            itemQueue.Enqueue(item);
            handleAvailable.Release();
        }

        public void Dispose()
        {
            disposed = true;

            while (disposedCount < size)
            {
                while (itemQueue.TryDequeue(out var item))
                {
                    item.Dispose();
                    disposedCount++;
                }
                if (disposedCount < size)
                    handleAvailable.Wait();
            }
        }
    }
}

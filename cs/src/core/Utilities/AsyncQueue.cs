// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;

namespace FASTER.core
{
    /// <summary>
    /// Async queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AsyncQueue<T>
    {
        private readonly SemaphoreSlim semaphore;
        private readonly ConcurrentQueue<T> queue;

        /// <summary>
        /// Queue count
        /// </summary>
        public int Count => queue.Count;

        /// <summary>
        /// Constructor
        /// </summary>
        public AsyncQueue()
        {
            semaphore = new SemaphoreSlim(0);
            queue = new ConcurrentQueue<T>();
        }

        /// <summary>
        /// Enqueue item
        /// </summary>
        /// <param name="item"></param>
        public void Enqueue(T item)
        {
            queue.Enqueue(item);
            semaphore.Release();
        }

        /// <summary>
        /// Async dequeue
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<T> DequeueAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            for (; ; )
            {
                await semaphore.WaitAsync(cancellationToken);

                if (queue.TryDequeue(out T item))
                {
                    return item;
                }
            }
        }

        /// <summary>
        /// Try dequeue (if item exists)
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryDequeue(out T item)
        {
            return queue.TryDequeue(out item);
        }
    }
}

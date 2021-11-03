// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Shared work queue that ensures one worker at any given time. Uses LIFO ordering of work.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    class WorkQueueLIFO<T> : IDisposable
    {
        const int kMaxQueueSize = 1 << 30;
        readonly ConcurrentStack<T> _queue;
        readonly Action<T> _work;
        int _count;

        public WorkQueueLIFO(Action<T> work)
        {
            _queue = new ConcurrentStack<T>();
            _work = work;
            _count = 0;
        }

        public void Dispose()
        {
            while (_count != 0)
                Thread.Sleep(10);
        }

        /// <summary>
        /// Enqueue work item, take ownership of draining the work queue
        /// if needed
        /// </summary>
        /// <param name="work">Work to enqueue</param>
        /// <param name="asTask">Process work as separate task</param>
        public void EnqueueAndTryWork(T work, bool asTask)
        {
            Interlocked.Increment(ref _count);
            _queue.Push(work);

            // Try to take over work queue processing if needed
            while (true)
            {
                int count = _count;
                if (count >= kMaxQueueSize) return;
                if (Interlocked.CompareExchange(ref _count, count + kMaxQueueSize, count) == count)
                    break;
            }

            if (asTask)
                _ = Task.Run(() => ProcessQueue());
            else
                ProcessQueue();
        }

        private void ProcessQueue()
        {
            // Process items in work queue
            while (true)
            {
                while (_queue.TryPop(out var workItem))
                {
                    try
                    {
                        _work(workItem);
                    }
                    catch { }
                    Interlocked.Decrement(ref _count);
                }

                int count = _count;
                if (count != kMaxQueueSize) continue;
                if (Interlocked.CompareExchange(ref _count, 0, count) == count)
                    break;
            }
        }
    }
}
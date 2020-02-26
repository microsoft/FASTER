using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Offers reactivity about when a counter reaches zero
    /// </summary>
    public sealed class AsyncCountDown<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
        where TKey: struct
    {

        ConcurrentDictionary<TKey, TValue> queue;
        TaskCompletionSource<int> tcs;
        object lockObject;

        /// <summary>
        /// Yep
        /// </summary>
        public AsyncCountDown()
        {
            lockObject = new object();
            queue = new ConcurrentDictionary<TKey, TValue>();
        }

        private void SetTaskCompletionSource()
        {
            tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        }


        /// <summary>
        /// Increments the counter by 1
        /// </summary>
        public void Add(TKey key, TValue value)
        {
            if (!queue.TryAdd(key, value))
                throw new Exception($"{nameof(AsyncCountDown<TKey, TValue>)} key was not unique");
        }

        

        /// <summary>
        /// Decrements the counter by 1
        /// </summary>
        public void Remove(TKey key)
        {
            queue.TryRemove(key, out _);

            if (queue.IsEmpty)
            {
                lock (lockObject)
                {
                    if (tcs == null)
                        return;

                    tcs.TrySetResult(0);
                    tcs = null;
                }
            }
        }


        /// <summary>
        /// Provides a way to execute a continuation when the counter reaches zero
        /// </summary>
        /// <returns>A Task that completes when the counter reaches zero</returns>
        public Task WaitEmptyAsync()
        {
            if (IsEmpty)
                return Task.CompletedTask;

            lock (lockObject)
            {
                if (IsEmpty)
                    return Task.CompletedTask;

                if (tcs == null)
                    tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

                return tcs.Task;
            }
        }
        
        /// <summary>
        /// Returns false is the counter is zero
        /// </summary>
        public bool IsEmpty 
        { 
            get
            {
                return queue.IsEmpty;
            }
        }

        

        /// <summary>
        /// Returns an enumerator that iterates through the items waiting for completion.
        /// </summary>
        /// <returns>An enumerator for the the items waiting for completion.</returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => queue.GetEnumerator();


        IEnumerator IEnumerable.GetEnumerator() => queue.GetEnumerator();
        
    }
}

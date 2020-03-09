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
        where TKey : struct
    {

        ConcurrentDictionary<TKey, TValue> queue;
        TaskCompletionSource<int> tcs;
        TaskCompletionSource<int> nextTcs;

        /// <summary>
        /// Yep
        /// </summary>
        public AsyncCountDown()
        {
            queue = new ConcurrentDictionary<TKey, TValue>();
            nextTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                TryCompleteAwaitingTask();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TryCompleteAwaitingTask()
        {
            //Complete the Task
            Volatile.Read(ref tcs)?.TrySetResult(0);

            //Reset TCS, so next awaiters produce a new one
            Interlocked.Exchange(ref tcs, null);            
        }


        /// <summary>
        /// Provides a way to execute a continuation when the counter reaches zero
        /// </summary>
        /// <returns>A Task that completes when the counter reaches zero</returns>
        public Task WaitEmptyAsync()
        {
            if (IsEmpty)
                return Task.CompletedTask;

            return GetOrCreateTaskCompletionSource();

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Task GetOrCreateTaskCompletionSource()
        {

            //if tcs is not null, we'll get it in taskSource
            var taskSource = Interlocked.CompareExchange(ref tcs, nextTcs, null);

            if (taskSource == null) 
            {
                //tcs was null and nextTcs got assigned to it. 
                taskSource = nextTcs;

                //We need a new nextTcs
                nextTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            if (IsEmpty)
                return Task.CompletedTask;

            return taskSource.Task;
        }

        /// <summary>
        /// Returns false is the counter is zero
        /// </summary>
        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

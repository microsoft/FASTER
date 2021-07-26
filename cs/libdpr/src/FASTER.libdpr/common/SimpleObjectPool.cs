using System;
using System.Collections.Concurrent;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A simple object pool that caches objects for reuse. Thread-safe
    /// </summary>
    /// <typeparam name="T"> type of project cached </typeparam>
    public class SimpleObjectPool<T> where T : class
    {
        private readonly Func<T> factory;
        private readonly ConcurrentQueue<T> freeObjects;

        /// <summary>
        ///     Creates a new simple object pool.
        /// </summary>
        /// <param name="factory"> method used to create new objects </param>
        public SimpleObjectPool(Func<T> factory)
        {
            this.factory = factory;
            freeObjects = new ConcurrentQueue<T>();
        }

        /// <summary>
        ///     Gets a new (reused) object
        /// </summary>
        /// <returns>object of type T</returns>
        public T Checkout()
        {
            return !freeObjects.TryDequeue(out var obj) ? factory() : obj;
        }

        /// <summary>
        ///     Returns a used object for future use
        /// </summary>
        /// <param name="obj">object to return</param>
        public void Return(T obj)
        {
            freeObjects.Enqueue(obj);
        }
    }
}
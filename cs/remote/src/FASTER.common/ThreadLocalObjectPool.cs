using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.common
{

    /// <summary>
    ///     An object pool that optimizes for use cases where objects are reused on the same thread. Thread-safe.
    ///     There will be memory utilization problems if checkout and return pairs are not called on the same thread.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ThreadLocalObjectPool<T> : IDisposable where T : class
    {
        // TODO(Tianyu): Replace with FastThreadLocal if performance is an issue
        private readonly ThreadLocal<SimpleObjectPool<T>> objects;

        /// <summary>
        ///     Constructs a new object pool
        /// </summary>
        /// <param name="factory"> method used to create new objects of type T </param>
        /// <param name="maxObjectsPerThread">
        /// Max number of objects that will be retained and recycled in this object pool per thread.
        /// Objects exceeding this count are created and destroyed on demand
        /// </param>
        /// <param name="destructor"> method used to dispose retained objects when they go out of scope. WARNING: NOT invoked on retained objects before reuse </param>
        public ThreadLocalObjectPool(Func<T> factory, int maxObjectPerThread = 128, Action<T> destructor = null)
        {
            objects = new ThreadLocal<SimpleObjectPool<T>>(
                () => new SimpleObjectPool<T>(factory, maxObjectPerThread, destructor), true);
        }
        
        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            foreach (var pool in objects.Values)
                pool.Dispose();
        }


        /// <summary>
        ///     Gets a new (reused) object
        /// </summary>
        /// <returns>object of type T</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Checkout()
        {
            return objects.Value.Checkout();
        }

        /// <summary>
        ///     Returns a used object for future use
        /// </summary>
        /// <param name="obj">object to return</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(T obj)
        {
            objects.Value.Return(obj);
        }
    }
}
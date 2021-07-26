using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.libdpr
{
    internal class LightConcurrentStack<T> where T : class
    {
        // Not expecting a lot of concurrency on the stack. Should be pretty cheap.
        private SpinLock latch;
        private readonly T[] stack;
        private int tail;

        internal LightConcurrentStack(int maxCapacity = 128)
        {
            stack = new T[maxCapacity];
            tail = 0;
            latch = new SpinLock();
        }

        internal bool TryPush(T elem)
        {
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            if (tail == stack.Length)
            {
                latch.Exit();
                return false;
            }

            stack[tail++] = elem;
            latch.Exit();
            return true;
        }

        internal bool TryPop(out T elem)
        {
            elem = null;
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            if (tail == 0)
            {
                latch.Exit();
                return false;
            }

            elem = stack[--tail];
            latch.Exit();
            return true;
        }
    }

    /// <summary>
    ///     An object pool that optimizes for use cases where objects are reused on the same thread. Thread-safe.
    ///     There will be memory utilization problems if checkout and return pairs are not called on the same thread.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ThreadLocalObjectPool<T> where T : class
    {
        private readonly Func<T> factory;
        private readonly ThreadLocal<LightConcurrentStack<T>> objects;

        /// <summary>
        ///     Constructs a new object pool
        /// </summary>
        /// <param name="factory">method used to create new objects</param>
        /// <param name="maxObjectPerThread"> maximum of reused objects per thread</param>
        public ThreadLocalObjectPool(Func<T> factory, int maxObjectPerThread = 128)
        {
            this.factory = factory;
            objects = new ThreadLocal<LightConcurrentStack<T>>(() => new LightConcurrentStack<T>(maxObjectPerThread));
        }

        /// <summary>
        ///     Gets a new (reused) object
        /// </summary>
        /// <returns>object of type T</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Checkout()
        {
            return !objects.Value.TryPop(out var obj) ? factory() : obj;
        }

        /// <summary>
        ///     Returns a used object for future use
        /// </summary>
        /// <param name="obj">object to return</param>
        public void Return(T obj)
        {
            objects.Value.TryPush(obj);
        }
    }
}
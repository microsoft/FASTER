// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.common
{
    /// <summary>
    /// Object pool
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SimpleObjectPool<T> : IDisposable where T : class
    {
        private readonly Func<T> factory;
        private readonly Action<T> destructor;
        private readonly LightConcurrentStack<T> stack;
        private int allocatedObjects;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory"> method used to create new objects of type T </param>
        /// <param name="destructor"> method used to dispose retained objects when they go out of scope. WARNING: NOT invoked on retained objects before reuse </param>
        /// <param name="maxObjects">
        /// Max number of objects that will be retained and recycled in this object pool.
        /// Objects exceeding this count are created and destroyed on demand
        /// </param>
        public SimpleObjectPool(Func<T> factory, Action<T> destructor = null, int maxObjects = 128)
        {
            this.factory = factory;
            this.destructor = destructor;
            stack = new LightConcurrentStack<T>(maxObjects);
            allocatedObjects = 0;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            while (allocatedObjects > 0)
            {
                while (stack.TryPop(out var elem))
                {
                    destructor?.Invoke(elem);
                    Interlocked.Decrement(ref allocatedObjects);
                }
                Thread.Yield();
            }
        }

        /// <summary>
        /// Checkout item
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Checkout()
        {
            if (!stack.TryPop(out var obj))
            {
                Interlocked.Increment(ref allocatedObjects);
                return factory();
            }
            return obj;
        }

        /// <summary>
        /// Return item
        /// </summary>
        /// <param name="obj"></param>
        public void Return(T obj)
        {
            if (!stack.TryPush(obj))
            {
                destructor?.Invoke(obj);
                Interlocked.Decrement(ref allocatedObjects);
            }
        }
    }
}
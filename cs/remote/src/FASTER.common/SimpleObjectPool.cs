// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.common
{
    /// <summary>
    /// Object pool
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SimpleObjectPool<T> : IDisposable where T : class, IDisposable
    {
        private readonly Func<T> factory;
        private readonly LightConcurrentStack<T> stack;
        private int allocatedObjects;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory"></param>
        /// <param name="maxObjects"></param>
        public SimpleObjectPool(Func<T> factory, int maxObjects = 128)
        {
            this.factory = factory;
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
                    elem.Dispose();
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
                obj.Dispose();
                Interlocked.Decrement(ref allocatedObjects);
            }
        }
    }
}
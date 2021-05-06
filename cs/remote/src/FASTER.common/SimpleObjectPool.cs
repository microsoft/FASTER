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
    internal class SimpleObjectPool<T> : IDisposable where T : class, IDisposable
    {
        private readonly Func<T> factory;
        private readonly LightConcurrentStack<T> stack;
        private int allocatedObjects;
        private readonly int maxObjects;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory"></param>
        /// <param name="maxObjects"></param>
        public SimpleObjectPool(Func<T> factory, int maxObjects = 128)
        {
            this.factory = factory;
            this.maxObjects = maxObjects;
            stack = new LightConcurrentStack<T>();
            allocatedObjects = 0;
        }

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReusableObject<T> Checkout()
        {
            if (!stack.TryPop(out var obj))
            {
                if (allocatedObjects < maxObjects)
                {
                    Interlocked.Increment(ref allocatedObjects);
                    return new ReusableObject<T>(factory(), stack);
                }
                // Overflow objects are simply discarded after use
                return new ReusableObject<T>(factory(), null);
            }
            return new ReusableObject<T>(obj, stack);
        }
    }
}
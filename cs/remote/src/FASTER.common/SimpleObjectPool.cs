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
    internal class SimpleObjectPool<T> : IDisposable where T : class
    {
        private readonly Func<T> factory;
        private readonly Action<T> destructor;
        private readonly LightConcurrentStack<T> stack;
        private int allocatedObjects;
        private readonly int maxObjects;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory"></param>
        /// <param name="destructor"></param>
        /// <param name="maxObjects"></param>
        public SimpleObjectPool(Func<T> factory, Action<T> destructor = null, int maxObjects = 128)
        {
            this.factory = factory;
            this.destructor = destructor;
            this.maxObjects = maxObjects;
            stack = new LightConcurrentStack<T>();
            allocatedObjects = 0;
        }

        public void Dispose()
        {
            while (stack.TryPop(out var elem))
            {
                destructor?.Invoke(elem);
                Interlocked.Decrement(ref allocatedObjects);
            }
            Debug.Assert(allocatedObjects == 0);
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
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Fast implementation of instance-thread-local variables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class FastThreadLocal<T>
    {
        // Max instances supported
        private const int kMaxInstances = 128;

        [ThreadStatic]
        private static T[] values;

        private readonly int id;
        private static readonly int[] instances = new int[kMaxInstances];

        public FastThreadLocal()
        {
            for (int i = 0; i < kMaxInstances; i++)
            {
                if (0 == Interlocked.CompareExchange(ref instances[i], 1, 0))
                {
                    id = i;
                    return;
                }
            }
            throw new Exception("Unsupported number of simultaneous instances");
        }

        public void InitializeThread()
        {
            if (values == null)
                values = new T[kMaxInstances];
        }

        public void DisposeThread()
        {
            Value = default(T);

            // Dispose values only if there are no other
            // instances active for this thread
            for (int i = 0; i < kMaxInstances; i++)
            {
                if ((instances[i] == 1) && (i != id))
                    return;
            }
            values = null;
        }

        /// <summary>
        /// Dispose instance for all threads
        /// </summary>
        public void Dispose()
        {
            instances[id] = 0;
        }

        public T Value
        {
            get { return values[id]; }
            set { values[id] = value; }
        }
    }
}

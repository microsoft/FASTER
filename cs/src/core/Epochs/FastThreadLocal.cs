// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net;
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
        [ThreadStatic]
        private static int t_iid;

        private readonly int id;
        private readonly int iid;

        private static readonly int[] instances = new int[kMaxInstances];
        private static int instanceId = 0;

        public FastThreadLocal()
        {
            iid = Interlocked.Increment(ref instanceId);

            for (int i = 0; i < kMaxInstances; i++)
            {
                if (0 == Interlocked.CompareExchange(ref instances[i], iid, 0))
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
            {
                values = new T[kMaxInstances];
            }
            if (t_iid != iid)
            {
                t_iid = iid;
                values[id] = default(T);
            }
        }

        public void DisposeThread()
        {
            values[id] = default(T);
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
            get => values[id];
            set => values[id] = value;
        }

        public bool IsInitializedForThread => (values != null) && (iid == t_iid);
    }
}

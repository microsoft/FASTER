// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.common
{
    /// <summary>
    /// Reusable object
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public struct ReusableObject<T> : IDisposable where T : class, IDisposable
    {
        private readonly LightConcurrentStack<T> pool;

        /// <summary>
        /// Object
        /// </summary>
        public T obj;

        internal ReusableObject(T obj, LightConcurrentStack<T> pool)
        {
            this.pool = pool;
            this.obj = obj;
        }

        /// <summary>
        /// Dispose instance
        /// </summary>
        public void Dispose()
        {
            if (pool != null)
                pool.TryPush(obj);
            else
                obj.Dispose();
        }
    }
}
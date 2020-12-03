// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.common
{
    public struct ReusableObject<T> : IDisposable where T : class
    {
        public static ReusableObject<T> INVALID = new ReusableObject<T>(null, null); 
        private readonly LightConcurrentStack<T> pool;
        public T obj;

        internal ReusableObject(T obj, LightConcurrentStack<T> pool)
        {
            this.pool = pool;
            this.obj = obj;
        }

        public void Dispose()
        {
            pool?.TryPush(obj);
        }
        
        public bool Equals(ReusableObject<T> other)
        {
            return EqualityComparer<T>.Default.Equals(obj, other.obj);
        }

        public override bool Equals(object obj)
        {
            return obj is ReusableObject<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            return EqualityComparer<T>.Default.GetHashCode(obj);
        }
    }
}
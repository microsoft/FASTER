using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.libdpr
{
    // TODO(Tianyu): Document
    public class SimpleObjectPool<T> where T : class
    {
        private Func<T> factory;
        private ConcurrentQueue<T> freeObjects;

        public SimpleObjectPool(Func<T> factory)
        {
            this.factory = factory;
            freeObjects = new ConcurrentQueue<T>();
        }
        
        public T Checkout()
        {
            return !freeObjects.TryDequeue(out var obj) ? factory() : obj;
        }

        public void Return(T obj)
        {
            freeObjects.Enqueue(obj);
        }
    }
}
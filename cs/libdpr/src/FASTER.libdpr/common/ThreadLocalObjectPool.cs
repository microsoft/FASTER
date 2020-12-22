using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.libdpr
{
    internal class LightConcurrentStack<T> where T : class
    {
        private T[] stack;
        private int tail;
        // Not expecting a lot of concurrency on the stack. Should be pretty cheap.
        private SpinLock latch;

        public LightConcurrentStack(int maxCapacity = 128)
        {
            stack = new T[maxCapacity];
            tail = 0;
            latch = new SpinLock();
        }
        
        public bool TryPush(T elem)
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

        public bool TryPop(out T elem)
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

    public class ThreadLocalObjectPool<T> where T : class
    {
        private Func<T> factory;
        private ThreadLocal<LightConcurrentStack<T>> objects;

        public ThreadLocalObjectPool(Func<T> factory, int maxObjectPerThread = 128)
        {
            this.factory = factory;
            objects = new ThreadLocal<LightConcurrentStack<T>>(() => new LightConcurrentStack<T>(maxObjectPerThread));
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Checkout()
        {
            return !objects.Value.TryPop(out var obj) ? factory() : obj;
        }

        public void Return(T obj)
        {
            objects.Value.TryPush(obj);
        }
    }
}
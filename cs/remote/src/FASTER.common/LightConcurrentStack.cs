// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;

namespace FASTER.common
{
    internal class LightConcurrentStack<T> where T : class
    {
        private readonly T[] stack;
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
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    class FixedPool<T> : IDisposable where T : IDisposable
    {
        readonly T[] items;
        readonly int[] owners;
        readonly int size;
        readonly Func<T> creator;
        bool disposed = false;

        public FixedPool(int size, Func<T> creator)
        {
            items = new T[size];
            owners = new int[size];
            this.size = size;
            this.creator = creator;
        }

        public (T, int) Get()
        {
            while (true)
            {
                for (int i = 0; i < size; i++)
                {
                    if (disposed)
                        throw new FasterException("Disposed");

                    var val = owners[i];
                    if (val == 0)
                    {
                        if (Interlocked.CompareExchange(ref owners[i], 2, val) == val)
                        {
                            try
                            {
                                items[i] = creator();
                            }
                            catch
                            {
                                Interlocked.Exchange(ref owners[i], val);
                                throw;
                            }

                            return (items[i], i);
                        }
                    }
                    else if (val == 1)
                    {
                        if (Interlocked.CompareExchange(ref owners[i], 2, val) == val)
                        {
                            return (items[i], i);
                        }
                    }
                }
            }
        }

        public void Return(int offset)
        {
            if (!disposed)
                Interlocked.CompareExchange(ref owners[offset], 1, 2);
            else
            {
                if (Interlocked.CompareExchange(ref owners[offset], -1, 2) == 2)
                    items[offset].Dispose();
            }
        }

        public void Dispose()
        {
            disposed = true;
            bool done = false;

            while (!done)
            {
                done = true;

                for (int i = 0; i < size; i++)
                {
                    var val = owners[i];
                    if (val == 0)
                    {
                        if (Interlocked.CompareExchange(ref owners[i], -1, val) != val)
                            done = false;
                    }
                    else if (val == 1)
                    {
                        done = false;
                        if (Interlocked.CompareExchange(ref owners[i], -1, val) == val)
                            items[i].Dispose();
                    }
                    else if (val == 2)
                    {
                        done = false;
                    }
                }
            }
        }
    }
}

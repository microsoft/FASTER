// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit)]
    struct AtomicOwner
    {
        [FieldOffset(0)]
        int owner;
        [FieldOffset(4)]
        int count;
        [FieldOffset(0)]
        long atomic;

        /// <summary>
        /// Enqueue token
        /// true: success + caller is new owner
        /// false: success + someone else is owner
        /// </summary>
        /// <returns></returns>
        public bool Enqueue()
        {
            while (true)
            {
                var older = this;
                var newer = older;
                newer.count++;
                if (older.owner == 0)
                    newer.owner = 1;

                if (Interlocked.CompareExchange(ref this.atomic, newer.atomic, older.atomic) == older.atomic)
                {
                    return older.owner == 0;
                }
            }
        }

        /// <summary>
        /// Dequeue token
        /// true: successful dequeue (caller is owner)
        /// false: failed dequeue
        /// </summary>
        /// <returns></returns>
        public bool Dequeue()
        {
            while (true)
            {
                var older = this;
                var newer = older;
                newer.count--;
                if (newer.count == 0)
                    newer.owner = 0;

                if (Interlocked.CompareExchange(ref this.atomic, newer.atomic, older.atomic) == older.atomic)
                {
                    return newer.owner != 0;
                }
            }
        }
    }
}

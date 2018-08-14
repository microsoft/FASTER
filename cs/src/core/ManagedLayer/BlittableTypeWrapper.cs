// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct BlittableTypeWrapper
    {
        [FieldOffset(0)]
        public long ptr;

        public ref T GetObject<T>()
        {
            return ref MallocFixedPageSize<T>.Instance.Get(ptr);
        }

        public static BlittableTypeWrapper Create<T>(T obj)
        {
            var ptr = MallocFixedPageSize<T>.Instance.Allocate();
            MallocFixedPageSize<T>.Instance.Set(ptr, ref obj);
            return new BlittableTypeWrapper { ptr = ptr };
        }

        public void Free<T>()
        {
            MallocFixedPageSize<T>.Instance.FreeAtEpoch(ptr);
            ptr = 0;
        }
    }
}

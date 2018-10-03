// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct BlittableTypeWrapper
    {
        public const int kSize = 8;

        [FieldOffset(0)]
        public IntPtr ptr;

        public ref T GetObject<T>()
        {
            return ref MallocFixedPageSize<T>.Instance.Get((long)ptr);
        }

        public static BlittableTypeWrapper Create<T>(T obj)
        {
            var ptr = MallocFixedPageSize<T>.Instance.Allocate();
            MallocFixedPageSize<T>.Instance.Set(ptr, ref obj);
            return new BlittableTypeWrapper { ptr = (IntPtr)ptr };
        }

        public void Free<T>()
        {
            MallocFixedPageSize<T>.Instance.FreeAtEpoch((long)ptr);
            ptr = IntPtr.Zero;
        }
    }
}

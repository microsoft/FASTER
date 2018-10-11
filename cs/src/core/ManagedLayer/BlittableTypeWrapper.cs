// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// Blittable type wrapper
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct BlittableTypeWrapper
    {
        /// <summary>
        /// Size of wrapper
        /// </summary>
        public const int kSize = 8;

        /// <summary>
        /// Actual pointer
        /// </summary>
        [FieldOffset(0)]
        public IntPtr ptr;

        /// <summary>
        /// Get object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public ref T GetObject<T>()
        {
            return ref MallocFixedPageSize<T>.Instance.Get((long)ptr);
        }

        /// <summary>
        /// Create object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static BlittableTypeWrapper Create<T>(T obj)
        {
            var ptr = MallocFixedPageSize<T>.Instance.Allocate();
            MallocFixedPageSize<T>.Instance.Set(ptr, ref obj);
            return new BlittableTypeWrapper { ptr = (IntPtr)ptr };
        }

        /// <summary>
        /// Free object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public void Free<T>()
        {
            MallocFixedPageSize<T>.Instance.FreeAtEpoch((long)ptr);
            ptr = IntPtr.Zero;
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Length specification for fixed size (normal) structs
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal readonly struct FixedLengthStruct<T> : IVariableLengthStruct<T>
    {
        private static readonly int size = Unsafe.SizeOf<T>();

        /// <summary>
        /// Get average length
        /// </summary>
        /// <returns></returns>
        public int GetInitialLength() => size;

        /// <summary>
        /// Get length
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public int GetLength(ref T t) => size;

        public unsafe void Serialize(ref T source, void* destination)
            => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source));

        public unsafe ref T AsRef(void* source) => ref Unsafe.AsRef<T>(source);
        public unsafe void Initialize(void* source, void* dest) { }
    }
}
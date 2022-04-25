// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// IVariableLengthStruct implementation for Memory{T} where T is unmanaged
    /// </summary>
    public class MemoryVarLenStruct<T> : IVariableLengthStruct<Memory<T>> where T : unmanaged
    {
        /// <inheritdoc/>
        public bool IsVariableLength => true;

        ///<inheritdoc/>
        public int GetInitialLength() => sizeof(int);

        ///<inheritdoc/>
        public unsafe int GetLength(ref Memory<T> t) => sizeof(int) + t.Length * sizeof(T);

        ///<inheritdoc/>
        public unsafe void Serialize(ref Memory<T> source, void* destination)
        {
            *(int*)destination = source.Length * sizeof(T);
            MemoryMarshal.Cast<T, byte>(source.Span)
                .CopyTo(new Span<byte>((byte*)destination + sizeof(int), source.Length * sizeof(T)));
        }

        [ThreadStatic]
        static (UnmanagedMemoryManager<T>, Memory<T>)[] refCache;

        [ThreadStatic]
        static int count;

        ///<inheritdoc/>
        public unsafe ref Memory<T> AsRef(void* source)
        {
            if (refCache == null)
            {
                refCache = new (UnmanagedMemoryManager<T>, Memory<T>)[4];
                for (int i = 0; i < 4; i++) refCache[i] = (new UnmanagedMemoryManager<T>(), default);
            }
            count = (count + 1) % 4;
            ref var cache = ref refCache[count];
            var len = *(int*)source;
            cache.Item1.SetDestination((T*)((byte*)source + sizeof(int)),  len / sizeof(T));
            cache.Item2 = cache.Item1.Memory;
            return ref cache.Item2;
        }

        /// <inheritdoc/>
        public unsafe void Initialize(void* source, void* end)
        {
            *(int*)source = (int)((long)end - (long)source) - sizeof(int);
        }
    }

    /// <summary>
    /// Input-specific IVariableLengthStruct implementation for Memory{T} where T is unmanaged, for Memory{T} as input
    /// </summary>
    public class MemoryVarLenStructForMemoryInput<T> : MemoryVarLenStruct<T>, IVariableLengthStruct<Memory<T>, Memory<T>> where T : unmanaged
    {
        /// <inheritdoc/>
        public bool IsVariableLengthInput => true;

        /// <inheritdoc/>
        public unsafe int GetInitialLength(ref Memory<T> input) => sizeof(int) + input.Length * sizeof(T);

        /// <inheritdoc/>
        public unsafe int GetLength(ref Memory<T> value, ref Memory<T> input)
            => sizeof(int) + (input.Length > value.Length ? input.Length : value.Length) * sizeof(T);

        /// <inheritdoc/>
        public unsafe int GetInputLength(ref Memory<T> input) => sizeof(int) + input.Length * sizeof(T);
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.


using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Heap container to store keys and values when they go pending
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IHeapContainer<T>
    {
        /// <summary>
        /// Get object
        /// </summary>
        /// <returns></returns>
        ref T Get();

        /// <summary>
        /// Dispose container
        /// </summary>
        void Dispose();
    }

    /// <summary>
    /// Heap container for standard C# objects (non-variable-length)
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class StandardHeapContainer<T> : IHeapContainer<T>
    {
        private T obj;

        public StandardHeapContainer(ref T obj)
        {
            this.obj = obj;
        }

        public ref T Get() => ref obj;

        public void Dispose() { }
    }

    /// <summary>
    /// Heap container for variable length structs
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class VarLenHeapContainer<T> : IHeapContainer<T>
    {
        private SectorAlignedMemory mem;


        public unsafe VarLenHeapContainer(ref T obj, IVariableLengthStruct<T> varLenStruct, SectorAlignedBufferPool pool)
        {
            var len = varLenStruct.GetLength(ref obj);
            mem = pool.Get(len);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref obj), mem.GetValidPointer(), len, len);
        }

        public unsafe ref T Get()
        {
            return ref Unsafe.AsRef<T>(mem.GetValidPointer());
        }

        public void Dispose()
        {
            mem.Return();
        }
    }
}

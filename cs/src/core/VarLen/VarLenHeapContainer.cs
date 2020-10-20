// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Heap container for variable length structs
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class VarLenHeapContainer<T> : IHeapContainer<T>
    {
        readonly SectorAlignedMemory mem;
        readonly IVariableLengthStruct<T> varLenStruct;

        public unsafe VarLenHeapContainer(ref T obj, IVariableLengthStruct<T> varLenStruct, SectorAlignedBufferPool pool)
        {
            this.varLenStruct = varLenStruct;
            var len = varLenStruct.GetLength(ref obj);
            mem = pool.Get(len);
            varLenStruct.Serialize(ref obj, mem.GetValidPointer());
        }

        public unsafe ref T Get()
        {
            return ref varLenStruct.AsRef(mem.GetValidPointer());
        }

        public void Dispose()
        {
            mem.Return();
        }
    }
}

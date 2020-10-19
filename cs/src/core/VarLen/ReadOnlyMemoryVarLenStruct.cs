
using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// IVariableLengthStruct implementation for ReadOnlyMemory&lt;T&gt; where T is unmanaged
    /// </summary>
    public class ReadOnlyMemoryVarLenStruct<T> : IVariableLengthStruct<ReadOnlyMemory<T>> where T : unmanaged
    {
        ///<inheritdoc/>
        public int GetInitialLength() => sizeof(int);

        ///<inheritdoc/>
        public unsafe int GetLength(ref ReadOnlyMemory<T> t) => sizeof(int) + t.Length * sizeof(T);

        ///<inheritdoc/>
        public unsafe void Serialize(ref ReadOnlyMemory<T> source, void* destination)
        {
            *(int*)destination = source.Length * sizeof(T);
            MemoryMarshal.Cast<T, byte>(source.Span)
                .CopyTo(new Span<byte>((byte*)destination + sizeof(int), source.Length * sizeof(T)));
        }

        [ThreadStatic]
        static UnmanagedMemoryManager<T> manager;

        [ThreadStatic]
        static ReadOnlyMemory<T>[] obj;

        [ThreadStatic]
        static int count;

        ///<inheritdoc/>
        public unsafe ref ReadOnlyMemory<T> AsRef(void* source)
        {
            if (manager == null)
            {
                manager = new UnmanagedMemoryManager<T>();
                obj = new ReadOnlyMemory<T>[4];
            }
            manager.SetDestination((T*)((byte*)source + sizeof(int)), (*(int*)source) / sizeof(T));
            count = (count + 1) % 4;
            obj[count] = manager.Memory;
            return ref obj[count];
        }

        ///<inheritdoc/>
        public unsafe void Initialize(void* source, void* end)
        {
            *(int*)source = (int)end - (int)source - sizeof(int);
        }
    }

}
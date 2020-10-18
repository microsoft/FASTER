
using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// IVariableLengthStruct implementation for Memory&lt;T&gt; where T is unmanaged
    /// </summary>
    public class MemoryVarLenStruct<T> : IVariableLengthStruct<Memory<T>> where T : unmanaged
    {
        ///<inheritdoc/>
        public int GetInitialLength()
        {
            return sizeof(int);
        }

        ///<inheritdoc/>
        public unsafe int GetLength(ref Memory<T> t)
        {
            return sizeof(int) + t.Length*sizeof(T);
        }

        ///<inheritdoc/>
        public unsafe void Serialize(ref Memory<T> source, void* destination)
        {
            *(int*)destination = source.Length*sizeof(T);
            MemoryMarshal.Cast<T, byte>(source.Span)
                .CopyTo(new Span<byte>((byte*)destination + sizeof(int), source.Length*sizeof(T)));
        }

        [ThreadStatic]
        static UnmanagedMemoryManager<T> manager;

        [ThreadStatic]
        static Memory<T>[] obj;

        [ThreadStatic]
        static int count;

        ///<inheritdoc/>
        public unsafe ref Memory<T> AsRef(void* source)
        {
            if (manager == null)
            {
                manager = new UnmanagedMemoryManager<T>();
                obj = new Memory<T>[4];
            }
            manager.SetDestination((T*)((byte*)source + sizeof(int)), (*(int*)source)/sizeof(T));
            count = (count + 1) % 4;
            obj[count] = manager.Memory;
            return ref obj[count];
        }

        ///<inheritdoc/>
        public unsafe ref Memory<T> AsRef(void* source, void* end)
        {
            if (manager == null)
            {
                manager = new UnmanagedMemoryManager<T>();
                obj = new Memory<T>[4];
            }
            int len = (int)end - (int)source - sizeof(int);
            *(int*)source = len;
            manager.SetDestination((T*)((byte*)source + sizeof(int)), len / sizeof(T));
            count = (count + 1) % 4;
            obj[count] = manager.Memory;
            return ref obj[count];
        }
    }
    /// <summary>
    /// IVariableLengthStruct implementation for ReadOnlyMemory&lt;T&gt; where T is unmanaged
    /// </summary>
    public class ReadOnlyMemoryVarLenStruct<T> : IVariableLengthStruct<ReadOnlyMemory<T>> where T : unmanaged
    {
        ///<inheritdoc/>
        public int GetInitialLength()
        {
            return sizeof(int);
        }

        ///<inheritdoc/>
        public unsafe int GetLength(ref ReadOnlyMemory<T> t)
        {
            return sizeof(int) + t.Length * sizeof(T);
        }

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
        public unsafe ref ReadOnlyMemory<T> AsRef(void* source, void* end)
        {
            if (manager == null)
            {
                manager = new UnmanagedMemoryManager<T>();
                obj = new ReadOnlyMemory<T>[4];
            }
            int len = (int)end - (int)source - sizeof(int);
            *(int*)source = len;
            manager.SetDestination((T*)((byte*)source + sizeof(int)), len / sizeof(T));
            count = (count + 1) % 4;
            obj[count] = manager.Memory;
            return ref obj[count];
        }
    }

}
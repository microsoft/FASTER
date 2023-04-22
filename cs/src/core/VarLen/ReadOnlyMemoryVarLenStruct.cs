
using System;
using System.Runtime.CompilerServices;
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
        static (UnmanagedMemoryManager<T>, ReadOnlyMemory<T>)[] manager;

        [ThreadStatic]
        static int count;

        /// <inheritdoc/>
        public unsafe ref ReadOnlyMemory<T> AsRef(void* source)
        {
            if (manager == null)
            {
                manager = new (UnmanagedMemoryManager<T>, ReadOnlyMemory<T>)[4];
                for (int i = 0; i < 4; i++) manager[i] = (new UnmanagedMemoryManager<T>(), default);
            }
            count = (count + 1) % 4;
            ref var cache = ref manager[count];
            cache.Item1.SetDestination((T*)((byte*)source + sizeof(int)), (*(int*)source) / sizeof(T));
            cache.Item2 = cache.Item1.Memory;
            return ref cache.Item2;
        }

        /// <inheritdoc/>
        public unsafe ReadOnlyMemory<T> AsValue(Memory<byte> source, void* sourcePtr)
            => Unsafe.As<Memory<byte>, ReadOnlyMemory<T>>(ref source).Slice(0, source.Length / sizeof(T));

        ///<inheritdoc/>
        public unsafe void Initialize(void* source, void* end)
        {
            *(int*)source = (int)((long)end - (long)source) - sizeof(int);
        }
    }

}

using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// IVariableLengthStruct implementation for ReadOnlyMemory&lt;T&gt; where T is unmanaged
    /// </summary>
    public class ReadOnlyMemoryVarLenStruct<T> : IVariableLengthStruct<ReadOnlyMemory<T>> where T : unmanaged
    {
        /// <inheritdoc/>
        public bool IsVariableLength => true;

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

        ///<inheritdoc/>
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

        ///<inheritdoc/>
        public unsafe void Initialize(void* source, void* end)
        {
            *(int*)source = (int)((long)end - (long)source) - sizeof(int);
        }
    }

    /// <summary>
    /// Input-specific IVariableLengthStruct implementation for Memory{T} where T is unmanaged, for Memory{T} as input
    /// </summary>
    public class MemoryVarLenStructForReadOnlyMemoryInput<T> : ReadOnlyMemoryVarLenStruct<T>, IVariableLengthStruct<Memory<T>, ReadOnlyMemory<T>> where T : unmanaged
    {
        /// <inheritdoc/>
        public bool IsVariableLengthInput => true;

        /// <inheritdoc/>
        public unsafe int GetInitialLength(ref ReadOnlyMemory<T> input) => sizeof(int) + input.Length * sizeof(T);

        /// <summary>
        /// Length of resulting object when doing RMW with given value and input. Here we set the length
        /// to the max of input and old value lengths. You can provide a custom implementation for other cases.
        /// </summary>
        public unsafe int GetLength(ref Memory<T> t, ref ReadOnlyMemory<T> input)
            => sizeof(int) + (input.Length > t.Length ? input.Length : t.Length) * sizeof(T);
    }
}
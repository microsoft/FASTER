
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
            var len = (*(int*)source) & ~IntExclusiveLocker.kHeaderMask;
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
    /// Input-specific IVariableLengthStruct implementation for Memory&lt;T&gt; where T is unmanaged, for Memory&lt;T&gt; as input
    /// </summary>
    public class MemoryVarLenStructForMemoryInput<T> : IVariableLengthStruct<Memory<T>, Memory<T>> where T : unmanaged
    {
        /// <inheritdoc/>
        public unsafe int GetInitialLength(ref Memory<T> input) => sizeof(int) + input.Length * sizeof(T);

        /// <inheritdoc/>
        public unsafe int GetLength(ref Memory<T> t, ref Memory<T> input)
            => sizeof(int) + (input.Length > t.Length ? input.Length : t.Length) * sizeof(T);
    }

    /// <summary>
    /// Input-specific IVariableLengthStruct implementation for Memory&lt;T&gt; where T is unmanaged, for Memory&lt;T&gt; as input
    /// </summary>
    public class MemoryVarLenStructForReadOnlyMemoryInput<T> : IVariableLengthStruct<Memory<T>, ReadOnlyMemory<T>> where T : unmanaged
    {
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
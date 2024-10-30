
using System;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// FASTER comparer for Memory&lt;T&gt;, for unmanaged type T
    /// </summary>
    public struct MemoryComparer<T> : IFasterEqualityComparer<Memory<T>>
        where T : unmanaged
    {
        ///<inheritdoc/>
        public readonly unsafe long GetHashCode64(ref Memory<T> k)
        {
            fixed (T* ptr = k.Span)
                return Utility.HashBytes((byte*)ptr, k.Length*sizeof(T));
        }

        ///<inheritdoc/>
        public readonly unsafe bool Equals(ref Memory<T> k1, ref Memory<T> k2)
        {
            return MemoryMarshal.Cast<T, byte>(k1.Span)
                .SequenceEqual(MemoryMarshal.Cast<T, byte>(k2.Span));
        }
    }

    /// <summary>
    /// FASTER comparer for ReadOnlyMemory&lt;T&gt;, for unmanaged type T
    /// </summary>
    public struct ReadOnlyMemoryComparer<T> : IFasterEqualityComparer<ReadOnlyMemory<T>>
        where T : unmanaged
    {
        ///<inheritdoc/>
        public readonly unsafe long GetHashCode64(ref ReadOnlyMemory<T> k)
        {
            fixed (T* ptr = k.Span)
                return Utility.HashBytes((byte*)ptr, k.Length * sizeof(T));
        }

        ///<inheritdoc/>
        public readonly unsafe bool Equals(ref ReadOnlyMemory<T> k1, ref ReadOnlyMemory<T> k2)
        {
            return MemoryMarshal.Cast<T, byte>(k1.Span)
                .SequenceEqual(MemoryMarshal.Cast<T, byte>(k2.Span));
        }
    }
}
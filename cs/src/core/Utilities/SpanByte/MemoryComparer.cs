
using System;

namespace FASTER.core
{
    /// <summary>
    /// FASTER comparer for Memory&lt;byte&gt;
    /// </summary>
    public struct MemoryComparer : IFasterEqualityComparer<Memory<byte>>
    {
        ///<inheritdoc/>
        public unsafe long GetHashCode64(ref Memory<byte> k)
        {
            fixed (byte* ptr = k.Span)
                return Utility.HashBytes(ptr, k.Length);
        }

        ///<inheritdoc/>
        public unsafe bool Equals(ref Memory<byte> k1, ref Memory<byte> k2)
        {
            return k1.Span.SequenceEqual(k2.Span);
        }
    }

    /// <summary>
    /// FASTER comparer for Memory&lt;byte&gt;
    /// </summary>
    public struct ReadOnlyMemoryComparer : IFasterEqualityComparer<ReadOnlyMemory<byte>>
    {
        ///<inheritdoc/>
        public unsafe long GetHashCode64(ref ReadOnlyMemory<byte> k)
        {
            fixed (byte* ptr = k.Span)
                return Utility.HashBytes(ptr, k.Length);
        }

        ///<inheritdoc/>
        public unsafe bool Equals(ref ReadOnlyMemory<byte> k1, ref ReadOnlyMemory<byte> k2)
        {
            return k1.Span.SequenceEqual(k2.Span);
        }
    }
}
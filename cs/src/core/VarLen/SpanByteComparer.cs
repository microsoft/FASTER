// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Equality comparer for SpanByte
    /// </summary>
    public readonly struct SpanByteComparer : IFasterEqualityComparer<SpanByte>
    {
        /// <inheritdoc />
        public readonly unsafe long GetHashCode64(ref SpanByte spanByte) => StaticGetHashCode64(ref spanByte);

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long StaticGetHashCode64(ref SpanByte spanByte)
        {
            if (spanByte.Serialized)
            {
                byte* ptr = (byte*)Unsafe.AsPointer(ref spanByte);
                return Utility.HashBytes(ptr + sizeof(int), spanByte.Length);
            }
            else
            {
                byte* ptr = (byte*)spanByte.Pointer;
                return Utility.HashBytes(ptr, spanByte.Length);
            }
        }

        /// <inheritdoc />
        public readonly unsafe bool Equals(ref SpanByte k1, ref SpanByte k2) => StaticEquals(ref k1, ref k2);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool StaticEquals(ref SpanByte k1, ref SpanByte k2)
        {
            return k1.AsReadOnlySpanWithMetadata().SequenceEqual(k2.AsReadOnlySpanWithMetadata())
                && (k1.MetadataSize == k2.MetadataSize);
        }
    }
}

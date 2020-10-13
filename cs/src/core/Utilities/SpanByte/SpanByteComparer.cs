// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Equality comparer for SpanByte
    /// </summary>
    public struct SpanByteComparer : IFasterEqualityComparer<SpanByte>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe long GetHashCode64(ref SpanByte spanByte)
        {
            if (spanByte.Serialized)
            {
                byte* ptr = (byte*)Unsafe.AsPointer(ref spanByte);
                return Utility.HashBytes(ptr + sizeof(int), *(int*)ptr);
            }
            else
            {
                byte* ptr = (byte*)spanByte.Pointer;
                return Utility.HashBytes(ptr, spanByte.Length);
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Equals(ref SpanByte k1, ref SpanByte k2)
        {
            return k1.AsReadOnlySpan().SequenceEqual(k2.AsReadOnlySpan());
        }
    }
}

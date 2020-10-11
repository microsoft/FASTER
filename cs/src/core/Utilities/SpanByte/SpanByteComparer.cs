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
            byte* ptr = (byte*)Unsafe.AsPointer(ref spanByte);
            return Utility.HashBytes(ptr, (*(int*)ptr) + sizeof(int));
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Equals(ref SpanByte k1, ref SpanByte k2)
        {
            return
                new ReadOnlySpan<byte>(Unsafe.AsPointer(ref k1), k1.length + sizeof(int))
                    .SequenceEqual(
                new ReadOnlySpan<byte>(Unsafe.AsPointer(ref k2), k2.length + sizeof(int)));
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using FASTER.core;
using FASTER.common;
using System;

namespace FASTER.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public unsafe sealed class SpanByteKeySerializer : IKeyInputSerializer<SpanByte, SpanByte>
    {
        readonly SpanByteVarLenStruct settings;

        /// <summary>
        /// Constructor
        /// </summary>
        public SpanByteKeySerializer()
        {
            settings = new SpanByteVarLenStruct();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadKeyByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += settings.GetLength(ref ret);
            return ref ret;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadInputByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += settings.GetLength(ref ret);
            return ref ret;
        }

        /// <inheritdoc />
        public bool Match(ref SpanByte k, bool asciiKey, ref SpanByte pattern, bool asciiPattern)
        {
            if (asciiKey && asciiPattern)
            {
                return GlobUtils.Match(pattern.ToPointer(), pattern.LengthWithoutMetadata, k.ToPointer(), k.LengthWithoutMetadata);
            }

            if (pattern.LengthWithoutMetadata > k.LengthWithoutMetadata)
                return false;
            return pattern.AsReadOnlySpan().SequenceEqual(k.AsReadOnlySpan().Slice(0, pattern.LengthWithoutMetadata));
        }
    }
}
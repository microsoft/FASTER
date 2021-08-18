// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using FASTER.core;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public unsafe sealed class SpanByteKeySerializer : IKeySerializer<SpanByte>
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
        public bool Match(ref SpanByte k, ref SpanByte pattern)
        {
            if (pattern.Length > k.Length)
                return false;

            byte[] kByte = k.ToByteArray();
            byte[] patternByte = pattern.ToByteArray();

            for (int i = 0; i < patternByte.Length; i++)
            {
                if (kByte[i] != patternByte[i])
                    return false;
            }

            return true;
        }
    }
}
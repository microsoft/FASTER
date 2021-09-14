// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public unsafe sealed class FixedLenKeySerializer<Key, Input> : IKeyInputSerializer<Key, Input>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLenKeySerializer()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Key ReadKeyByRef(ref byte* src)
        {
            var _src = (void*)src;
            src += Unsafe.SizeOf<Key>();
            return ref Unsafe.AsRef<Key>(_src);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Input ReadInputByRef(ref byte* src)
        {
            var _src = (void*)src;
            src += Unsafe.SizeOf<Input>();
            return ref Unsafe.AsRef<Input>(_src);
        }

        /// <inheritdoc />
        public bool Match(ref Key k, bool asciiKey, ref Key pattern, bool asciiPattern)
        {
            if (k.Equals(pattern))
                return true;
            return false;
        }
    }
}
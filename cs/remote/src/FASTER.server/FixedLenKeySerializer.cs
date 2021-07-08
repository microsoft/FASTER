// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using FASTER.core;
using FASTER.common;
using System.Diagnostics;

namespace FASTER.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public unsafe sealed class FixedLenKeySerializer<Key> : IKeySerializer<Key>
    {
        readonly SpanByteVarLenStruct settings;

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
        public bool Match(ref Key k, ref Key pattern)
        {
            if (k.Equals(pattern))
                return true;
            return false;
        }
    }
}
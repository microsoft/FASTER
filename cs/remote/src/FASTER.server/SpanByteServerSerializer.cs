// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using FASTER.core;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public unsafe sealed class SpanByteServerSerializer : IServerSerializer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory>
    {
        readonly SpanByteVarLenStruct settings;
        readonly int keyLength;
        readonly int valueLength;
        
        [ThreadStatic]
        static SpanByteAndMemory output;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="maxKeyLength">Max key length</param>
        /// <param name="maxValueLength">Max value length</param>
        public SpanByteServerSerializer(int maxKeyLength = 512, int maxValueLength = 512)
        {
            settings = new SpanByteVarLenStruct();
            keyLength = maxKeyLength;
            valueLength = maxValueLength;
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
        public ref SpanByte ReadValueByRef(ref byte* src)
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
        public bool Write(ref SpanByteAndMemory k, ref byte* dst, int length)
        {
            if (k.Length > length) return false;

            var dest = new SpanByte(length, (IntPtr)dst);
            if (k.IsSpanByte)
                k.SpanByte.CopyTo(ref dest);
            else
                k.Memory.Memory.Span.CopyTo(dest.AsSpan());
            return true;
        }

        /// <inheritdoc />
        public ref SpanByteAndMemory AsRefOutput(byte* src, int length)
        {
            *(int*)src = length - sizeof(int);
            output = SpanByteAndMemory.FromFixedSpan(new Span<byte>(src, length));
            return ref output;
        }

        /// <inheritdoc />
        public void SkipOutput(ref byte* src) => src += (*(int*)src) + sizeof(int);

        /// <inheritdoc />
        public int GetLength(ref SpanByteAndMemory o) => o.Length;
    }
}
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.common;
using FASTER.core;

namespace FASTER.common
{
    public unsafe sealed class SpanByteParameterSerializer 
        : IServerSerializer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory>, IClientSerializer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory>
    {
        readonly SpanByteVarLenStruct settings;
        readonly int keyLength;
        readonly int valueLength;

        public SpanByteParameterSerializer(int maxKeyLength = 512, int maxValueLength = 512)
        {
            settings = new SpanByteVarLenStruct();
            keyLength = maxKeyLength;
            valueLength = maxValueLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadKeyByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += settings.GetLength(ref ret);
            return ref ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadValueByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += settings.GetLength(ref ret);
            return ref ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadInputByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += settings.GetLength(ref ret);
            return ref ret;
        }

        public bool Write(ref SpanByte k, ref byte* dst, int length)
        {
            var len = settings.GetLength(ref k);
            if (length < len) return false;
            Buffer.MemoryCopy(Unsafe.AsPointer(ref k), dst, len, len);
            dst += len;
            return true;
        }

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

        SpanByteAndMemory output;

        public ref SpanByteAndMemory AsRefOutput(byte* src, int length)
        {
            *(int*)src = length - sizeof(int);
            output = SpanByteAndMemory.FromFixedSpan(new Span<byte>(src, length));
            return ref output;
        }

        public void SkipOutput(ref byte* src)
        {
            src += (*(int*)src) + sizeof(int);
        }

        public SpanByteAndMemory ReadOutput(ref byte* src)
        {
            int length = *(int*)src;
            var _output = SpanByteAndMemory.FromFixedSpan(new Span<byte>(src, length + sizeof(int)));
            src += length + sizeof(int);
            return _output;
        }
    }
}
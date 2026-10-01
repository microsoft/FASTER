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
        // Bit 31 of the serialized length word marks an *unserialized* SpanByte, whose payload field is a
        // raw pointer rather than inline data. Bit 30 marks the presence of an 8-byte metadata header.
        const int kUnserializedBitMask = unchecked((int)0x80000000);
        const int kExtraMetadataBitMask = 0x40000000;
        const int kHeaderMask = unchecked((int)0xC0000000);

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
            keyLength = maxKeyLength;
            valueLength = maxValueLength;
        }

        /// <summary>
        /// Read a serialized SpanByte at <paramref name="src"/>, verifying that it is well-formed and lies
        /// entirely within [src, srcEnd), then advance <paramref name="src"/> past it.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ref SpanByte ReadByRef(ref byte* src, byte* srcEnd)
        {
            if (srcEnd - src < sizeof(int))
                throw new FormatException("Truncated SpanByte header in payload");

            var header = *(int*)src;

            // An unserialized SpanByte interprets the following bytes as a pointer, so it must never be
            // accepted from a payload. Serialized SpanBytes always have this bit cleared on the wire.
            if ((header & kUnserializedBitMask) != 0)
                throw new FormatException("Unserialized SpanByte is not valid in payload");

            int length = header & ~kHeaderMask;
            int metadataSize = (header & kExtraMetadataBitMask) >> (30 - 3);

            // The payload must hold the metadata header (if any), and the whole element must be in bounds.
            if (length < metadataSize || length > srcEnd - src - sizeof(int))
                throw new FormatException("SpanByte length exceeds payload");

            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += sizeof(int) + length;
            return ref ret;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadKeyByRef(ref byte* src, byte* srcEnd) => ref ReadByRef(ref src, srcEnd);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadValueByRef(ref byte* src, byte* srcEnd) => ref ReadByRef(ref src, srcEnd);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadInputByRef(ref byte* src, byte* srcEnd) => ref ReadByRef(ref src, srcEnd);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadKeyByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += ret.TotalSize;
            return ref ret;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadValueByRef(ref byte* src)
        {            
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += ret.TotalSize;
            return ref ret;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadInputByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += ret.TotalSize;
            return ref ret;
        }

        /// <inheritdoc />
        public bool Write(ref SpanByte k, ref byte* dst, int length)
        {
            // The element is written as a four-byte length followed by the payload
            if (k.Length < 0 || length < sizeof(int) || k.Length > length - sizeof(int)) return false;

            *(int*)dst = k.Length;
            dst += sizeof(int);
            var dest = new SpanByte(k.Length, (IntPtr)dst);
            k.CopyTo(ref dest);
            dst += k.Length;
            return true;
        }


        /// <inheritdoc />
        public bool Write(ref SpanByteAndMemory k, ref byte* dst, int length)
        {
            if (k.Length < 0 || k.Length > length) return false;

            var dest = new Span<byte>(dst, k.Length);
            if (k.IsSpanByte)
                k.SpanByte.AsSpan().Slice(0, k.Length).CopyTo(dest);
            else
                // The rented buffer is at least k.Length bytes, and may be larger; copy only the payload
                k.Memory.Memory.Span.Slice(0, k.Length).CopyTo(dest);
            dst += k.Length;
            return true;
        }

        /// <inheritdoc />
        public ref SpanByteAndMemory AsRefOutput(byte* src, int length)
        {
            output = SpanByteAndMemory.FromFixedSpan(new Span<byte>(src, length));
            return ref output;
        }

        /// <inheritdoc />
        public bool SkipOutput(ref byte* src, int length)
        {
            // If the output did not fit in the response buffer it was redirected to the heap, leaving
            // stale bytes at src; copy it in instead of advancing over whatever happens to be there.
            if (!output.IsSpanByte)
                return Write(ref output, ref src, length);

            if (length < sizeof(int)) return false;
            int payloadLength = *(int*)src;
            if (payloadLength < 0 || payloadLength > length - sizeof(int)) return false;
            src += sizeof(int) + payloadLength;
            return true;
        }

        /// <inheritdoc />
        public int GetLength(ref SpanByteAndMemory o) => o.Length;
    }
}
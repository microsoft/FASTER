// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// Represents a pinned variable length byte array that is viewable as a fixed (pinned) Span&lt;byte&gt;
    /// Format: [4-byte (int) length of payload][payload bytes...]
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct SpanByte
    {
        /// <summary>
        /// Length of the payload
        /// </summary>
        [FieldOffset(0)]
        public int length;

        /// <summary>
        /// Start of payload
        /// </summary>
        [FieldOffset(4)]
        public byte payload;

        /// <summary>
        /// Get Span&lt;byte&gt; equivalent
        /// </summary>
        /// <returns></returns>
        public Span<byte> AsSpan()
        {
            return new Span<byte>(Unsafe.AsPointer(ref length), length + sizeof(int));
        }

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; equivalent
        /// </summary>
        /// <returns></returns>
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new Span<byte>(Unsafe.AsPointer(ref length), length + sizeof(int));
        }

        /// <summary>
        /// View a fixed Span&lt;byte&gt; as a SpanByte
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static ref SpanByte FromFixedSpan(Span<byte> span)
        {
            var ptr = Unsafe.AsPointer(ref span[0]);
            *(int*)ptr = span.Length - sizeof(int);
            return ref Unsafe.AsRef<SpanByte>(ptr);
        }

        /// <summary>
        /// View a pinned Memory&lt;byte&gt; as a SpanByte
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static ref SpanByte FromPinnedMemory(Memory<byte> memory)
        {
            return ref FromFixedSpan(memory.Span);
        }


        /// <summary>
        /// Convert [length | payload] to byte array
        /// </summary>
        /// <param name="dst"></param>
        public byte[] ToByteArray()
        {
            var fullLength = length + sizeof(int);
            var dst = new byte[fullLength];
            byte* src = (byte*)Unsafe.AsPointer(ref this);
            for (int i = 0; i < fullLength; i++)
            {
                dst[i] = *src;
                src++;
            }
            return dst;
        }

        /// <summary>
        /// Copy to another pre-allocated SpanByte
        /// </summary>
        /// <param name="dst"></param>
        public void CopyTo(ref SpanByte dst)
        {
            var fullLength = length + sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), fullLength, fullLength);
        }
    }

    /// <summary>
    /// Equality comparer for SpanByte
    /// </summary>
    public struct SpanByteComparer : IFasterEqualityComparer<SpanByte>
    {
        /// <inheritdoc />
        public unsafe long GetHashCode64(ref SpanByte spanByte)
        {
            byte* ptr = (byte*)Unsafe.AsPointer(ref spanByte);
            return Utility.HashBytes(ptr, (*(int*)ptr) + sizeof(int));
        }

        /// <inheritdoc />
        public unsafe bool Equals(ref SpanByte k1, ref SpanByte k2)
        {
            if (k1.length != k2.length) return false;

            byte* src = sizeof(int) + (byte*)Unsafe.AsPointer(ref k1);
            byte* dst = sizeof(int) + (byte*)Unsafe.AsPointer(ref k2);

            for (int i = 0; i < k1.length; i++)
                if (*(src + i) != *(dst + i))
                    return false;
            return true;
        }
    }

    /// <summary>
    /// IVariableLengthStruct for SpanByte
    /// </summary>
    public struct SpanByteLength : IVariableLengthStruct<SpanByte>
    {
        /// <inheritdoc />
        public int GetInitialLength()
        {
            return sizeof(int);
        }

        /// <inheritdoc />
        public int GetLength(ref SpanByte t)
        {
            return sizeof(int) + t.length;
        }
    }

    /// <summary>
    /// Callback functions for SpanByte
    /// </summary>
    public class SpanByteFunctions<Context> : FunctionsBase<SpanByte, SpanByte, byte[], byte[], Context>
    {
        /// <inheritdoc />
        public override void SingleReader(ref SpanByte key, ref byte[] input, ref SpanByte value, ref byte[] dst)
        {
            dst = value.ToByteArray();
        }

        /// <inheritdoc />
        public override void ConcurrentReader(ref SpanByte key, ref byte[] input, ref SpanByte value, ref byte[] dst)
        {
            dst = value.ToByteArray();
        }

        /// <inheritdoc />
        public override void SingleWriter(ref SpanByte key, ref SpanByte src, ref SpanByte dst)
        {
            src.CopyTo(ref dst);
        }

        /// <inheritdoc />
        public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte src, ref SpanByte dst)
        {
            if (dst.length < src.length) return false;
            src.CopyTo(ref dst);
            return true;
        }
    }
}

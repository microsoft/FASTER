// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
        /// Total size in bytes, including header
        /// </summary>
        public int TotalSize => length + sizeof(int);

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
        /// View a pinned memory pointer of given total length as SpanByte
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="totalLength"></param>
        /// <returns></returns>
        public static ref SpanByte FromPointer(byte* ptr, int totalLength)
        {
            *(int*)ptr = totalLength - sizeof(int);
            return ref Unsafe.AsRef<SpanByte>(ptr);
        }

        /// <summary>
        /// Copy source bytes to the destination span, with a 4 byte length header in destination.
        /// Destination should be at least source length + 4 bytes.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        /// <returns>True if copy succeeds</returns>
        public static unsafe bool Copy(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            if (destination.Length >= source.Length + sizeof(int))
            {
                fixed (byte* dst = destination)
                {
                    *(int*)dst = source.Length;
                    source.CopyTo(destination.Slice(sizeof(int)));
                }
            }
            return false;
        }

        /// <summary>
        /// Copy span contents as payload of this SpanByte, update this length to span length
        /// </summary>
        /// <param name="src"></param>
        public void CopyFrom(ReadOnlySpan<byte> src)
        {
            length = src.Length;
            src.CopyTo(AsSpan().Slice(sizeof(int)));
        }

        /// <summary>
        /// Convert [length | payload] to new byte array
        /// </summary>
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
        /// Convert [length | payload] to specified (disposable) memory owner
        /// </summary>
        public IMemoryOwner<byte> ToMemoryOwner(MemoryPool<byte> pool)
        {
            var fullLength = length + sizeof(int);
            var dst = pool.Rent(fullLength);
            AsReadOnlySpan().CopyTo(dst.Memory.Span);
            return dst;
        }

        /// <summary>
        /// Convert to SpanByteAndMemory wrapper
        /// </summary>
        /// <returns></returns>
        public SpanByteAndMemory ToSpanByteAndMemory()
        {
            return new SpanByteAndMemory(ref this);
        }

        /// <summary>
        /// Try to copy to given pre-allocated SpanByte, checking if space permits at destination SpanByte
        /// </summary>
        /// <param name="dst"></param>
        public bool TryCopyTo(ref SpanByte dst)
        {
            var fullLength = length + sizeof(int);
            if (dst.length < length) return false;
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), fullLength, fullLength);
            return true;
        }

        /// <summary>
        /// Blindly copy to given pre-allocated SpanByte, assuming sufficient space
        /// </summary>
        /// <param name="dst"></param>
        public void CopyTo(ref SpanByte dst)
        {
            var fullLength = length + sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), fullLength, fullLength);
        }

        /// <summary>
        /// Copy to given SpanByteAndMemory
        /// </summary>
        /// <param name="dst"></param>
        /// <param name="memoryPool"></param>
        public void CopyTo(ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (TryCopyTo(ref dst.SpanByte))
                    return;
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(TotalSize);
            fixed (byte* bp = dst.Memory.Memory.Span)
                CopyTo(bp);
        }


        /// <summary>
        /// Copy to given memory location via pointer
        /// </summary>
        /// <param name="dst"></param>
        public void CopyTo(byte* dst)
        {
            var fullLength = length + sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), dst, fullLength, fullLength);
        }

    }
}

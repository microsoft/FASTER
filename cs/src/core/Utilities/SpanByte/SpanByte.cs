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
        const int kTypeBitMask = 1 << 31;

        /// <summary>
        /// Length of the payload
        /// </summary>
        [FieldOffset(0)]
        private int length;

        /// <summary>
        /// Start of payload
        /// </summary>
        [FieldOffset(4)]
        private IntPtr payload;

        internal IntPtr Pointer => payload;

        /// <summary>
        /// Length of payload
        /// </summary>
        public int Length
        {
            get { return length & ~kTypeBitMask; }
            set { length = (length & kTypeBitMask) | value; }
        }

        /// <summary>
        /// Format of structure
        /// </summary>
        public bool Serialized => (length & kTypeBitMask) == 0;

        /// <summary>
        /// Total serialized size in bytes, including header
        /// </summary>
        public int TotalSize => Length + sizeof(int);


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="length"></param>
        /// <param name="payload"></param>
        public SpanByte(int length, IntPtr payload)
        {
            this.length = length | kTypeBitMask;
            this.payload = payload;
        }

        /// <summary>
        /// Check or set struct as invalid
        /// </summary>
        public bool Invalid
        {
            get { return ((length & kTypeBitMask) != 0) && payload == IntPtr.Zero; }
            set { 
                if (value) 
                { 
                    length |= kTypeBitMask;
                    payload = IntPtr.Zero;
                }
                else
                {
                    if (Invalid) length = 0;
                }
            }
        }

        /// <summary>
        /// Get Span&lt;byte&gt; for this SpanByte's payload
        /// </summary>
        /// <returns></returns>
        public Span<byte> AsSpan()
        {
            if (Serialized)
                return new Span<byte>(Unsafe.AsPointer(ref payload), length);
            else
                return new Span<byte>((void*)payload, Length);
        }

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; for this SpanByte's payload
        /// </summary>
        /// <returns></returns>
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            if (Serialized)
                return new ReadOnlySpan<byte>(Unsafe.AsPointer(ref payload), length);
            else
                return new ReadOnlySpan<byte>((void*)payload, Length);
        }

        /// <summary>
        /// Reinterpret a fixed Span&lt;byte&gt; as a serialized SpanByte. Automatically adds Span length to the first 4 bytes.
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static ref SpanByte Reinterpret(Span<byte> span)
        {
            fixed (byte* ptr = span)
            {
                *(int*)ptr = span.Length - sizeof(int);
                return ref Unsafe.AsRef<SpanByte>(ptr);
            }
        }

        /// <summary>
        /// Reinterpret a fixed ReadOnlySpan&lt;byte&gt; as a serialized SpanByte, without adding length header
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static ref SpanByte ReinterpretWithoutLength(ReadOnlySpan<byte> span)
        {
            fixed (byte* ptr = span)
            {
                return ref Unsafe.AsRef<SpanByte>(ptr);
            }
        }

        /// <summary>
        /// Reinterpret a fixed pointer as a serialized SpanByte
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        public static ref SpanByte Reinterpret(byte* ptr)
        {
            return ref Unsafe.AsRef<SpanByte>(ptr);
        }

        /// <summary>
        /// Reinterpret a fixed ref as a serialized SpanByte (user needs to write the payload length to the first 4 bytes)
        /// </summary>
        /// <returns></returns>
        public static ref SpanByte Reinterpret<T>(ref T t)
        {
            return ref Unsafe.As<T, SpanByte>(ref t);
        }

        /// <summary>
        /// Create a SpanByte around a fixed Span&lt;byte&gt;. Warning: ensure the Span is fixed until operation returns.
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static SpanByte FromFixedSpan(Span<byte> span)
        {
            return new SpanByte(span.Length, (IntPtr)Unsafe.AsPointer(ref span[0]));
        }

        /// <summary>
        /// Create a SpanByte around a fixed ReadOnlySpan&lt;byte&gt;. Warning: ensure the Span is fixed until operation returns.
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static SpanByte FromFixedSpan(ReadOnlySpan<byte> span)
        {
            fixed (byte* ptr = span)
            {
                return new SpanByte(span.Length, (IntPtr)ptr);
            }
        }

        /// <summary>
        /// Create SpanByte around a pinned Memory&lt;byte&gt;. Warning: ensure the Memory is pinned until operation returns.
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static SpanByte FromPinnedMemory(Memory<byte> memory)
        {
            return FromFixedSpan(memory.Span);
        }

        /// <summary>
        /// Create a SpanByte around a pinned memory pointer of given length
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public static SpanByte FromPointer(byte* ptr, int length)
        {
            return new SpanByte(length, (IntPtr)ptr);
        }


        /// <summary>
        /// Convert payload to new byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            return AsReadOnlySpan().ToArray();
        }

        /// <summary>
        /// Convert payload to specified (disposable) memory owner
        /// </summary>
        public (IMemoryOwner<byte> memory, int length) ToMemoryOwner(MemoryPool<byte> pool)
        {
            var dst = pool.Rent(Length);
            AsReadOnlySpan().CopyTo(dst.Memory.Span);
            return (dst, Length);
        }

        /// <summary>
        /// Convert to SpanByteAndMemory wrapper
        /// </summary>
        /// <returns></returns>
        public SpanByteAndMemory ToSpanByteAndMemory()
        {
            return new SpanByteAndMemory(this);
        }

        /// <summary>
        /// Try to copy to given pre-allocated SpanByte, checking if space permits at destination SpanByte
        /// </summary>
        /// <param name="dst"></param>
        public bool TryCopyTo(ref SpanByte dst)
        {
            if (dst.Length < Length) return false;
            CopyTo(ref dst);
            return true;
        }

        /// <summary>
        /// Blindly copy to given pre-allocated SpanByte, assuming sufficient space
        /// </summary>
        /// <param name="dst"></param>
        public void CopyTo(ref SpanByte dst)
        {
            dst.Length = Length;
            AsReadOnlySpan().CopyTo(dst.AsSpan());
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
            dst.Length = Length;
            AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span);
        }

        /// <summary>
        /// Copy serialized version to specified memory location
        /// </summary>
        /// <param name="destination"></param>
        public void CopyTo(byte* destination)
        {
            if (Serialized)
            {
                Buffer.MemoryCopy(Unsafe.AsPointer(ref this.length), destination, Length + sizeof(int), Length + sizeof(int));
            }
            else
            {
                *(int*)destination = Length;
                Buffer.MemoryCopy((void*)payload, destination + sizeof(int), Length, Length);
            }

        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Buffers;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Output that encapsulates sync stack output (via SpanByte) and async heap output (via IMemoryOwner)
    /// </summary>
    public unsafe struct SpanByteAndMemory : IHeapConvertible
    {
        private void* stackPointer;

        /// <summary>
        /// Constructor using given SpanByte
        /// </summary>
        /// <param name="spanByte"></param>
        public SpanByteAndMemory(ref SpanByte spanByte)
        {
            stackPointer = Unsafe.AsPointer(ref spanByte);
            Memory = default;
        }

        /// <summary>
        /// Constructor using given IMemoryOwner
        /// </summary>
        /// <param name="memory"></param>
        public SpanByteAndMemory(IMemoryOwner<byte> memory)
        {
            stackPointer = null;
            Memory = memory;
        }

        /// <summary>
        /// Heap output as IMemoryOwner
        /// </summary>
        public IMemoryOwner<byte> Memory { get; set; }

        /// <summary>
        /// Stack output as SpanByte
        /// </summary>
        public ref SpanByte SpanByte => ref Unsafe.AsRef<SpanByte>(stackPointer);

        /// <summary>
        /// Convert to be used on heap (IMemoryOwner)
        /// </summary>
        public void ConvertToHeap() { stackPointer = null; }

        /// <summary>
        /// Is it allocated as SpanByte (on stack)?
        /// </summary>
        public bool IsSpanByte => stackPointer != null;
    }
}

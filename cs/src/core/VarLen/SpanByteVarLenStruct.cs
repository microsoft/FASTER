// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// IVariableLengthStruct for SpanByte
    /// </summary>
    public struct SpanByteVarLenStruct : IVariableLengthStruct<SpanByte>
    {
        /// <inheritdoc />
        public int GetInitialLength() => sizeof(int);

        /// <inheritdoc />
        public int GetLength(ref SpanByte t) => sizeof(int) + t.Length;

        /// <inheritdoc />
        public unsafe void Serialize(ref SpanByte source, void* destination) => source.CopyTo((byte*)destination);

        /// <inheritdoc />
        public unsafe ref SpanByte AsRef(void* source) => ref Unsafe.AsRef<SpanByte>(source);

        /// <inheritdoc />
        public unsafe SpanByte AsValue(Memory<byte> source, void* sourcePtr)
            => Unsafe.AsRef<SpanByte>(sourcePtr).Deserialize();

        /// <inheritdoc/>
        public unsafe int GetSerializedLength(void* source)
            => Unsafe.AsRef<SpanByte>(source).Length + sizeof(int);

        /// <inheritdoc />
        public unsafe void Initialize(void* source, void* dest) { *(int*)source = (int)((byte*)dest - (byte*)source) - sizeof(int); }
    }

    /// <summary>
    /// IVariableLengthStruct for SpanByte value, specific to SpanByte input
    /// </summary>
    public struct SpanByteVarLenStructForSpanByteInput : IVariableLengthStruct<SpanByte, SpanByte>
    {
        /// <inheritdoc />
        public int GetInitialLength(ref SpanByte input) => sizeof(int) + input.Length;

        /// <summary>
        /// Length of resulting object when doing RMW with given value and input. Here we set the length
        /// to the max of input and old value lengths. You can provide a custom implementation for other cases.
        /// </summary>
        public int GetLength(ref SpanByte t, ref SpanByte input)
            => sizeof(int) + (t.Length > input.Length ? t.Length : input.Length);
    }
}

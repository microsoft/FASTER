// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
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
            return sizeof(int) + t.Length;
        }

        /// <inheritdoc />
        public unsafe void Serialize(ref SpanByte source, void* destination)
        {
            source.CopyTo((byte*)destination);
        }

        /// <inheritdoc />
        public unsafe ref SpanByte AsRef(void* source) => ref Unsafe.AsRef<SpanByte>(source);

        /// <inheritdoc />
        public unsafe ref SpanByte AsRef(void* source, void* dest) => ref Unsafe.AsRef<SpanByte>(source);
    }
}

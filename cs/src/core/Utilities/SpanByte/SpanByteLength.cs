// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
            return sizeof(int) + t.length;
        }
    }
}

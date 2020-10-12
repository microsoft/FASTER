// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Callback functions for SpanByte with byte[] output
    /// </summary>
    public class SpanByteFunctions_ByteArrayOutput<Context> : SpanByteFunctions<byte[], Context>
    {
        /// <inheritdoc />
        public override void SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst)
        {
            dst = value.ToByteArray();
        }

        /// <inheritdoc />
        public override void ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref byte[] dst)
        {
            dst = value.ToByteArray();
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
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

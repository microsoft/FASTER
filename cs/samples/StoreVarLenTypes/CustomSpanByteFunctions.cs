// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Callback functions for FASTER operations over SpanByte. We use byte arrays as output for simplicity.
    /// To avoid byte array allocation, use SpanByteFunctions&lt;byte&gt;, which uses SpanByteMemory as output type.
    /// </summary>
    public sealed class CustomSpanByteFunctions : SpanByteFunctions_ByteArrayOutput<byte>
    {
        // Read completion callback
        public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref byte[] output, byte ctx, Status status)
        {
            if (status != Status.OK)
            {
                Console.WriteLine("Error!");
                return;
            }

            for (int i = 0; i < output.Length; i++)
            {
                if (output[i] != ctx)
                {
                    Console.WriteLine("Error!");
                    return;
                }
            }
        }
    }
}
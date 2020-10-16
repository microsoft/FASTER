// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Callback functions for FASTER operations. We use byte arrays as output for simplicity. To avoid byte array 
    /// allocation, use SpanByteFunctions[Empty], which uses SpanByteMemory as output type.
    /// </summary>
    public sealed class Functions : SpanByteFunctions_ByteArrayOutput<Empty>
    {
        // Read completion callback
        public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref byte[] output, Empty ctx, Status status)
        {
            if (status != Status.OK)
            {
                Console.WriteLine("Sample1: Error!");
                return;
            }

            for (int i = 0; i < output.Length; i++)
            {
                if (output[i] != (byte)output.Length)
                {
                    Console.WriteLine("Sample1: Error!");
                    return;
                }
            }
        }
    }
}
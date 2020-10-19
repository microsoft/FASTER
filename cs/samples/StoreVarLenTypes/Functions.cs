// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Buffers;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Callback functions for FASTER operations over Memory
    /// </summary>
    public sealed class MyMemoryFunctions<T> : MemoryFunctions<ReadOnlyMemory<T>, T, T>
        where T : unmanaged
    {
        /// <inheritdoc/>
        public MyMemoryFunctions(MemoryPool<T> memoryPool = default)
            : base(memoryPool) { }

        /// <inheritdoc/>
        public override void ReadCompletionCallback(ref ReadOnlyMemory<T> key, ref Memory<T> input, ref (IMemoryOwner<T>, int) output, T ctx, Status status)
        {
            if (status != Status.OK)
            {
                Console.WriteLine("Error!");
                return;
            }

            for (int i = 0; i < output.Item2; i++)
            {
                if (!output.Item1.Memory.Span[i].Equals(ctx))
                {
                    Console.WriteLine("Error!");
                    output.Item1.Dispose();
                    return;
                }
            }
            output.Item1.Dispose();
        }
    }

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
                Console.WriteLine("Error!");
                return;
            }

            for (int i = 0; i < output.Length; i++)
            {
                if (output[i] != (byte)output.Length)
                {
                    Console.WriteLine("Error!");
                    return;
                }
            }
        }
    }
}
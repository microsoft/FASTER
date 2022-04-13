// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Buffers;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Custom callback functions for FASTER operations with ReadOnlyMemory&lt;T&gt; as Key, Memory&lt;T&gt; as Value and Input,
    /// (IMemoryOwner&lt;T&gt;, int) as Output, and T as Context, for unmanaged (blittable) type T
    /// </summary>
    public sealed class CustomMemoryFunctions<T> : MemoryFunctions<ReadOnlyMemory<T>, T, T>
        where T : unmanaged
    {
        /// <inheritdoc/>
        public CustomMemoryFunctions(MemoryPool<T> memoryPool = default)
            : base(memoryPool) { }

        /// <inheritdoc/>
        public override void ReadCompletionCallback(ref ReadOnlyMemory<T> key, ref Memory<T> input, ref (IMemoryOwner<T>, int) output, T ctx, Status status, RecordMetadata recordMetadata)
        {
            if (!status.Found)
            {
                Console.WriteLine("Error!");
                return;
            }

            for (int i = 0; i < output.Item2; i++)
            {
                if (!output.Item1.Memory.Span[i].Equals(ctx))
                {
                    Console.WriteLine("Error!");
                    break;
                }
            }
            output.Item1.Dispose();
        }
    }
}
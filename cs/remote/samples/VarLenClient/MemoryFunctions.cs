// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using FASTER.client;

namespace VarLenClient
{
    public class MemoryFunctions : MemoryFunctionsBase<int>
    {
        public override void ReadCompletionCallback(ref ReadOnlyMemory<int> key, ref ReadOnlyMemory<int> input, ref (IMemoryOwner<int>, int) output, byte ctx, Status status)
        {
            Memory<int> expected = new Memory<int>(new int[key.Span.Length]);

            try
            {
                if (ctx == 0)
                {
                    expected.Span.Fill(key.Span[0] + 10000);
                    if (!status.Found || !expected.Span.SequenceEqual(output.Item1.Memory.Span.Slice(0, output.Item2)))
                        throw new Exception("Incorrect read result");
                }
                else if (ctx == 1)
                {
                    expected.Span.Fill(key.Span[0] + 10000 + 25 + 25);
                    if (!status.Found || !expected.Span.SequenceEqual(output.Item1.Memory.Span.Slice(0, output.Item2)))
                        throw new Exception("Incorrect read result");
                }
                else
                {
                    throw new Exception("Unexpected user context");
                }
            }
            finally
            {
                output.Item1.Dispose();
            }
        }
    }

    public class MemoryFunctionsByte : MemoryFunctionsBase<byte>
    {
        public override void ReadCompletionCallback(ref ReadOnlyMemory<byte> key, ref ReadOnlyMemory<byte> input, ref (IMemoryOwner<byte>, int) output, byte ctx, Status status)
        {
            try
            {
                if (ctx == 0)
                {
                    if (!status.Found || !key.Span.SequenceEqual(output.Item1.Memory.Span.Slice(0, output.Item2)))
                        throw new Exception("Incorrect read result");
                }
                else
                {
                    throw new Exception("Unexpected user context");
                }
            }
            finally
            {
                output.Item1.Dispose();
            }
        }
    }

}

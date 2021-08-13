using System;
using System.Buffers;
using FASTER.client;
using FASTER.common;
using NUnit.Framework;

namespace FASTER.remote.test
{
    class VarLenMemoryClient : IDisposable
    {
        readonly FasterKVClient<ReadOnlyMemory<int>, ReadOnlyMemory<int>> client;

        public VarLenMemoryClient(string address = "127.0.0.1", int port = 33278)
        {
            client = new FasterKVClient<ReadOnlyMemory<int>, ReadOnlyMemory<int>>(address, port);
        }

        public void Dispose()
        {
            client.Dispose();
        }

        public ClientSession<ReadOnlyMemory<int>, ReadOnlyMemory<int>, ReadOnlyMemory<int>, (IMemoryOwner<int>, int), long, MemoryFunctions, MemoryParameterSerializer<int>> GetSession()
            => client.NewSession<ReadOnlyMemory<int>, (IMemoryOwner<int>, int), long, MemoryFunctions, MemoryParameterSerializer<int>>(new MemoryFunctions(), WireFormat.DefaultVarLenKV, new MemoryParameterSerializer<int>());
    }

    /// <summary>
    /// Callback functions
    /// </summary>
    public class MemoryFunctions : ICallbackFunctions<ReadOnlyMemory<int>, ReadOnlyMemory<int>, ReadOnlyMemory<int>, (IMemoryOwner<int>, int), long>
    {
        /// <inheritdoc />
        public virtual void DeleteCompletionCallback(ref ReadOnlyMemory<int> key, long ctx) { }

        /// <inheritdoc />
        public virtual void ReadCompletionCallback(ref ReadOnlyMemory<int> key, ref ReadOnlyMemory<int> input, ref (IMemoryOwner<int>, int) output, long ctx, Status status)
        {
            try
            {
                Assert.IsTrue(status == Status.OK);
                int check = key.Span[0];
                int len = key.Span[1];
                Assert.IsTrue(check == ctx);
                Assert.IsTrue(output.Item2 == len);
                Memory<int> expected = new Memory<int>(new int[len]);
                expected.Span.Fill(check);
                Assert.IsTrue(expected.Span.SequenceEqual(output.Item1.Memory.Span.Slice(0, output.Item2)));
            }
            finally
            {
                output.Item1.Dispose();
            }
        }

        /// <inheritdoc />
        public virtual void RMWCompletionCallback(ref ReadOnlyMemory<int> key, ref ReadOnlyMemory<int> input, ref (IMemoryOwner<int>, int) output, long ctx, Status status) { }

        /// <inheritdoc />
        public virtual void UpsertCompletionCallback(ref ReadOnlyMemory<int> key, ref ReadOnlyMemory<int> value, long ctx) { }

        /// <inheritdoc />
        public virtual void SubscribeKVCallback(ref ReadOnlyMemory<int> key, ref ReadOnlyMemory<int> input, ref (IMemoryOwner<int>, int) output, long ctx, Status status) 
        {
            try
            {
                Assert.IsTrue(status == Status.OK);
                int check = key.Span[0];
                int len = key.Span[1];
                Assert.IsTrue(output.Item2 == len);
                Memory<int> expected = new Memory<int>(new int[len]);
                expected.Span.Fill(check);
                Assert.IsTrue(expected.Span.SequenceEqual(output.Item1.Memory.Span.Slice(0, output.Item2)));
            }
            finally
            {
                output.Item1.Dispose();
            }
        }
    }
}

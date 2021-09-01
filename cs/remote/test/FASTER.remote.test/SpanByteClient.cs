using System;
using System.Threading;
using FASTER.client;
using NUnit.Framework;

namespace FASTER.remote.test
{
    class SpanByteClient : IDisposable
    {
        readonly FasterKVClient<SpanByte, SpanByte> client;

        public SpanByteClient(string address = "127.0.0.1", int port = 33278)
        {
            client = new FasterKVClient<SpanByte, SpanByte>(address, port);
        }

        public void Dispose()
        {
            client.Dispose();
        }

        public ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, SpanByteClientFunctions, SpanByteClientSerializer> GetSession()
            => client.NewSession(new SpanByteClientFunctions());

        public ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, SpanByteClientFunctions, SpanByteClientSerializer> GetSession(SpanByteClientFunctions f)
            => client.NewSession(f);
    }

    /// <summary>
    /// Callback functions
    /// </summary>
    public class SpanByteClientFunctions : SpanByteFunctionsBase
    {
        readonly AutoResetEvent evt = new AutoResetEvent(false);

        /// <inheritdoc />
        public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status)
        {
            try
            {
                Assert.IsTrue(status == Status.OK);
                var expected = output.IsSpanByte ? output.SpanByte.AsSpan() : output.Memory.Memory.Span;
                Assert.IsTrue(key.AsSpan().SequenceEqual(expected.Slice(0, output.Length)));
            }
            finally
            {
                output.Memory?.Dispose();
            }
        }

        /// <inheritdoc />
        public override void SubscribeKVCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status)
        {
            try
            {
                
                Assert.IsTrue(status == Status.OK);
                var expected = output.IsSpanByte ? output.SpanByte.AsSpan() : output.Memory.Memory.Span;
                Assert.IsTrue(key.AsSpan().SequenceEqual(expected.Slice(0, output.Length)));
                evt.Set();
            }
            finally
            {
                output.Memory?.Dispose();
            }
        }

        /// <inheritdoc />
        public override void SubscribeCallback(ref SpanByte key, ref SpanByte value, byte ctx)
        {
            Assert.IsTrue(key.AsSpan().SequenceEqual(value.AsSpan()));
            evt.Set();
        }

        public void WaitSubscribe() => evt.WaitOne();
    }
}

using System;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{

    /// <summary>
    /// Session provider for FasterKV store based on
    /// [K, V, I, O, C] = [SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long]
    /// </summary>
    public sealed class SpanByteFasterKVProvider : ISessionProvider, IDisposable
    {
        readonly FasterKV<SpanByte, SpanByte> store;
        readonly SpanByteServerSerializer serializer;
        readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Create SpanByte FasterKV backend
        /// </summary>
        /// <param name="store"></param>
        /// <param name="maxSizeSettings"></param>
        public SpanByteFasterKVProvider(FasterKV<SpanByte, SpanByte> store, MaxSizeSettings maxSizeSettings = default)
        {
            this.store = store;
            this.serializer = new SpanByteServerSerializer();
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
        }

        /// <inheritdoc />
        public IServerSession GetSession(WireFormat wireFormat, Socket socket)
        {
            return new BinaryServerSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteServerSerializer>
                (socket, store, new SpanByteFunctionsForServer<long>(wireFormat), serializer, maxSizeSettings);
        }

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }
}
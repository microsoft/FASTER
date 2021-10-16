using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Session provider for FasterKV store based on
    /// [K, V, I, O, C] = [SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long]
    /// </summary>
    public class SpanByteFasterKVProvider : ISessionProvider
    {
        /// <summary>
        /// Store
        /// </summary>
        protected readonly FasterKV<SpanByte, SpanByte> store;

        /// <summary>
        /// Serializer
        /// </summary>
        protected readonly SpanByteServerSerializer serializer;

        /// <summary>
        /// KV broker
        /// </summary>
        protected readonly SubscribeKVBroker<SpanByte, SpanByte, SpanByte, IKeyInputSerializer<SpanByte, SpanByte>> kvBroker;

        /// <summary>
        /// Broker
        /// </summary>
        protected readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker;

        /// <summary>
        /// Size settings
        /// </summary>
        protected readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Create SpanByte FasterKV backend
        /// </summary>
        /// <param name="store"></param>
        /// <param name="kvBroker"></param>
        /// <param name="broker"></param>
        /// <param name="recoverStore"></param>
        /// <param name="maxSizeSettings"></param>
        public SpanByteFasterKVProvider(FasterKV<SpanByte, SpanByte> store, SubscribeKVBroker<SpanByte, SpanByte, SpanByte, IKeyInputSerializer<SpanByte, SpanByte>> kvBroker = null, SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker = null, bool recoverStore = false, MaxSizeSettings maxSizeSettings = default)
        {
            this.store = store;
            if (recoverStore)
            {
                try
                {
                    store.Recover();
                }
                catch
                { }
            }
            this.kvBroker = kvBroker;
            this.broker = broker;
            this.serializer = new SpanByteServerSerializer();
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
        }

        /// <inheritdoc />
        public virtual IServerSession GetSession(WireFormat wireFormat, Socket socket)
        {
            switch (wireFormat)
            {
                case WireFormat.WebSocket:
                    return new BinaryServerSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteServerSerializer>
                        (socket, store, new SpanByteFunctionsForServer<long>(), serializer, maxSizeSettings, kvBroker, broker, true);
                default:
                    return new BinaryServerSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteServerSerializer>
                        (socket, store, new SpanByteFunctionsForServer<long>(), serializer, maxSizeSettings, kvBroker, broker);
            }
        }
    }
}
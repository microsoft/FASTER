using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Session provider for FasterKV store based on
    /// [K, V, I, O, C] = [SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long]
    /// </summary>
    public sealed class SpanByteFasterKVProvider : FasterKVProviderBase<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteServerSerializer>
    {
        /// <summary>
        /// Create default SpanByte FasterKV backend with SpanByteFunctionsForServer&lt;long&gt; as functions, and SpanByteServerSerializer as parameter serializer
        /// </summary>
        /// <param name="store"></param>
        /// <param name="kvBroker"></param>
        /// <param name="broker"></param>
        /// <param name="recoverStore"></param>
        /// <param name="maxSizeSettings"></param>
        public SpanByteFasterKVProvider(FasterKV<SpanByte, SpanByte> store, SubscribeKVBroker<SpanByte, SpanByte, SpanByte, IKeyInputSerializer<SpanByte, SpanByte>> kvBroker = null, SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker = null, bool recoverStore = false, MaxSizeSettings maxSizeSettings = default)
            : base(store, new(), kvBroker, broker, recoverStore, maxSizeSettings)
        {
        }

        /// <inheritdoc />
        public override SpanByteFunctionsForServer<long> GetFunctions() => new();
    }
}
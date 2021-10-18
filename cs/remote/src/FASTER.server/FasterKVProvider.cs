using System;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Session provider for FasterKV store
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    /// <typeparam name="ParameterSerializer"></typeparam>
    public sealed class FasterKVProvider<Key, Value, Input, Output, Functions, ParameterSerializer> : ISessionProvider
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly FasterKV<Key, Value> store;
        readonly Func<WireFormat, Functions> functionsGen;
        readonly ParameterSerializer serializer;
        readonly MaxSizeSettings maxSizeSettings;
        readonly SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> subscribeKVBroker;
        readonly SubscribeBroker<Key, Value, IKeySerializer<Key>> subscribeBroker;

        /// <summary>
        /// Create FasterKV backend
        /// </summary>
        /// <param name="store"></param>
        /// <param name="functionsGen"></param>
        /// <param name="subscribeKVBroker"></param>
        /// <param name="subscribeBroker"></param>
        /// <param name="serializer"></param>
        /// <param name="maxSizeSettings"></param>
        public FasterKVProvider(FasterKV<Key, Value> store, Func<WireFormat, Functions> functionsGen, SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> subscribeKVBroker = null, SubscribeBroker<Key, Value, IKeySerializer<Key>> subscribeBroker = null, ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default)
        {
            this.store = store;
            this.functionsGen = functionsGen;
            this.serializer = serializer;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
            this.subscribeKVBroker = subscribeKVBroker;
            this.subscribeBroker = subscribeBroker;
        }

        /// <inheritdoc />
        public IServerSession GetSession(WireFormat wireFormat, Socket socket, NetworkProtocol protocol)
        {
            switch (wireFormat)
            {
                default:
                    switch (protocol)
                    {
                        case NetworkProtocol.WebSocket:
                            return new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
                                (socket, store, functionsGen(wireFormat), serializer, maxSizeSettings, subscribeKVBroker, subscribeBroker, true);
                        default:
                            return new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
                                (socket, store, functionsGen(wireFormat), serializer, maxSizeSettings, subscribeKVBroker, subscribeBroker);
                    }
            }
        }
    }
}
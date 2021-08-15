using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
            where Functions : IFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly FasterKV<Key, Value> store;
        readonly Func<WireFormat, Functions> functionsGen;
        readonly ParameterSerializer serializer;
        readonly MaxSizeSettings maxSizeSettings;
        readonly SubscribeKVBroker<Key, Value, IKeySerializer<Key>> subscribeKVBroker;

        /// <summary>
        /// Create FasterKV backend
        /// </summary>
        /// <param name="store"></param>
        /// <param name="functionsGen"></param>
        /// <param name="subscribeKVBroker"></param>
        /// <param name="serializer"></param>
        /// <param name="maxSizeSettings"></param>
        public FasterKVProvider(FasterKV<Key, Value> store, Func<WireFormat, Functions> functionsGen, SubscribeKVBroker<Key, Value, IKeySerializer<Key>> subscribeKVBroker = default, ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default)
        {
            this.store = store;
            this.functionsGen = functionsGen;
            this.serializer = serializer;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
            this.subscribeKVBroker = subscribeKVBroker;
        }

        /// <inheritdoc />
        public IServerSession GetSession(WireFormat wireFormat, Socket socket)
        {
            switch (wireFormat)
            {
                case WireFormat.WebSocket:
                    return new WebsocketServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
                        (socket, store, functionsGen(wireFormat), serializer, maxSizeSettings, subscribeKVBroker);
                default:
                    return new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
                        (socket, store, functionsGen(wireFormat), serializer, maxSizeSettings, subscribeKVBroker);
            }
        }
    }
}
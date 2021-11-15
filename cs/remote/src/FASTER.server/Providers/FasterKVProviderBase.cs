// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Abstract session provider for FasterKV store based on
    /// [K, V, I, O, F, P]
    /// </summary>
    public abstract class FasterKVProviderBase<Key, Value, Input, Output, Functions, ParameterSerializer> : ISessionProvider
        where Functions : IAdvancedFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        /// <summary>
        /// Store
        /// </summary>
        protected readonly FasterKV<Key, Value> store;

        /// <summary>
        /// Serializer
        /// </summary>
        protected readonly ParameterSerializer serializer;

        /// <summary>
        /// KV broker
        /// </summary>
        protected readonly SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> kvBroker;

        /// <summary>
        /// Broker
        /// </summary>
        protected readonly SubscribeBroker<Key, Value, IKeySerializer<Key>> broker;

        /// <summary>
        /// Size settings
        /// </summary>
        protected readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Create FasterKV backend
        /// </summary>
        /// <param name="store"></param>
        /// <param name="serializer"></param>
        /// <param name="kvBroker"></param>
        /// <param name="broker"></param>
        /// <param name="recoverStore"></param>
        /// <param name="maxSizeSettings"></param>
        public FasterKVProviderBase(FasterKV<Key, Value> store, ParameterSerializer serializer, SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> kvBroker = null, SubscribeBroker<Key, Value, IKeySerializer<Key>> broker = null, bool recoverStore = false, MaxSizeSettings maxSizeSettings = default)
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
            this.serializer = serializer;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
        }

        /// <summary>
        /// GetFunctions() for custom functions provided by the client
        /// </summary>
        /// <returns></returns>
        public abstract Functions GetFunctions();

        /// <inheritdoc />
        public virtual IServerSession GetSession(WireFormat wireFormat, Socket socket)
        {
            switch (wireFormat)
            {
                case WireFormat.WebSocket:
                    return new WebsocketServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
                        (socket, store, GetFunctions(), serializer, maxSizeSettings, kvBroker, broker);
                default:
                    return new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
                        (socket, store, GetFunctions(), serializer, maxSizeSettings, kvBroker, broker);
            }
        }
    }
}
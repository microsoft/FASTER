// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
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
    public sealed class FasterKVProvider<Key, Value, Input, Output, Functions, ParameterSerializer> 
        : FasterKVProviderBase<Key, Value, Input, Output, Functions, ParameterSerializer>
            where Functions : IFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly Func<Functions> functionsGen;

        /// <summary>
        /// Create FasterKV backend
        /// </summary>
        /// <param name="functionsGen"></param>
        /// <param name="store"></param>
        /// <param name="serializer"></param>
        /// <param name="kvBroker"></param>
        /// <param name="broker"></param>
        /// <param name="recoverStore"></param>
        /// <param name="maxSizeSettings"></param>
        public FasterKVProvider(Func<Functions> functionsGen, FasterKV<Key, Value> store, ParameterSerializer serializer = default, SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> kvBroker = null, SubscribeBroker<Key, Value, IKeySerializer<Key>> broker = null, bool recoverStore = false, MaxSizeSettings maxSizeSettings = default)
            : base(store, serializer, kvBroker, broker, recoverStore, maxSizeSettings)
        {
            this.functionsGen = functionsGen;
        }

        /// <inheritdoc />
        public override Functions GetFunctions() => functionsGen();
    }
}
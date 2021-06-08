// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal unsafe sealed class SubscribeKVBroker<Key, Value, Input, Output, Functions, ParameterSerializer>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly ParameterSerializer serializer;
        int sid = 0;
        ConcurrentDictionary<byte[], (int, BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>)> subscriptions;

        public SubscribeKVBroker(ParameterSerializer serializer)
        {
            this.serializer = serializer;
        }

        public async Task Start()
        {
            // Read from queue and send to all subscribed sessions
        }

        public int Subscribe(ref byte* key, BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            var start = key;
            serializer.ReadKeyByRef(ref key);
            var id = Interlocked.Increment(ref sid);
            if (subscriptions == null)
                Interlocked.CompareExchange(ref subscriptions, new ConcurrentDictionary<byte[], (int, BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>)>(new ByteArrayComparer()), null);
            subscriptions.TryAdd(new Span<byte>(start, (int)(key - start)).ToArray(), (id, session));
            return id;
        }

        public void Publish(byte* key)
        {
            Input input = default;
            if (subscriptions == null) return;

            var start = key;
            ref Key k = ref serializer.ReadKeyByRef(ref key);
            if (subscriptions.TryGetValue(new Span<byte>(start, (int)(key - start)).ToArray(), out var value))
            {
                value.Item2.Publish(ref k, ref input, value.Item1);
            }
            // Add to async queue and return
        }
    }
}

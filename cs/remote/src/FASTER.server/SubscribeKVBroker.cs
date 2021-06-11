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
    internal sealed class SubscribeKVBroker<Key, Value, Input, Output, Functions, ParameterSerializer>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly ParameterSerializer serializer;
        private BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer> subscribeServerSession;
        private int sid = 0;
        private ConcurrentDictionary<byte[], (int, HashSet<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>>)> subscriptions;
        private AsyncQueue<(Key, byte[])> publishQueue;

        public void removeSubscription(ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            foreach (var key in subscriptions.Keys)
                subscriptions[key].Item2.Remove((BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>)session);
        }

        public SubscribeKVBroker(ParameterSerializer serializer)
        {
            this.serializer = serializer;
        }

        public void assignSubscriptionSession(BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer> subscribeServerSession)
        {
            this.subscribeServerSession = subscribeServerSession;
        }

        public async Task Start()
        {
            Input input = default;
            var uniqueKeys = new ConcurrentDictionary<byte[], Key>(new ByteArrayComparer());

            // Read from queue and send to all subscribed sessions
            while (true) {
                while (publishQueue.Count > 0)
                {
                    var subscriptionKey = await publishQueue.DequeueAsync();
                    uniqueKeys.TryAdd(subscriptionKey.Item2, subscriptionKey.Item1);

                    foreach (var byteKey in uniqueKeys.Keys)
                    {
                        Output output = default;
                        uniqueKeys.TryGetValue(byteKey, out var typedKey);
                        subscriptions.TryGetValue(byteKey, out var value);
                        var status = subscribeServerSession.ReadBeforePublish(ref typedKey, ref input, ref output, value.Item1);
                        foreach (var session in value.Item2)
                            session.Publish(ref typedKey, ref input, value.Item1, status, ref output);
                    }
                }
            }
        }

        public unsafe int Subscribe(ref byte* key, BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            var start = key;
            serializer.ReadKeyByRef(ref key);
            var id = Interlocked.Increment(ref sid);
            if (subscriptions == null)
                Interlocked.CompareExchange(ref subscriptions, new ConcurrentDictionary<byte[], (int, HashSet<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>>)>(new ByteArrayComparer()), null);
            if (publishQueue == null)
            {
                Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<(Key, byte[])>(), null);
                Task.Run(() => Start());
            }
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            subscriptions.TryAdd(subscriptionKey, (id, new HashSet<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>>()));
            subscriptions[subscriptionKey].Item2.Add(session);
            return id;
        }

        public unsafe void Publish(byte* key)
        {
            if (subscriptions == null) return;

            var start = key;
            ref Key k = ref serializer.ReadKeyByRef(ref key);
            var subscriptionsKey = new Span<byte>(start, (int)(key - start)).ToArray();
            //subscriptions.TryGetValue(subscriptionsKey, out var value);
            publishQueue.Enqueue((k, subscriptionsKey));
            //Start();
            // Add to async queue and return
        }
    }
}

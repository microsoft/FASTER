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
    /// <summary>
    /// Broker used for PUB-SUB to FASTER KV store. There is a broker per FasterKV instance.
    /// A single broker can be used with multiple FasterKVProviders. 
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="KeyValueSerializer"></typeparam>
    public sealed class SubscribeBroker<Key, Value, KeyValueSerializer> : IDisposable
        where KeyValueSerializer : IKeySerializer<Key>
    {
        private int sid = 0;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> subscriptions;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> prefixSubscriptions;
        private AsyncQueue<(byte[], byte[])> publishQueue;
        readonly IKeySerializer<Key> keySerializer;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="keySerializer">Serializer for Prefix Match and serializing Key</param>
        public SubscribeBroker(IKeySerializer<Key> keySerializer)
        {
            this.keySerializer = keySerializer;
        }

        /// <summary>
        /// Remove all subscriptions for a session,
        /// called during dispose of server session
        /// </summary>
        /// <param name="session">server session</param>
        public void RemoveSubscription(IServerSession session)
        {
            if (subscriptions != null)
            {
                foreach (var subscribedkey in subscriptions.Keys)
                {
                    subscriptions.TryGetValue(subscribedkey, out var subscriptionDict);
                    foreach (var sid in subscriptionDict.Keys)
                    {
                        if (subscriptionDict[sid] == session) {
                            subscriptionDict.TryRemove(sid, out _);
                            break;
                        }
                    }
                }
            }

            if (prefixSubscriptions != null)
            {
                foreach (var subscribedkey in prefixSubscriptions.Keys)
                {
                    prefixSubscriptions.TryGetValue(subscribedkey, out var subscriptionDict);
                    foreach (var sid in subscriptionDict.Keys)
                    {
                        if (subscriptionDict[sid] == session) {
                            subscriptionDict.TryRemove(sid, out _);
                            break;
                        }
                    }
                }
            }
        }

        internal async Task Start()
        {
            var uniqueKeys = new Dictionary<byte[], byte[]>(new ByteArrayComparer());
            var uniqueKeySubscriptions = new List<(ServerSessionBase, int, bool)>();

            while (true)
            {

                var (publishKey, publishValue) = await publishQueue.DequeueAsync();
                uniqueKeys.Add(publishKey, publishValue);

                while (publishQueue.Count > 0)
                {
                    (publishKey, publishValue) = await publishQueue.DequeueAsync();
                    uniqueKeys.Add(publishKey, publishValue);
                }

                unsafe
                {
                    var enumerator = uniqueKeys.GetEnumerator();
                    while (enumerator.MoveNext())
                    {
                        byte[] keyBytes = enumerator.Current.Key;
                        byte[] valBytes = enumerator.Current.Value;
                        fixed (byte* ptr = &keyBytes[0], valPtr = &valBytes[0])
                        {
                            byte* keyPtr = ptr;
                            bool foundSubscription = subscriptions.TryGetValue(keyBytes, out var subscriptionServerSessionDict);
                            if (foundSubscription)
                            {                                
                                foreach (var sid in subscriptionServerSessionDict.Keys)
                                {
                                    byte* keyBytePtr = ptr;
                                    byte* valBytePtr = valPtr;
                                    var serverSession = subscriptionServerSessionDict[sid];
                                    serverSession.Publish(ref keyBytePtr, keyBytes.Length, ref valBytePtr, sid, false);
                                }
                            }

                            foreach (var subscribedPrefixBytes in prefixSubscriptions.Keys)
                            {
                                fixed (byte* subscribedPrefixPtr = &subscribedPrefixBytes[0])
                                {
                                    byte* subPrefixPtr = subscribedPrefixPtr;
                                    byte* reqKeyPtr = ptr;

                                    bool match = keySerializer.Match(ref keySerializer.ReadKeyByRef(ref reqKeyPtr),
                                        ref keySerializer.ReadKeyByRef(ref subPrefixPtr));
                                    if (match)
                                    {
                                        prefixSubscriptions.TryGetValue(subscribedPrefixBytes, out var prefixSubscriptionServerSessionDict);
                                        foreach (var sid in prefixSubscriptionServerSessionDict.Keys)
                                        {
                                            byte* keyBytePtr = ptr;
                                            byte* valBytePtr = valPtr;
                                            var serverSession = prefixSubscriptionServerSessionDict[sid];
                                            serverSession.Publish(ref keyBytePtr, keyBytes.Length, ref valBytePtr, sid, true);
                                        }
                                    }
                                }
                            }
                        }
                        uniqueKeySubscriptions.Clear();
                    }
                    uniqueKeys.Clear();
                }
            }
        }

        /// <summary>
        /// Subscribe to a particular Key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe int Subscribe(ref byte* key, ServerSessionBase session)
        {
            var start = key;
            keySerializer.ReadKeyByRef(ref key);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<(byte[], byte[])>(), null) == null)
            {
                subscriptions= new ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>>(new ByteArrayComparer());
                prefixSubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>>(new ByteArrayComparer());
                Task.Run(() => Start());
            }
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            bool added = subscriptions.TryAdd(subscriptionKey, new ConcurrentDictionary<int, ServerSessionBase>());
            subscriptions[subscriptionKey].TryAdd(sid, session);
            return id;
        }

        /// <summary>
        /// Subscribe to a particular prefix
        /// </summary>
        /// <param name="prefix">prefix to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe int PSubscribe(ref byte* prefix, ServerSessionBase session)
        {
            var start = prefix;
            keySerializer.ReadKeyByRef(ref prefix);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<(byte[], byte[])>(), null) == null)
            {
                subscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>>(new ByteArrayComparer());
                prefixSubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>>(new ByteArrayComparer());
                Task.Run(() => Start());
            }
            var subscriptionPrefix = new Span<byte>(start, (int)(prefix - start)).ToArray();
            prefixSubscriptions.TryAdd(subscriptionPrefix, new ConcurrentDictionary<int, ServerSessionBase>());
            prefixSubscriptions[subscriptionPrefix].TryAdd(sid, session);
            return id;
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
        /// <param name="valueLength">value length that has been updated</param>
        public unsafe void Publish(byte* key, byte* value, int valueLength)
        {
            if (subscriptions == null && prefixSubscriptions == null) return;

            var start = key;
            ref Key k = ref keySerializer.ReadKeyByRef(ref key);
            var keyBytes = new Span<byte>(start, (int)(key - start)).ToArray();
            var valBytes = new Span<byte>(value, (int)(valueLength)).ToArray();

            publishQueue.Enqueue((keyBytes, valBytes));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            subscriptions.Clear();
            prefixSubscriptions.Clear();
        }
    }
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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
    /// <typeparam name="KeySerializer"></typeparam>
    public sealed class SubscribeKVBroker<Key, Value, KeySerializer> : IDisposable
        where KeySerializer : IKeySerializer<Key>
    {
        private int sid = 0;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> subscriptions;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> prefixSubscriptions;
        private AsyncQueue<byte[]> publishQueue;
        readonly IKeySerializer<Key> keySerializer;
        readonly FasterLog log;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="keySerializer">Serializer for Prefix Match and serializing Key</param>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="startFresh">start the log from scratch, do not continue</param>
        public SubscribeKVBroker(IKeySerializer<Key> keySerializer, string logDir, bool startFresh = true)
        {
            this.keySerializer = keySerializer;
            var device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(logDir + "/pubsubkv", preallocateFile: false);
            device.Initialize((long)(1 << 30)*64);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
            if (startFresh)
                log.TruncateUntil(log.CommittedUntilAddress);
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
            var uniqueKeys = new HashSet<byte[]>(new ByteArrayComparer());
            var uniqueKeySubscriptions = new List<(ServerSessionBase, int, bool)>();
            long truncateUntilAddress = log.BeginAddress;

            while (true)
            {
                var iter = log.Scan(log.BeginAddress, long.MaxValue, scanUncommitted: true);
                await iter.WaitAsync();
                while (iter.GetNext(out byte[] subscriptionKey, out int entryLength, out long currentAddress, out long nextAddress))
                {
                    if (currentAddress >= long.MaxValue) return;
                    uniqueKeys.Add(subscriptionKey);
                    truncateUntilAddress = nextAddress;
                }

                if (truncateUntilAddress > log.BeginAddress)
                    log.TruncateUntil(truncateUntilAddress);

                unsafe
                {
                    foreach (var keyBytes in uniqueKeys)
                    {
                        fixed (byte* ptr = &keyBytes[0])
                        {
                            byte* keyPtr = ptr;
                            bool foundSubscription = subscriptions.TryGetValue(keyBytes, out var subscriptionServerSessionDict);
                            if (foundSubscription)
                            {                                
                                foreach (var sid in subscriptionServerSessionDict.Keys)
                                {
                                    byte* keyBytePtr = ptr;
                                    var serverSession = subscriptionServerSessionDict[sid];
                                    byte* nullBytePtr = null;
                                    serverSession.Publish(ref keyBytePtr, keyBytes.Length, ref nullBytePtr, sid, false);
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
                                            var serverSession = prefixSubscriptionServerSessionDict[sid];
                                            byte* nullBytrPtr = null;
                                            serverSession.Publish(ref keyBytePtr, keyBytes.Length, ref nullBytrPtr, sid, true);
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
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
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
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
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
        public unsafe void Publish(byte* key)
        {
            if (subscriptions == null && prefixSubscriptions == null) return;

            var start = key;
            ref Key k = ref keySerializer.ReadKeyByRef(ref key);
            log.Enqueue(new Span<byte>(start, (int)(key - start)));
            log.RefreshUncommitted();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            subscriptions?.Clear();
            prefixSubscriptions?.Clear();
            log.Dispose();
        }
    }
}

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
    public sealed class SubscribeKVBroker
    {
        private int sid = 0;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>> subscriptionProviders;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>> prefixSubscriptionProviders;
        private FasterServer server;
        private AsyncQueue<byte[]> publishQueue;

        public SubscribeKVBroker(FasterServer server)
        {
            this.server = server;
        }

        public void removeSubscription(IServerSession session)
        {
            if (subscriptionProviders == null && prefixSubscriptionProviders == null)
                return;

            if (subscriptionProviders != null)
            {
                foreach (var key in subscriptionProviders.Keys)
                {
                    bool found = false;
                    foreach (var providerSession in subscriptionProviders[key].Keys)
                    {
                        foreach (var subscribedSession in subscriptionProviders[key][providerSession])
                        {
                            if (subscribedSession.Item1 == session)
                            {
                                subscriptionProviders[key][providerSession].Remove(subscribedSession);
                                found = true;                                
                                break;
                            }
                        }
                        if (found)
                            break;
                    }
                }
            }

            if (prefixSubscriptionProviders != null)
            {
                foreach (var key in prefixSubscriptionProviders.Keys)
                {
                    bool found = false;
                    foreach (var providerSession in prefixSubscriptionProviders[key].Keys)
                    {
                        foreach (var subscribedSession in prefixSubscriptionProviders[key][providerSession])
                        {
                            if (subscribedSession.Item1 == session)
                            {
                                found = true;
                                prefixSubscriptionProviders[key][providerSession].Remove(subscribedSession);
                                break;
                            }
                        }
                        if (found)
                            break;
                    }
                }
            }
        }

        public async Task Start()
        {
            var uniqueKeys = new HashSet<byte[]>(new ByteArrayComparer());
            var providerSubs = new Dictionary<ISessionProvider, List<(IServerSession, int, bool)>>();

            while (true) {

                var subscriptionKey = await publishQueue.DequeueAsync();
                uniqueKeys.Add(subscriptionKey);

                while (publishQueue.Count > 0)
                {
                    subscriptionKey = await publishQueue.DequeueAsync();
                    uniqueKeys.Add(subscriptionKey);
                }

                unsafe
                {
                    foreach (var keyBytes in uniqueKeys)
                    {
                        fixed (byte* ptr = &keyBytes[0])
                        {
                            byte* keyPtr = ptr;
                            bool foundSubscription = subscriptionProviders.TryGetValue(keyBytes, out var subSessionDict);
                            if (foundSubscription)
                            {
                                foreach (var provider in subSessionDict.Keys)
                                {
                                    var subs = subSessionDict[provider];
                                    var providerSubList = new List<(IServerSession, int, bool)>();
                                    foreach (var sub in subs)
                                    {
                                        var subTuple = (sub.Item1, sub.Item2, false);
                                        providerSubList.Add(subTuple);
                                    }
                                    providerSubs.Add(provider, providerSubList);
                                }
                            }

                            foreach (var subscribedPrefixBytes in prefixSubscriptionProviders.Keys)
                            {
                                fixed (byte* subscribedPrefixPtr = &subscribedPrefixBytes[0])
                                {
                                    byte* subPrefixPtr = subscribedPrefixPtr;
                                    byte* reqKeyPtr = ptr;

                                    prefixSubscriptionProviders.TryGetValue(subscribedPrefixBytes, out var sessionDict);
                                    foreach (var provider in sessionDict.Keys)
                                    {
                                        bool match = provider.CheckSubKeyMatch(ref subPrefixPtr, ref reqKeyPtr);
                                        if (match) {
                                            var subs = sessionDict[provider];
                                            var providerSubList = new List<(IServerSession, int, bool)>();
                                            foreach (var sub in subs)
                                            {
                                                var subTuple = (sub.Item1, sub.Item2, true);
                                                providerSubList.Add(subTuple);
                                            }
                                            if (providerSubs.ContainsKey(provider))
                                                providerSubs[provider].AddRange(providerSubList);
                                            else
                                                providerSubs.Add(provider, providerSubList);
                                        }
                                    }
                                }
                            }

                            if (providerSubs.Count > 0)
                            {
                                foreach(var provider in providerSubs.Keys)
                                    provider.ReadAndPublish(keyPtr, providerSubs[provider]);
                            }
                        }
                        providerSubs.Clear();
                    }
                    uniqueKeys.Clear();
                }
            }
        }

        public unsafe int Subscribe(ref byte* key, int keyLength, IServerSession session, WireFormat wireFormat)
        {
            var start = key;
            var end = key + keyLength;
            //serializer.ReadKeyByRef(ref key);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                subscriptionProviders = new ConcurrentDictionary<byte[], ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>>(new ByteArrayComparer());
                prefixSubscriptionProviders = new ConcurrentDictionary<byte[], ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>>(new ByteArrayComparer());
                Task.Run(() => Start());
            }
            var subscriptionKey = new Span<byte>(start, (int)(end - start)).ToArray();
            var provider = this.server.sessionProviders[wireFormat];
            bool added = subscriptionProviders.TryAdd(subscriptionKey, new ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>());
            subscriptionProviders[subscriptionKey].TryAdd(provider, new List<(IServerSession, int)>());
            subscriptionProviders[subscriptionKey][provider].Add((session, sid));
            return id;
        }

        public unsafe int PSubscribe(ref byte* prefix, int prefixLength, IServerSession session, WireFormat wireFormat)
        {
            var start = prefix;
            var end = prefix + prefixLength;
            //serializer.ReadKeyByRef(ref prefix);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                subscriptionProviders = new ConcurrentDictionary<byte[], ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>>(new ByteArrayComparer());
                prefixSubscriptionProviders = new ConcurrentDictionary<byte[], ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>>(new ByteArrayComparer());
                Task.Run(() => Start());
            }
            var subscriptionPrefix = new Span<byte>(start, (int)(end - start)).ToArray();
            var provider = this.server.sessionProviders[wireFormat];
            prefixSubscriptionProviders.TryAdd(subscriptionPrefix, new ConcurrentDictionary<ISessionProvider, List<(IServerSession, int)>>());
            prefixSubscriptionProviders[subscriptionPrefix].TryAdd(provider, new List<(IServerSession, int)>());
            prefixSubscriptionProviders[subscriptionPrefix][provider].Add((session, sid));
            return id;
        }


        public unsafe void Publish(byte* key, int keyLength)
        {
            if (subscriptionProviders == null && prefixSubscriptionProviders == null) return;

            var start = key;
            var end = key + keyLength;
            //ref Key k = ref serializer.ReadKeyByRef(ref key);
            var keyBytes= new Span<byte>(start, (int)(end - start)).ToArray();

            publishQueue.Enqueue(keyBytes);
        }
    }
}

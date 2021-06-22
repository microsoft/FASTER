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
        readonly ClientSession<Key, Value, Input, Output, long, Functions> subscriptionSession;
        private int sid = 0;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>> subscriptions;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>> psubscriptions;
        private AsyncQueue<byte[]> publishQueue;

        public SubscribeKVBroker(ParameterSerializer serializer, ClientSession<Key, Value, Input, Output, long, Functions> subscriptionSession)
        {
            this.serializer = serializer;
            this.subscriptionSession = subscriptionSession;
        }

        public void removeSubscription(ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            if (subscriptions == null && psubscriptions == null)
                return;

            if (subscriptions != null)
            {
                foreach (var key in subscriptions.Keys)
                {
                    foreach (var subscribedSession in subscriptions[key].Keys)
                    {
                        if (subscribedSession == session)
                        {
                            subscriptions[key].TryRemove(subscribedSession, out _);
                        }
                    }
                }
            }

            if (psubscriptions != null)
            {
                foreach (var key in psubscriptions.Keys)
                {
                    foreach (var subscribedSession in psubscriptions[key].Keys)
                    {
                        if (subscribedSession == session)
                        {
                            psubscriptions[key].TryRemove(subscribedSession, out _);
                        }
                    }
                }
            }
        }

        public unsafe (Status, Output) ReadBeforePublish(ref byte* keyBytePtr, ref Input input, ref byte* outputBytePtr, int lengthOutput, int sid)
        {
            MessageType message = MessageType.SubscribeKV;

            ref Key key = ref serializer.ReadKeyByRef(ref keyBytePtr);
            ref Output output = ref serializer.AsRefOutput(outputBytePtr, (int)(lengthOutput));

            long ctx = ((long)message << 32) | (long)sid;
            var status = subscriptionSession.Read(ref key, ref input, ref output, ctx, 0);
            if (status == Status.PENDING)
                subscriptionSession.CompletePending(true);

            return (status, output);
        }

        unsafe bool CheckSubKeyMatch(ref byte* subKeyPtr, ref byte* keyPtr)
        {
            ref Key key = ref serializer.ReadKeyByRef(ref keyPtr);
            ref Key subKey = ref serializer.ReadKeyByRef(ref subKeyPtr);
            if (serializer.Match(ref key, ref subKey) == true)
                return true;
            return false;
        }

        public async Task Start()
        {
            Input input = default;
            var uniqueKeys = new HashSet<byte[]>(new ByteArrayComparer());

            byte[] outputBytes = new byte[1024];
            List<(BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int, bool)> subscribedSessions = new List<(BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int, bool)>();

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
                    foreach (var byteKey in uniqueKeys)
                    {
                        fixed (byte* ptr1 = &byteKey[0], ptr2 = &outputBytes[0])
                        {
                            byte* keyPtr = ptr1;
                            byte* outputPtr = ptr2;

                            bool foundSubscription = subscriptions.TryGetValue(byteKey, out var subSessionDict);
                            if (foundSubscription)
                            {
                                foreach (var session in subSessionDict.Keys)
                                {
                                    subSessionDict.TryGetValue(session, out int sid);
                                    subscribedSessions.Add((session, sid, false));
                                }
                            }

                            foreach (var subscribedPrefixBytes in psubscriptions.Keys)
                            {
                                fixed (byte* subscribedPrefixPtr = &subscribedPrefixBytes[0])
                                {
                                    byte* subPrefixPtr = subscribedPrefixPtr;
                                    byte* reqKeyPtr = ptr1;

                                    bool match = CheckSubKeyMatch(ref subPrefixPtr, ref reqKeyPtr);
                                    if (match)
                                    {
                                        psubscriptions.TryGetValue(subscribedPrefixBytes, out var sessionDict);
                                        foreach (var session in sessionDict.Keys)
                                        {
                                            sessionDict.TryGetValue(session, out int sid);
                                            subscribedSessions.Add((session, sid, true));
                                        }
                                    }
                                }
                            }


                            if (subscribedSessions.Count > 0)
                            {
                                var (status, output) = ReadBeforePublish(ref keyPtr, ref input, ref outputPtr, outputBytes.Length, 0);
                                foreach (var subSessionTuple in subscribedSessions)
                                {
                                    var session = subSessionTuple.Item1;
                                    var sid = subSessionTuple.Item2;
                                    var prefix = subSessionTuple.Item3;
                                    byte* publishKeyPtr = ptr1;
                                    session.Publish(sid, status, ref output, ref serializer.ReadKeyByRef(ref publishKeyPtr), prefix);
                                }
                            }
                        }
                        subscribedSessions.Clear();
                    }
                    uniqueKeys.Clear();
                }
            }
        }

        public unsafe int Subscribe(ref byte* key, BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            var start = key;
            serializer.ReadKeyByRef(ref key);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                this.subscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>(new ByteArrayComparer());
                this.psubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>(new ByteArrayComparer());
                Task.Run(() => Start());
            }
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            subscriptions.TryAdd(subscriptionKey, new ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>());
            subscriptions[subscriptionKey].TryAdd(session, id);
            return id;
        }

        public unsafe int PSubscribe(ref byte* prefix, BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            var start = prefix;
            serializer.ReadKeyByRef(ref prefix);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                this.subscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>(new ByteArrayComparer());
                this.psubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>(new ByteArrayComparer());
                Task.Run(() => Start());
            }
            var subscriptionPrefix = new Span<byte>(start, (int)(prefix - start)).ToArray();
            psubscriptions.TryAdd(subscriptionPrefix, new ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>());
            psubscriptions[subscriptionPrefix].TryAdd(session, id);
            return id;
        }


        public unsafe void Publish(byte* key)
        {
            if (subscriptions == null && psubscriptions == null) return;

            var start = key;
            ref Key k = ref serializer.ReadKeyByRef(ref key);
            var keyBytes= new Span<byte>(start, (int)(key - start)).ToArray();

            publishQueue.Enqueue(keyBytes);
        }
    }
}

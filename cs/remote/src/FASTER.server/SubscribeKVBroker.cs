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
        //private Trie<byte[], (int, HashSet<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>>)> subscriptionsTrie;
        private AsyncQueue<byte[]> publishQueue;        

        public SubscribeKVBroker(ParameterSerializer serializer, ClientSession<Key, Value, Input, Output, long, Functions> subscriptionSession)
        {
            this.serializer = serializer;
            this.subscriptionSession = subscriptionSession;
        }

        public void removeSubscription(ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> session)
        {
            if (subscriptions == null)
                return;

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

            //foreach (var key in subscriptionsTrie.Keys)
            //    subscriptionsTrie[key].Item2.Remove((BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>)session);
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
            /* Here do the matching of prefixes */

            Input input = default;
            var uniqueKeys = new HashSet<byte[]>(new ByteArrayComparer());
            // unique is a set of byte arrays non-conc
            // Read from queue and send to all subscribed sessions
            byte[] outputBytes = new byte[1024];
            List<(BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int)> subscribedSessions = new List<(BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int)>();

            while (true) {

                var subscriptionKey = await publishQueue.DequeueAsync();
                uniqueKeys.Add(subscriptionKey);

                while (publishQueue.Count > 0) // delete this line
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

                            foreach (var subscribedKeyBytes in subscriptions.Keys)
                            {
                                fixed (byte* subscribedKeyPtr = &subscribedKeyBytes[0])
                                {
                                    byte* subKeyPtr = subscribedKeyPtr;
                                    byte* reqKeyPtr = ptr1;

                                    bool match = CheckSubKeyMatch(ref subKeyPtr, ref reqKeyPtr);
                                    if (match)
                                    {
                                        foreach (var session in subscriptions[subscribedKeyBytes].Keys)
                                        {
                                            subscriptions[subscribedKeyBytes].TryGetValue(session, out int sid);
                                            subscribedSessions.Add((session, sid));
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
                                    session.Publish(sid, status, ref output);
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
            if (subscriptions == null)
            {
                // BC: we need a map of prefix => Set<(int, session)>
                Interlocked.CompareExchange(ref subscriptions, new ConcurrentDictionary<byte[], ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>(new ByteArrayComparer()), null);
                //Interlocked.CompareExchange(ref subscriptionsTrie, new Trie<byte[], (int, HashSet<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>>) > (new ByteArrayComparer()), null);
            }
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                Task.Run(() => Start());
            }
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            subscriptions.TryAdd(subscriptionKey, new ConcurrentDictionary<BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>, int>());
            subscriptions[subscriptionKey].TryAdd(session, id);
            // use a conc hash set
            return id;
        }

        public unsafe void Publish(byte* key)
        {
            if (subscriptions == null) return;

            var start = key;
            ref Key k = ref serializer.ReadKeyByRef(ref key);
            var subscriptionsKey = new Span<byte>(start, (int)(key - start)).ToArray();
            publishQueue.Enqueue(subscriptionsKey);
        }
    }
}

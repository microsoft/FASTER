using System;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;
using System.Collections.Generic;
using System.Collections.Concurrent;

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
        readonly ClientSession<Key, Value, Input, Output, long, Functions> subscriptionSession;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>, int>> subscribedSessions;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>, int>> prefixSubscribedSessions;

        /// <summary>
        /// Create FasterKV backend
        /// </summary>
        /// <param name="store"></param>
        /// <param name="functionsGen"></param>
        /// <param name="serializer"></param>
        /// <param name="maxSizeSettings"></param>
        public FasterKVProvider(FasterKV<Key, Value> store, Func<WireFormat, Functions> functionsGen,  ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default)
        {
            this.store = store;
            this.functionsGen = functionsGen;
            this.serializer = serializer;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
            this.subscriptionSession = store.For(functionsGen(WireFormat.DefaultVarLenKV)).NewSession<Functions>();
            this.subscribedSessions = new ConcurrentDictionary<byte[], ConcurrentDictionary<FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>();
            this.prefixSubscribedSessions = new ConcurrentDictionary<byte[], ConcurrentDictionary<FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>, int>>();
        }

        /// <inheritdoc />
        public IServerSession GetSession(WireFormat wireFormat, Socket socket, SubscribeKVBroker subscribeKVBroker)
        {
            return new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>(socket, store, functionsGen(wireFormat), serializer, maxSizeSettings, wireFormat, subscribeKVBroker);
        }

        public unsafe bool CheckSubKeyMatch(ref byte* subKeyPtr, ref byte* keyPtr)
        {
            ref Key key = ref serializer.ReadKeyByRef(ref keyPtr);
            ref Key subKey = ref serializer.ReadKeyByRef(ref subKeyPtr);
            if (serializer.Match(ref key, ref subKey) == true)
                return true;
            return false;
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

        public unsafe void ReadAndPublish(byte* keyPtr, List<(IServerSession, int, bool)> subSessions)
        {
            Input input = default;
            byte[] outputBytes = new byte[1024];
            fixed (byte* ptr = &outputBytes[0]) {
                byte* outputPtr = ptr;
                byte* publishKeyPtr = keyPtr;
                byte* publishOutputPtr = outputPtr;
                var (status, output) = ReadBeforePublish(ref keyPtr, ref input, ref outputPtr, outputBytes.Length, 0);
                foreach (var subSessionTuple in subSessions)
                {
                    var session = subSessionTuple.Item1;
                    var sid = subSessionTuple.Item2;
                    var prefix = subSessionTuple.Item3;
                    //session.Publish(sid, status, outputPtr, outputBytes.Length, publishKeyPtr, prefix);
                    session.Publish(sid, status, publishOutputPtr, serializer.GetLength(ref output), publishKeyPtr, prefix);
                }
            }
            subscribedSessions.Clear();
        }
    }
}
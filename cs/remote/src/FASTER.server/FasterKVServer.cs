// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.common;
using System;
using System.Runtime.CompilerServices;
using System.Collections.Concurrent;
using System.Threading;

namespace FASTER.server
{
    /// <summary>
    /// Server for FasterKV
    /// </summary>
    public sealed class FasterKVServer<Key, Value, Input, Output, Functions, ParameterSerializer> : IDisposable
            where Functions : IFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly SocketAsyncEventArgs acceptEventArg;
        readonly Socket servSocket;
        readonly MaxSizeSettings maxSizeSettings;
        readonly ConcurrentDictionary<ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>, byte> activeSessions;
        int activeSessionCount;
        bool disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store">Instance of FasterKV store to use in server</param>
        /// <param name="functionsGen">Functions generator (based on wire format)</param>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="serializer">Parameter serializer</param>
        /// <param name="maxSizeSettings">Max size settings</param>
        public FasterKVServer(FasterKV<Key, Value> store, Func<WireFormat, Functions> functionsGen, string address, int port, ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default) : base()
        {
            activeSessions = new ConcurrentDictionary<ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>, byte>();
            activeSessionCount = 0;
            disposed = false;

            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            acceptEventArg = new SocketAsyncEventArgs
            {
                UserToken = (store, functionsGen, serializer)
            };
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Start server
        /// </summary>
        public void Start()
        {
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            disposed = true;
            servSocket.Dispose();
            DisposeActiveSessions();
        }

        private bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            // Ok to create new event args on accept because we assume a connection to be long-running
            var receiveEventArgs = new SocketAsyncEventArgs();
            var bufferSize = BufferSizeUtils.ClientBufferSize(maxSizeSettings);
            var buffer = new byte[bufferSize];
            receiveEventArgs.SetBuffer(buffer, 0, bufferSize);

            var (store, functionsGen, serializer) = ((FasterKV<Key, Value>, Func<WireFormat, Functions>, ParameterSerializer))e.UserToken;

            var args = new ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>
            {
                socket = e.AcceptSocket,
                store = store,
                functionsGen = functionsGen,
                serializer = serializer,
                maxSizeSettings = maxSizeSettings
            };

            receiveEventArgs.UserToken = args;
            receiveEventArgs.Completed += RecvEventArg_Completed;

            e.AcceptSocket.NoDelay = true;
            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(receiveEventArgs))
                Task.Run(() => RecvEventArg_Completed(null, receiveEventArgs));
            return true;
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            do
            {
                if (!HandleNewConnection(e)) break;
                e.AcceptSocket = null;
            } while (!servSocket.AcceptAsync(e));
        }

        private void DisposeActiveSessions()
        {
            while (activeSessionCount > 0)
            {
                foreach (var kvp in activeSessions)
                {
                    var _session = kvp.Key;
                    if (_session != null)
                    {
                        if (activeSessions.TryRemove(_session, out _))
                        {
                            _session.Dispose();
                            Interlocked.Decrement(ref activeSessionCount);
                        }
                    }
                }
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || disposed)
            {
                DisposeConnectionSession(e);
                return false;
            }

            if (connArgs.session == null)
            {
                if (!CreateSession(e))
                    return false;
            }

            connArgs.session.AddBytesRead(e.BytesTransferred);
            var newHead = connArgs.session.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        private bool CreateSession(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;

            if (e.BytesTransferred < 4)
            {
                e.SetBuffer(0, e.Buffer.Length);
                return true;
            }

            if (e.Buffer[3] > 127)
            {
                connArgs.session = new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>(connArgs.socket, connArgs.store, connArgs.functionsGen(WireFormat.Binary), connArgs.serializer, connArgs.maxSizeSettings);
            }
            else
                throw new FasterException("Unexpected wire format");

            if (activeSessions.TryAdd(connArgs.session, default))
                Interlocked.Increment(ref activeSessionCount);
            else
                throw new Exception("Unexpected: unable to add session to activeSessions");

            if (disposed)
            {
                DisposeConnectionSession(e);
                return false;
            }
            return true;
        }

        private void DisposeConnectionSession(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            connArgs.socket.Dispose();
            e.Dispose();
            var _session = connArgs.session;
            if (_session != null)
            {
                if (activeSessions.TryRemove(_session, out _))
                {
                    _session.Dispose();
                    Interlocked.Decrement(ref activeSessionCount);
                }
            }
        }

        private void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            do
            {
                // No more things to receive
                if (!HandleReceiveCompletion(e)) break;
            } while (!connArgs.socket.ReceiveAsync(e));
        }
    }
}

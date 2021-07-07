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
    /// Remote server framework for FASTER artifacts
    /// </summary>
    public sealed class FasterServer : IDisposable
    {
        readonly SocketAsyncEventArgs acceptEventArg;
        readonly Socket servSocket;
        readonly int networkBufferSize;
        readonly ConcurrentDictionary<IServerSession, byte> activeSessions;
        readonly ConcurrentDictionary<WireFormat, ISessionProvider> sessionProviders;
        int activeSessionCount;
        bool disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="networkBufferSize">Size of network buffer</param>
        public FasterServer(string address, int port, int networkBufferSize = default)
        {
            activeSessions = new ConcurrentDictionary<IServerSession, byte>();
            sessionProviders = new ConcurrentDictionary<WireFormat, ISessionProvider>();
            activeSessionCount = 0;
            disposed = false;

            this.networkBufferSize = networkBufferSize;
            if (networkBufferSize == default)
                this.networkBufferSize = BufferSizeUtils.ClientBufferSize(new MaxSizeSettings());

            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Register session provider for specified wire format with the server
        /// </summary>
        /// <param name="wireFormat">Wire format</param>
        /// <param name="backendProvider">Session provider</param>
        public void Register(WireFormat wireFormat, ISessionProvider backendProvider)
        {
            if (!sessionProviders.TryAdd(wireFormat, backendProvider))
                throw new FasterException($"Wire format {wireFormat} already registered");
        }

        /// <summary>
        /// Unregister provider associated with specified wire format
        /// </summary>
        /// <param name="wireFormat"></param>
        /// <param name="provider"></param>
        public void Unregister(WireFormat wireFormat, out ISessionProvider provider)
            => sessionProviders.TryRemove(wireFormat, out provider);

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
            var buffer = new byte[networkBufferSize];
            receiveEventArgs.SetBuffer(buffer, 0, networkBufferSize);

            var args = new ConnectionArgs
            {
                socket = e.AcceptSocket
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
            var connArgs = (ConnectionArgs) e.UserToken;
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

        private unsafe bool CreateSession(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs) e.UserToken;

            if (e.BytesTransferred < 4) return false;

            WireFormat protocol;

            // FASTER's binary protocol family is identified by inverted size (int) field in the start of a packet
            // This results in a fourth byte value (little endian) > 127, denoting a non-ASCII wire format.
            if (e.Buffer[3] > 127)
            {
                if (e.BytesTransferred < 4 + BatchHeader.Size) return false;
                fixed (void* bh = &e.Buffer[4])
                    protocol = ((BatchHeader*)bh)->Protocol;
            }
            else
            {
                protocol = WireFormat.ASCII;
            }

            if (!sessionProviders.TryGetValue(protocol, out var provider))
            {
                throw new FasterException($"Unsupported wire format {protocol}");
            }

            connArgs.session = provider.GetSession(protocol, connArgs.socket);

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
            var connArgs = (ConnectionArgs) e.UserToken;
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
            try
            {
                var connArgs = (ConnectionArgs)e.UserToken;
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) break;
                } while (!connArgs.socket.ReceiveAsync(e));
            }
            // ignore session socket disposed due to server dispose
            catch (ObjectDisposedException) { }
        }
    }
}

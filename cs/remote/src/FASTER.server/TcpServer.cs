// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// TcpServer
    /// </summary>
    public class TcpServer : FasterServerBase
    {        
        readonly SocketAsyncEventArgs acceptEventArg;
        readonly Socket servSocket;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="networkBufferSize"></param>
        public TcpServer(string address, int port, int networkBufferSize = default)
            : base(address, port, networkBufferSize)
        {            
            var ip = Address == null ? IPAddress.Any : IPAddress.Parse(Address);
            var endPoint = new IPEndPoint(ip, Port);
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            servSocket.Dispose();            
        }

        private unsafe void DisposeConnectionSession(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs)e.UserToken;
            connArgs.socket.Dispose();
            e.Dispose();            
            DisposeSession(connArgs.session);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Start()
        {
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                do
                {
                    if (!HandleNewConnection(e)) break;
                    e.AcceptSocket = null;
                } while (!servSocket.AcceptAsync(e));
            }
            // socket disposed
            catch (ObjectDisposedException) { }
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
            var buffer = new byte[NetworkBufferSize];
            receiveEventArgs.SetBuffer(buffer, 0, NetworkBufferSize);

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
            // socket disposed
            catch (ObjectDisposedException)
            {
                DisposeConnectionSession(e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || Disposed)
            {
                DisposeConnectionSession(e);
                return false;
            }

            if (connArgs.session == null)
            {
                return CreateSession(e);
            }

            ProcessRequest(e);

            return true;
        }

        private unsafe bool CreateSession(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs)e.UserToken;

            // We need at least 4 bytes to determine session            
            if (e.BytesTransferred < 4)
            {                
                e.SetBuffer(e.BytesTransferred, e.Buffer.Length - e.BytesTransferred);
                return true;
            }

            WireFormat protocol;

            // FASTER's binary protocol family is identified by inverted size (int) field in the start of a packet
            // This results in a fourth byte value (little endian) > 127, denoting a non-ASCII wire format.
            if (e.Buffer[3] > 127)
            {
                if (e.BytesTransferred < 4 + BatchHeader.Size)
                {
                    e.SetBuffer(e.BytesTransferred, e.Buffer.Length - e.BytesTransferred);
                    return true;
                }
                fixed (void* bh = &e.Buffer[4])
                    protocol = ((BatchHeader*)bh)->Protocol;
            }
            else if (e.Buffer[0] == 71 && e.Buffer[1] == 69 && e.Buffer[2] == 84)
            {
                protocol = WireFormat.WebSocket;
            }
            else
            {
                protocol = WireFormat.ASCII;
            }

            if (!GetSessionProviders().TryGetValue(protocol, out var provider))
            {
                Console.WriteLine($"Unsupported incoming wire format {protocol}");
                DisposeConnectionSession(e);
                return false;
            }

            INetworkSender networkSender = new TcpNetworkSender(connArgs.socket, provider.GetMaxSizeSettings);            
            if (!AddSession(protocol, ref provider, networkSender, out var session))
            {
                DisposeConnectionSession(e);
                return false;
            }
            connArgs.session = session;

            if (Disposed)
            {
                DisposeConnectionSession(e);
                return false;
            }

            ProcessRequest(e);
            return true;
        }

        private unsafe void ProcessRequest(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs)e.UserToken;
            connArgs.session.AddBytesRead(e.BytesTransferred);

            if (connArgs.recvBufferPtr == null)
            {
                connArgs.recvHandle = GCHandle.Alloc(e.Buffer, GCHandleType.Pinned);
                connArgs.recvBufferPtr = (byte*)connArgs.recvHandle.AddrOfPinnedObject();
            }

            var _newHead = connArgs.session.TryConsumeMessages(connArgs.recvBufferPtr, e.Buffer.Length);

            if (_newHead == e.Buffer.Length)
            {
                connArgs.recvHandle.Free();
                connArgs.recvBufferPtr = null;

                // Need to grow input buffer
                var newBuffer = new byte[e.Buffer.Length * 2];
                Array.Copy(e.Buffer, newBuffer, e.Buffer.Length);
                e.SetBuffer(newBuffer, _newHead, newBuffer.Length - _newHead);
            }
            else
                e.SetBuffer(_newHead, e.Buffer.Length - _newHead);
        }
    }
}

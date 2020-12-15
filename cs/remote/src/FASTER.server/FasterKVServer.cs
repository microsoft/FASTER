// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

using FASTER.core;
using FASTER.common;
using System;

namespace FASTER.server
{
    /// <summary>
    /// Server for FasterKV
    /// </summary>
    public sealed class FasterKVServer<Key, Value, Input, Output, Functions, ParameterSerializer> : IDisposable
            where Functions : IFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        private readonly SocketAsyncEventArgs acceptEventArg;
        private readonly Socket servSocket;
        private readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store">Instance of FasterKV store to use in server</param>
        /// <param name="functions">Functions</param>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="serializer">Parameter serializer</param>
        /// <param name="maxSizeSettings">Max size settings</param>
        public FasterKVServer(FasterKV<Key, Value> store, Functions functions, string address, int port, ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default) : base()
        {
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            servSocket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            acceptEventArg = new SocketAsyncEventArgs
            {
                UserToken = (store, functions, serializer)
            };
            acceptEventArg.Completed += AcceptEventArg_Completed;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            servSocket.Dispose();
        }

        private bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            var (store, functions, serializer) = ((FasterKV<Key, Value>, Functions, ParameterSerializer))e.UserToken;
            var sns = new ServerNetworkSession<Key, Value, Input, Output, Functions, ParameterSerializer>(e.AcceptSocket, store, functions, serializer, maxSizeSettings);

            // Ok to create new event args on accept because we assume a connection to be long-running
            var receiveEventArgs = new SocketAsyncEventArgs();
            var bufferSize = BufferSizeUtils.ClientBufferSize(maxSizeSettings);
            receiveEventArgs.SetBuffer(new byte[bufferSize], 0, bufferSize);
            receiveEventArgs.UserToken = sns;
            receiveEventArgs.Completed += ServerNetworkSession<Key, Value, Input, Output, Functions, ParameterSerializer>.RecvEventArg_Completed;

            e.AcceptSocket.NoDelay = true;
            // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(receiveEventArgs))
                Task.Run(() => ServerNetworkSession<Key, Value, Input, Output, Functions, ParameterSerializer>.RecvEventArg_Completed(null, receiveEventArgs));
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

        /// <summary>
        /// Start server
        /// </summary>
        public void Start()
        {
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }
    }

}

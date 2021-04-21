// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal abstract class ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> : IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        protected readonly Socket socket;
        protected readonly ClientSession<Key, Value, Input, Output, long, ServerFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>> session;
        protected readonly ParameterSerializer serializer;
        protected readonly MaxSizeSettings maxSizeSettings;

        private readonly NetworkSender messageManager;
        private readonly int serverBufferSize;
        
        protected ReusableObject<SeaaBuffer> responseObject;
        protected int bytesRead;

        public ServerSessionBase(Socket socket, FasterKV<Key, Value> store, Functions functions, ParameterSerializer serializer, MaxSizeSettings maxSizeSettings)
        {
            this.socket = socket;
            session = store.For(new ServerFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>(functions, this)).NewSession<ServerFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>>();
            this.maxSizeSettings = maxSizeSettings;
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            messageManager = new NetworkSender(serverBufferSize);
            this.serializer = serializer;

            bytesRead = 0;
        }

        public abstract int TryConsumeMessages(byte[] buf);
        public abstract void CompleteRead(ref Output output, long ctx, core.Status status);
        public abstract void CompleteRMW(long ctx, core.Status status);

        protected void GetResponseObject() { if (responseObject.obj == null) responseObject = messageManager.GetReusableSeaaBuffer(); }

        protected void SendResponse(int size) => messageManager.Send(socket, responseObject, 0, size);
        protected void SendResponse(int offset, int size) => messageManager.Send(socket, responseObject, offset, size);

        private void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                connArgs.socket.Dispose();
                e.Dispose();
                connArgs.session?.Dispose();
                connArgs.session = null;
                return false;
            }

            if (connArgs.session == null)
            {
                if (e.BytesTransferred < 4)
                {
                    e.SetBuffer(0, e.Buffer.Length);
                    return true;
                }

                if (e.Buffer[3] > 127)
                    connArgs.session = new BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>(connArgs.socket, connArgs.store, connArgs.functionsGen(WireFormat.Binary), connArgs.serializer, connArgs.maxSizeSettings);
                else
                    throw new FasterException("Unexpected wire format");
            }

            connArgs.session.AddBytesRead(e.BytesTransferred);
            var newHead = connArgs.session.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var connArgs = (ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            do
            {
                // No more things to receive
                if (!HandleReceiveCompletion(e)) break;
            } while (!connArgs.socket.ReceiveAsync(e));
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose()
        {
            session.Dispose();
            messageManager.Dispose();
        }
    }

}

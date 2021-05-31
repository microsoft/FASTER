// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
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

        protected void SendResponse(int size)
        {
            try
            {
                messageManager.Send(socket, responseObject, 0, size);
            }
            catch
            {
                responseObject.Dispose();
            }
        }
        protected void SendResponse(int offset, int size)
        {
            try
            {
                messageManager.Send(socket, responseObject, offset, size);
            }
            catch
            {
                responseObject.Dispose();
            }
        }

        public void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;



        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose()
        {
            session.Dispose();
            socket.Dispose();
            if (responseObject.obj != null)
                responseObject.Dispose();
            messageManager.Dispose();
        }
    }

}

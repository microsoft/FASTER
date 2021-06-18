// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal abstract class ServerSessionBase : IDisposable
    {
        protected readonly Socket socket;
        protected readonly MaxSizeSettings maxSizeSettings;
        private readonly NetworkSender messageManager;
        private readonly int serverBufferSize;
        
        protected ReusableObject<SeaaBuffer> responseObject;
        protected int bytesRead;

        public ServerSessionBase(Socket socket, MaxSizeSettings maxSizeSettings)
        {
            this.socket = socket;
            this.maxSizeSettings = maxSizeSettings;
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            messageManager = new NetworkSender(serverBufferSize);
            bytesRead = 0;
        }

        public abstract int TryConsumeMessages(byte[] buf);


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
            socket.Dispose();
            if (responseObject.obj != null)
                responseObject.Dispose();
            messageManager.Dispose();
        }
    }

}
